import hashlib
import itertools
import logging
import threading
from urllib.parse import urlsplit

import grpc
from prometheus_client import Counter

import grpc_errors
import judge_server_pb2 as pb
import judge_server_pb2_grpc as pb_grpc
import sandbox_health
import tls

log = logging.getLogger("judge-worker")

# The sandbox's token: metadata of every call, the sha256 of the shared secret (token_header).
TOKEN_METADATA_KEY = "x-judge-server-token"
# A judge response carries every case's capped output, which can top gRPC's default 4 MiB.
_MAX_RECEIVE_BYTES = 64 * 1024 * 1024
_INITIAL_RECONNECT_BACKOFF_MS = 1000
_MAX_RECONNECT_BACKOFF_MS = 5000

# The codes dispatch_judge fails over on (grpc_errors.RETRYABLE_CODES: not up, TLS handshake failed, judger failure
# before any case ran, or busy); every other code is raised at once.
# Of those, the ones that say the sandbox itself is unwell. A busy sandbox is alive: it stays in rotation.
_DOWN_CODES = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INTERNAL})

# Judge requests sent to each sandbox, by outcome. E4's c3 criterion counts the failed ones (sub-project 6 §3.2 C).
DISPATCHES = Counter("oj_judge_dispatch", "Judge requests sent to a sandbox, by outcome", ["sandbox", "outcome"])

# Rotating counter for round-robin start selection across configured judge-servers.
_round_robin = itertools.count()

# One gRPC channel per sandbox address, for the life of the process: a channel multiplexes calls and reconnects by
# itself, retrying a sandbox that went away after at most _MAX_RECONNECT_BACKOFF_MS (gRPC's default backoff grows to
# 120 s, so a restarted sandbox would be refused for minutes).
_channels = {}
_channels_lock = threading.Lock()


def reset_round_robin():
    """Reset the round-robin counter (used by tests for deterministic start selection)."""
    global _round_robin
    _round_robin = itertools.count()


def token_header(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def parse_address(url):
    """The gRPC target (host:port) of a configured sandbox URL: an http:// or https:// scheme is dropped (the transport
    is chosen by OJ_TLS_DIR, not by the scheme), a bare host:port is kept. ValueError for a path, an empty host or a bad
    port: a misconfiguration that would otherwise fail on every submission."""
    rest = url.strip()
    for scheme in ("https://", "http://"):
        if rest.lower().startswith(scheme):
            rest = rest[len(scheme):]
            break
    parts = urlsplit("//" + rest)
    # parts.port raises ValueError itself for a port that is not a number in range; 0 is in range but cannot be dialled.
    if not parts.hostname or parts.path or parts.query or parts.fragment or parts.port == 0:
        raise ValueError(f"not a judge-server address (host:port): {url!r}")
    return parts.netloc


def _compile_config(c):
    # qduoj's names become the proto's (max_cpu_time -> max_cpu_time_ms, max_memory -> max_memory_bytes; a -1, no
    # limit, is kept); an absent or null field is left unset.
    return pb.CompileConfig(
        src_name=c.get("src_name") or "", exe_name=c.get("exe_name") or "",
        max_cpu_time_ms=c.get("max_cpu_time") or 0, max_real_time_ms=c.get("max_real_time") or 0,
        max_memory_bytes=c.get("max_memory") or 0, compile_command=c.get("compile_command") or "",
        env=c.get("env") or [])


def _run_config(r):
    # qduoj's null seccomp_rule (java, js) is the proto's "": no filter; its 1/0 flag is a bool.
    return pb.RunConfig(
        command=r.get("command") or "", seccomp_rule=r.get("seccomp_rule") or "", env=r.get("env") or [],
        memory_limit_check_only=bool(r.get("memory_limit_check_only")))


def build_judge_request(event):
    """The event's judge fields as the sandbox's JudgeRequest (judge_server.proto). A run carries its cases inline (Run
    code design §8); a submission names the bundle the worker synced to the shared cache."""
    request = pb.JudgeRequest(
        src=event["src"], max_cpu_time_ms=event["max_cpu_time"], max_memory_bytes=event["max_memory"],
        output=bool(event.get("output", True)))
    language_config = event["language_config"]
    if language_config.get("compile"):
        request.language_config.compile.CopyFrom(_compile_config(language_config["compile"]))
    if language_config.get("run"):
        request.language_config.run.CopyFrom(_run_config(language_config["run"]))
    if "test_case" in event:
        request.test_case.SetInParent()  # an empty list still chooses inline: the sandbox refuses it by name
        request.test_case.cases.extend(
            pb.InlineTestCase(input=case.get("input") or "", output=case.get("output") or "")
            for case in event["test_case"])
    else:
        request.test_case_id = event["test_case_id"]
    if event.get("spj_version"):
        # A checker problem's checker (SP13): submission-service sends spj_version, spj_config, spj_compile_config and
        # spj_src all together or not at all.
        spj_config = event["spj_config"]
        request.spj.CopyFrom(pb.Checker(
            version=event["spj_version"], src=event["spj_src"], compile=_compile_config(event["spj_compile_config"]),
            exe_name=spj_config.get("exe_name") or "", command=spj_config.get("command") or "",
            seccomp_rule=spj_config.get("seccomp_rule") or ""))
    return request


def _case_to_dict(case):
    return {
        "cpu_time": case.cpu_time_ms,
        "real_time": case.real_time_ms,
        "memory": case.memory_bytes,
        "signal": case.signal,
        "exit_code": case.exit_code,
        "error": case.error,
        "result": case.result,
        "test_case": case.test_case,
        "output_md5": case.output_md5,
        # The program's own bytes: not necessarily UTF-8, and the verdict event is JSON.
        "output": case.output.decode("utf-8", errors="replace"),
    }


def response_to_dict(resp):
    """A JudgeResponse as the dict handler.build_judged_event reads: {"err": None, "data": [one dict per case]} or
    {"err": "CompileError" | "SPJCompileError", "data": the compiler's message}. A response with no outcome set is
    {"err": "UnexpectedResponse", ...}, which handler turns into SYSTEM_ERROR. A short or empty case list is passed on
    as it is: handler refuses an incomplete judgement (verdict.incomplete_reason)."""
    outcome = resp.WhichOneof("outcome")
    if outcome == "results":
        return {"err": None, "data": [_case_to_dict(case) for case in resp.results.cases]}
    if outcome == "compile_error":
        return {"err": "CompileError", "data": resp.compile_error.message}
    if outcome == "spj_compile_error":
        return {"err": "SPJCompileError", "data": resp.spj_compile_error.message}
    return {"err": "UnexpectedResponse", "data": "judge-server answered without an outcome"}


def _channel(address):
    """The channel to `address`, opened on first use: over mutual TLS with the worker's certificate, or plaintext when
    OJ_TLS_DIR is not set (tls.grpc_channel_credentials). With TLS on and a certificate file missing this raises, and
    nothing is cached: the worker never falls back to plaintext."""
    with _channels_lock:
        channel = _channels.get(address)
        if channel is None:
            options = [("grpc.max_receive_message_length", _MAX_RECEIVE_BYTES),
                       ("grpc.initial_reconnect_backoff_ms", _INITIAL_RECONNECT_BACKOFF_MS),
                       ("grpc.max_reconnect_backoff_ms", _MAX_RECONNECT_BACKOFF_MS)]
            credentials = tls.grpc_channel_credentials()
            if credentials is None:
                channel = grpc.insecure_channel(address, options=options)
            else:
                channel = grpc.secure_channel(address, credentials, options=options)
            _channels[address] = channel
        return channel


def _stub(address):
    """The JudgeServer client on the cached channel to `address` (tests fake this)."""
    return pb_grpc.JudgeServerStub(_channel(address))


def call_judge_server(address, token, request, timeout=30):
    """JudgeServer.Judge on one sandbox, as the verdict dict. `timeout` is the call's deadline: when it passes the
    sandbox kills the job and the call raises DEADLINE_EXCEEDED. Raises grpc.RpcError for every failure of the call
    itself; a compile error or a wrong answer is a result, not an error."""
    response = _stub(address).Judge(request, timeout=timeout, metadata=[(TOKEN_METADATA_KEY, token_header(token))])
    return response_to_dict(response)


def _is_retryable(exc):
    """A sandbox that is down or full is worth failing over (grpc_errors.RETRYABLE_CODES); any other failure is raised
    at once."""
    return grpc_errors.code_of(exc) in grpc_errors.RETRYABLE_CODES


def _marks_down(exc):
    """Whether the failure says the sandbox is unwell (sandbox_health.mark down). A busy sandbox (RESOURCE_EXHAUSTED)
    is failed over from but left in rotation."""
    return grpc_errors.code_of(exc) in _DOWN_CODES


def dispatch_judge(urls, token, request, timeout=30):
    """Send the JudgeRequest to one of `urls`: the healthy sandboxes first, each list in round-robin order, failing
    over to the next candidate on a retryable error (_is_retryable). A failure that says the sandbox is unwell marks it
    down and a success marks it up (sandbox_health). Raises the last error if every candidate fails, the error itself
    at once if it is not retryable, or ValueError if no URLs are configured or one is not a host:port
    (parse_address)."""
    if not urls:
        raise ValueError("no judge-server URLs configured")
    addresses = {url: parse_address(url) for url in urls}
    start = next(_round_robin) % len(urls)
    ordered = sandbox_health.order(urls[start:] + urls[:start])
    last_exc = None
    for url in ordered:
        label = sandbox_health.sandbox_label(url)
        try:
            result = call_judge_server(addresses[url], token, request, timeout=timeout)
        except grpc.RpcError as exc:
            DISPATCHES.labels(sandbox=label, outcome="failed").inc()
            if not _is_retryable(exc):
                raise
            if _marks_down(exc):
                sandbox_health.mark(url, False)
            last_exc = exc
            log.warning("judge-server %s failed (%s: %s), trying next", url, grpc_errors.code_of(exc).name,
                        grpc_errors.details_of(exc))
            continue
        DISPATCHES.labels(sandbox=label, outcome="ok").inc()
        sandbox_health.mark(url, True)
        return result
    raise last_exc
