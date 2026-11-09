"""The dispatcher against a real in-process gRPC server (plaintext): what the faked stubs of test_dispatcher.py cannot
show, namely the generated stub's method and metadata on the wire, the message size limit, and the exceptions gRPC
really raises (their status code and details are what _is_retryable reads)."""
import hashlib
import re
import socket
import struct
import threading
import time
from concurrent import futures
from pathlib import Path
from unittest.mock import MagicMock, patch

import grpc
import pytest

import dispatcher
import infra_errors
import judge_server_pb2 as pb
import judge_server_pb2_grpc as pb_grpc
import sandbox_health
import verdict
import worker

CODE = grpc.StatusCode
FIXTURES = Path(__file__).parent / "fixtures" / "tls"


class _Sandbox(pb_grpc.JudgeServerServicer):
    """Answers Judge with `reply`, or aborts the call with `abort` = (status code, details); `delay` seconds first.
    JudgeStream sends `reply`'s cases one by one (without their output), then `reply` as done; `abort_after` cases it
    aborts instead, and `no_done` ends the stream after the cases."""

    def __init__(self, reply=None, abort=None, delay=0.0, abort_after=None, no_done=False):
        self.reply, self.abort, self.delay = reply, abort, delay
        self.abort_after, self.no_done = abort_after, no_done
        self.calls = []
        self.stream_calls = []

    def Judge(self, request, context):
        self.calls.append((request, dict(context.invocation_metadata())))
        time.sleep(self.delay)
        if self.abort:
            context.abort(*self.abort)
        return self.reply

    def JudgeStream(self, request, context):
        self.stream_calls.append((request, dict(context.invocation_metadata())))
        time.sleep(self.delay)
        if self.abort:
            context.abort(*self.abort)
        for i, case in enumerate(self.reply.results.cases):
            if self.abort_after is not None and i == self.abort_after:
                context.abort(CODE.UNAVAILABLE, "sandbox died mid-judgement")
            event = pb.CaseResult()
            event.CopyFrom(case)
            event.ClearField("output")
            yield pb.JudgeEvent(case=event)
        if not self.no_done:
            yield pb.JudgeEvent(done=self.reply)


class _UnaryOnlySandbox(_Sandbox):
    """A sandbox from before JudgeStream existed: the base servicer answers the stream with UNIMPLEMENTED."""
    JudgeStream = pb_grpc.JudgeServerServicer.JudgeStream


def _refused_url():
    """The URL of a port nothing listens on."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{s.getsockname()[1]}"


def _accepted(output=b"3\n"):
    return pb.JudgeResponse(results=pb.CaseResults(cases=[pb.CaseResult(test_case="1", result=0, output=output)]))


def _two_cases():
    return pb.JudgeResponse(results=pb.CaseResults(cases=[
        pb.CaseResult(test_case="1", result=0, cpu_time_ms=12, real_time_ms=15, memory_bytes=4096, output=b"1\n"),
        pb.CaseResult(test_case="2", result=-1, cpu_time_ms=21, output=b"22\n")]))


def test_a_call_carries_the_token_over_the_wire_and_returns_the_verdict_dict(serve):
    sandbox = _Sandbox(reply=_accepted())
    url = serve(sandbox)
    request = pb.JudgeRequest(src="int main(){}", max_cpu_time_ms=1000, test_case_id="p__h")

    out = dispatcher.dispatch_judge([url], "secret", request, timeout=5)

    assert out["err"] is None and out["data"][0]["output"] == "3\n"
    received, metadata = sandbox.calls[0]
    assert received == request
    assert metadata["x-judge-server-token"] == hashlib.sha256(b"secret").hexdigest()


def test_a_refused_connection_is_unavailable_and_dispatch_fails_over_to_the_live_sandbox(serve):
    dispatcher.reset_round_robin()
    dead, live = _refused_url(), serve(_Sandbox(reply=_accepted()))

    out = dispatcher.dispatch_judge([dead, live], "t", pb.JudgeRequest(), timeout=5)

    assert out["data"][0]["result"] == 0
    assert sandbox_health.order([dead, live]) == [live, dead]


def test_every_sandbox_refusing_raises_unavailable(serve):
    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([_refused_url(), _refused_url()], "t", pb.JudgeRequest(), timeout=5)
    assert caught.value.code() == CODE.UNAVAILABLE


def test_invalid_argument_from_the_sandbox_is_raised_at_once_with_its_details(serve):
    dispatcher.reset_round_robin()
    refusing = _Sandbox(abort=(CODE.INVALID_ARGUMENT, "unknown seccomp rule"))
    other = _Sandbox(reply=_accepted())
    first, second = serve(refusing), serve(other)

    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([first, second], "t", pb.JudgeRequest(), timeout=5)

    assert (caught.value.code(), caught.value.details()) == (CODE.INVALID_ARGUMENT, "unknown seccomp rule")
    assert other.calls == []


def test_a_sandbox_still_judging_when_the_deadline_passes_is_not_tried_again_elsewhere(serve):
    dispatcher.reset_round_robin()
    slow, other = _Sandbox(reply=_accepted(), delay=1.0), _Sandbox(reply=_accepted())
    first, second = serve(slow), serve(other)

    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([first, second], "t", pb.JudgeRequest(), timeout=0.2)

    assert caught.value.code() == CODE.DEADLINE_EXCEEDED
    assert other.calls == []
    assert sandbox_health.order([first, second]) == [first, second]


def test_a_response_over_grpcs_default_4_mib_is_received(serve):
    # Every case's capped output travels in one response: the channel raises the receive limit for it.
    url = serve(_Sandbox(reply=_accepted(output=b"x" * (5 * 1024 * 1024))))

    out = dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=10)

    assert len(out["data"][0]["output"]) == 5 * 1024 * 1024


# --- what the worker makes of the exceptions gRPC really raises (infra_errors) ---

def test_a_real_refused_connection_is_waited_out_and_never_shows_the_debug_dump(serve):
    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([_refused_url()], "t", pb.JudgeRequest(), timeout=5)
    exc = caught.value

    assert len(str(exc).splitlines()) > 1                  # the debug dump that must not reach error_message
    assert infra_errors.transient_reason(exc) == "sandbox"
    message = infra_errors.describe(exc)                   # (a waited-out failure is never shown: GIVE_UP_MESSAGE)
    assert message == infra_errors.TLS_FAILURE_MESSAGE and len(message.splitlines()) == 1
    assert "Connection refused" not in message and "127.0.0.1" not in message


def test_a_real_invalid_argument_is_not_waited_out_and_described_on_one_line(serve):
    url = serve(_Sandbox(abort=(CODE.INVALID_ARGUMENT, "unknown seccomp rule")))
    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5)
    exc = caught.value

    assert len(str(exc).splitlines()) > 1
    assert infra_errors.describe(exc) == "INVALID_ARGUMENT: unknown seccomp rule"
    assert infra_errors.transient_reason(exc) is None


def test_an_unavailable_naming_ssl_or_a_certificate_is_not_waited_out(serve):
    # The words come from the servicer here, so this shows only the wire: the status code and details the classifier
    # reads are the ones the client really receives. The real handshake failures are below.
    url = serve(_Sandbox(abort=(CODE.UNAVAILABLE, "Ssl handshake failed: certificate verify failed")))
    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5)

    assert infra_errors.transient_reason(caught.value) is None
    assert infra_errors.describe(caught.value) == "UNAVAILABLE: the judge sandbox's TLS certificate check failed"


# --- real TLS handshake failures: the details gRPC itself writes, through dispatch_judge to infra_errors ---
# The sandbox here is a mutual-TLS server with the test fixtures' server leaf (SAN localhost / 127.0.0.1), signed by the
# fixtures' CA, which requires a client certificate signed by that CA, like the Go judge-server.

def _fixture(name):
    return (FIXTURES / name).read_bytes()


@pytest.fixture
def serve_tls(serve):
    """serve_tls(servicer) -> the "https://127.0.0.1:port" URL of a live mutual-TLS gRPC server. `serve` closes the
    channels the dispatcher opens."""
    servers = []

    def start(servicer):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        pb_grpc.add_JudgeServerServicer_to_server(servicer, server)
        credentials = grpc.ssl_server_credentials([(_fixture("server.key"), _fixture("server.crt"))],
                                                  root_certificates=_fixture("ca.crt"), require_client_auth=True)
        servers.append(server)
        port = server.add_secure_port("127.0.0.1:0", credentials)
        server.start()
        return f"https://127.0.0.1:{port}"

    yield start
    for server in servers:
        server.stop(None)


def _worker_credentials(monkeypatch, root="ca.crt", client_certificate=True):
    """What the worker's channel would present (tls.grpc_channel_credentials), with `root` as the CA it trusts."""
    client = dict(private_key=_fixture("tls-probe.key"), certificate_chain=_fixture("tls-probe.crt")) \
        if client_certificate else {}
    credentials = grpc.ssl_channel_credentials(root_certificates=_fixture(root), **client)
    monkeypatch.setattr(dispatcher.tls, "grpc_channel_credentials", lambda: credentials)


def _dispatch_failure(url):
    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5)
    return caught.value


def _published_message(url):
    """The error_message the worker publishes for a submission judged on `url`, whose real dispatch_judge failure it
    classifies and describes (worker.judge_with_infra_retry). Any wait fails the test: the failure must end at once."""
    producer = MagicMock()
    config = MagicMock(JUDGED_TOPIC="submission.judged", STARTED_TOPIC="submission.started", WORKER_ID="w1",
                       PUBLISH_TIMEOUT_SECONDS=10, JUDGE_INFRA_RETRY_SECONDS=300)

    def judge(*args, **kwargs):
        return dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5)

    def no_wait(seconds):
        pytest.fail(f"the worker waited {seconds} s on a failure that is permanent")

    with patch("worker.process_event", side_effect=judge):
        worker.handle_one({"submissionId": "s-tls"}, config, producer, wait=no_wait)
    sent = [c.kwargs["value"] for c in producer.send.call_args_list if c.args[0] == "submission.judged"]
    assert [v["status"] for v in sent] == [verdict.SYSTEM_ERROR]
    return sent[0]["error_message"]


def _assert_only_the_fixed_message_is_shown(message, caplog, *logged):
    """The submitter reads the fixed line, with no address and none of gRPC's text; each of `logged` is in the log."""
    assert message == "UNAVAILABLE: the judge sandbox's TLS certificate check failed"
    assert "ipv4:" not in message and not re.search(r"\d+\.\d+\.\d+\.\d+", message)
    assert "Ssl handshake failed" not in message and "Peer name" not in message and "\n" not in message
    for text in logged:
        assert text in caplog.text


def test_a_real_tls_call_with_the_right_ca_and_certificate_is_judged(serve_tls, monkeypatch):
    # the control of the failures below: the same server and client succeed when nothing is misconfigured
    _worker_credentials(monkeypatch)
    out = dispatcher.dispatch_judge([serve_tls(_Sandbox(reply=_accepted()))], "t", pb.JudgeRequest(), timeout=5)
    assert out["data"][0]["result"] == 0


def test_a_real_wrong_ca_is_a_permanent_tls_failure(serve_tls, monkeypatch, caplog):
    # The worker trusts a certificate that did not sign the sandbox's: a wrong ca.crt in its OJ_TLS_DIR.
    _worker_credentials(monkeypatch, root="tls-probe.crt")
    url = serve_tls(_Sandbox(reply=_accepted()))
    exc = _dispatch_failure(url)

    assert exc.code() == CODE.UNAVAILABLE                   # the status of a sandbox that is down, too
    assert "ssl" in exc.details().lower() or "certificate" in exc.details().lower()
    assert "CERTIFICATE_VERIFY_FAILED" in exc.details()
    assert infra_errors.transient_reason(exc) is None
    assert "ipv4:127.0.0.1" in exc.details()                # the address that must not reach the submitter
    # end to end: the submitter's line is fixed, and the log keeps the sandbox's address and gRPC's error text
    _assert_only_the_fixed_message_is_shown(_published_message(url), caplog,
                                            "ipv4:127.0.0.1", "Ssl handshake failed", "CERTIFICATE_VERIFY_FAILED")


def test_a_real_hostname_mismatch_is_a_permanent_tls_failure(serve_tls, caplog):
    # The worker dials a name the sandbox's certificate does not carry (here the name is overridden: the fixture's
    # certificate holds localhost and 127.0.0.1 and the address is 127.0.0.1).
    url = serve_tls(_Sandbox(reply=_accepted()))
    address = dispatcher.parse_address(url)
    credentials = grpc.ssl_channel_credentials(_fixture("ca.crt"), _fixture("tls-probe.key"),
                                               _fixture("tls-probe.crt"))
    dispatcher._channels[address] = grpc.secure_channel(
        address, credentials, options=[("grpc.ssl_target_name_override", "judge-server")])
    exc = _dispatch_failure(url)

    assert exc.code() == CODE.UNAVAILABLE and "Peer name judge-server is not in peer certificate" in exc.details()
    assert infra_errors.transient_reason(exc) is None
    _assert_only_the_fixed_message_is_shown(_published_message(url), caplog,
                                            "ipv4:127.0.0.1", "Peer name judge-server is not in peer certificate")


def test_a_tls_failure_on_one_sandbox_still_fails_over_to_the_next(serve_tls, monkeypatch):
    # dispatch_judge's loop is unchanged: UNAVAILABLE is retryable and marks the sandbox down, TLS words or not.
    dispatcher.reset_round_robin()
    broken, healthy = serve_tls(_Sandbox(reply=_accepted())), serve_tls(_Sandbox(reply=_accepted()))
    broken_address = dispatcher.parse_address(broken)
    dispatcher._channels[broken_address] = grpc.secure_channel(
        broken_address, grpc.ssl_channel_credentials(_fixture("tls-probe.crt")))    # a wrong CA, no client certificate
    _worker_credentials(monkeypatch)                                                  # the healthy sandbox's channel

    out = dispatcher.dispatch_judge([broken, healthy], "t", pb.JudgeRequest(), timeout=5)

    assert out["data"][0]["result"] == 0
    assert sandbox_health.order([broken, healthy]) == [healthy, broken]


def test_a_real_client_certificate_the_sandbox_rejects_reads_like_a_closed_socket_and_is_waited_out(serve_tls,
                                                                                                    monkeypatch):
    # A known limit of the details rule: under TLS 1.3 the sandbox's rejection of a missing or unknown client
    # certificate comes after the handshake, and the client only sees the connection closed, which is also what a
    # restarting sandbox looks like. Neither ssl nor certificate is in it, so it is waited out like any UNAVAILABLE.
    _worker_credentials(monkeypatch, client_certificate=False)
    exc = _dispatch_failure(serve_tls(_Sandbox(reply=_accepted())))

    assert exc.code() == CODE.UNAVAILABLE and "Socket closed" in exc.details()
    assert infra_errors.transient_reason(exc) == "sandbox"


def test_a_real_connection_cut_during_the_tls_handshake_is_waited_out(monkeypatch):
    # A sandbox restarting while a worker dials it: the TCP connection is accepted, then dropped after the client's
    # hello. It reads like a handshake failure but names neither ssl nor a certificate.
    _worker_credentials(monkeypatch)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)

    def cut():
        while True:
            try:
                connection, _ = listener.accept()
            except OSError:
                return
            connection.recv(4096)
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))   # reset, not close
            connection.close()

    threading.Thread(target=cut, daemon=True).start()
    url = f"https://127.0.0.1:{listener.getsockname()[1]}"
    channels = {}
    monkeypatch.setattr(dispatcher, "_channels", channels)
    try:
        exc = _dispatch_failure(url)
    finally:
        for channel in channels.values():
            channel.close()
        listener.close()

    assert exc.code() == CODE.UNAVAILABLE
    assert "ssl" not in exc.details().lower() and "certificate" not in exc.details().lower()
    assert infra_errors.transient_reason(exc) == "sandbox"


# --- JudgeStream: the cases as they are judged, then the verdict ---

def test_with_on_case_the_stream_reports_each_case_without_output_and_returns_the_verdict(serve):
    sandbox = _Sandbox(reply=_two_cases())
    url = serve(sandbox)
    seen = []

    out = dispatcher.dispatch_judge([url], "secret", pb.JudgeRequest(src="x"), timeout=5, on_case=seen.append)

    assert seen == [
        {"test_case": "1", "result": 0, "error": 0, "cpu_time": 12, "real_time": 15, "memory": 4096},
        {"test_case": "2", "result": -1, "error": 0, "cpu_time": 21, "real_time": 0, "memory": 0}]
    assert out["err"] is None and [c["output"] for c in out["data"]] == ["1\n", "22\n"]
    assert sandbox.calls == []                       # the stream, not the unary call
    _, metadata = sandbox.stream_calls[0]
    assert metadata["x-judge-server-token"] == hashlib.sha256(b"secret").hexdigest()


def test_without_on_case_the_unary_judge_is_used(serve):
    sandbox = _Sandbox(reply=_two_cases())
    url = serve(sandbox)

    out = dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5)

    assert len(out["data"]) == 2 and sandbox.stream_calls == [] and len(sandbox.calls) == 1


def test_a_sandbox_without_judge_stream_is_asked_again_with_judge(serve):
    sandbox = _UnaryOnlySandbox(reply=_two_cases())
    url = serve(sandbox)
    seen = []

    out = dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5, on_case=seen.append)

    assert seen == [] and len(out["data"]) == 2
    assert len(sandbox.calls) == 1                   # the fallback went to the same sandbox
    assert sandbox_health.order([url]) == [url]


def test_a_stream_that_dies_mid_judgement_fails_over_and_only_the_next_verdict_counts(serve):
    dispatcher.reset_round_robin()
    dying, live = _Sandbox(reply=_two_cases(), abort_after=1), _Sandbox(reply=_two_cases())
    first, second = serve(dying), serve(live)
    seen = []

    out = dispatcher.dispatch_judge([first, second], "t", pb.JudgeRequest(), timeout=5, on_case=seen.append)

    # the first sandbox reported case 1 before dying; the second reported both; the verdict is the second's
    assert [c["test_case"] for c in seen] == ["1", "1", "2"]
    assert len(out["data"]) == 2
    assert sandbox_health.order([first, second]) == [second, first]


def test_a_stream_that_ends_without_a_verdict_is_an_unexpected_response(serve):
    url = serve(_Sandbox(reply=_two_cases(), no_done=True))
    seen = []

    out = dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5, on_case=seen.append)

    assert len(seen) == 2
    assert out["err"] == "UnexpectedResponse"


def test_a_streamed_compile_error_is_still_a_compile_error(serve):
    reply = pb.JudgeResponse(compile_error=pb.CompileError(message="expected ';'"))
    url = serve(_Sandbox(reply=reply))
    seen = []

    out = dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5, on_case=seen.append)

    assert seen == [] and out == {"err": "CompileError", "data": "expected ';'"}


def test_a_stream_refused_with_invalid_argument_is_raised_at_once(serve):
    url = serve(_Sandbox(reply=_two_cases(), abort=(CODE.INVALID_ARGUMENT, "unknown seccomp rule")))
    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5, on_case=lambda c: None)
    assert (caught.value.code(), caught.value.details()) == (CODE.INVALID_ARGUMENT, "unknown seccomp rule")
