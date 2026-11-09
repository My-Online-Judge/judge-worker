"""Ping every sandbox, keep sandbox_health current, and report each answer to submission-service's judge-server
registry on the sandbox's behalf.

The sandboxes run untrusted code, so they sit on an internal network with no route to submission-service. The worker
reaches both: it calls each sandbox's gRPC Ping (the hostname, version, cpu_core, cpu and memory QingdaoU's own heartbeat
sends, plus running_jobs and max_jobs), marks the sandbox up or down in sandbox_health (dispatch tries the healthy ones
first; oj_sandbox_up reads the same marks), and forwards the answer as that sandbox's heartbeat when
SANDBOX_HEARTBEAT_URL is set. A sandbox that stops answering is marked down at once and simply stops being reported; the
registry marks it offline after its 30 s window.
"""
import logging
import threading
import time

import grpc
import requests

import dispatcher
import grpc_errors
import judge_server_pb2 as pb
import sandbox_health
import tls

log = logging.getLogger("judge-worker")


def ping(address, token, timeout):
    """JudgeServer.Ping on the sandbox at `address` (host:port, dispatcher.parse_address), as the dict the registry's
    heartbeat carries: the PingResponse's fields plus action "pong". `timeout` is the call's deadline in seconds. Raises
    grpc.RpcError when the call fails (not up, TLS handshake failed, deadline passed, token refused)."""
    response = dispatcher._stub(address).Ping(
        pb.PingRequest(), timeout=timeout, metadata=[(dispatcher.TOKEN_METADATA_KEY, dispatcher.token_header(token))])
    return {"hostname": response.hostname, "judger_version": response.judger_version, "cpu_core": response.cpu_core,
            "cpu": response.cpu, "memory": response.memory, "running_jobs": response.running_jobs,
            "max_jobs": response.max_jobs, "action": "pong"}


def _why(exc):
    """One line for the log: a failed call as `CODE: details`, anything else as its own text."""
    code = grpc_errors.code_of(exc)
    return f"{code.name}: {grpc_errors.details_of(exc)}" if code else str(exc)


def report_once(urls, token, heartbeat_url, timeout=3, *, tls_kwargs=None):
    """One round. tls_kwargs: tls.requests_kwargs(), read once by start(); submission-service requires the worker's
    certificate on the heartbeat (each sandbox's channel presents it by itself, dispatcher._channel)."""
    tls_kwargs = tls_kwargs or {}
    digest = dispatcher.token_header(token)
    for url in urls:
        try:
            info = ping(dispatcher.parse_address(url), token, timeout)
        # RpcError: the call failed. ValueError: not a host:port. OSError, RuntimeError: no channel can be opened because
        # a certificate file is gone (tls.paths raises RuntimeError for a missing one, the read an OSError for an
        # unreadable one; no call is made). Either way this sandbox is unreachable: marked down, and the round goes on to
        # the next.
        except (grpc.RpcError, ValueError, OSError, RuntimeError) as exc:
            sandbox_health.mark(url, False)
            log.warning("sandbox %s: no answer to Ping (%s)", url, _why(exc))
            continue
        sandbox_health.mark(url, True)
        if not heartbeat_url:
            continue
        info.update(action="heartbeat", service_url=url)
        try:
            # Over mutual TLS with the worker's certificate: submission-service allow-lists the CN judge-worker.
            requests.post(heartbeat_url, json=info, headers={"X-JUDGE-SERVER-TOKEN": digest},
                          timeout=timeout, **tls_kwargs).raise_for_status()
        except requests.RequestException as exc:
            log.warning("sandbox %s: status not reported (%s)", url, exc)


def start(config):
    """Ping every SANDBOX_HEARTBEAT_INTERVAL_SECONDS on a daemon thread; None when no sandbox is configured. With TLS on
    and a certificate file missing, raises here: the worker does not start (never falls back to plaintext)."""
    if not config.JUDGE_SERVER_URLS:
        return None
    tls_kwargs = tls.requests_kwargs()
    tls.grpc_channel_credentials()  # the channels to the sandboxes read the same files at first use: fail closed here too

    def loop():
        while True:
            try:
                report_once(config.JUDGE_SERVER_URLS, config.JUDGE_SERVER_TOKEN, config.SANDBOX_HEARTBEAT_URL,
                            tls_kwargs=tls_kwargs)
            except Exception:  # e.g. a certificate file gone since start: one lost round, never the thread
                log.exception("sandbox status round failed")
            time.sleep(config.SANDBOX_HEARTBEAT_INTERVAL_SECONDS)

    thread = threading.Thread(target=loop, name="sandbox-status", daemon=True)
    thread.start()
    return thread
