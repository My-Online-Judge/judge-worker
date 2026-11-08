"""The dispatcher against a real in-process gRPC server (plaintext): what the faked stubs of test_dispatcher.py cannot
show, namely the generated stub's method and metadata on the wire, the message size limit, and the exceptions gRPC
really raises (their status code and details are what _is_retryable reads)."""
import hashlib
import socket
import time

import grpc
import pytest

import dispatcher
import infra_errors
import judge_server_pb2 as pb
import judge_server_pb2_grpc as pb_grpc
import sandbox_health

CODE = grpc.StatusCode


class _Sandbox(pb_grpc.JudgeServerServicer):
    """Answers Judge with `reply`, or aborts the call with `abort` = (status code, details); `delay` seconds first."""

    def __init__(self, reply=None, abort=None, delay=0.0):
        self.reply, self.abort, self.delay = reply, abort, delay
        self.calls = []

    def Judge(self, request, context):
        self.calls.append((request, dict(context.invocation_metadata())))
        time.sleep(self.delay)
        if self.abort:
            context.abort(*self.abort)
        return self.reply


def _refused_url():
    """The URL of a port nothing listens on."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{s.getsockname()[1]}"


def _accepted(output=b"3\n"):
    return pb.JudgeResponse(results=pb.CaseResults(cases=[pb.CaseResult(test_case="1", result=0, output=output)]))


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

def test_a_real_refused_connection_is_waited_out_and_described_on_one_line(serve):
    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([_refused_url()], "t", pb.JudgeRequest(), timeout=5)
    exc = caught.value

    assert len(str(exc).splitlines()) > 1                  # the debug dump that must not reach error_message
    message = infra_errors.describe(exc)
    assert message.startswith("UNAVAILABLE: ") and "Connection refused" in message
    assert len(message.splitlines()) == 1
    assert infra_errors.transient_reason(exc) == "sandbox"


def test_a_real_invalid_argument_is_not_waited_out_and_described_on_one_line(serve):
    url = serve(_Sandbox(abort=(CODE.INVALID_ARGUMENT, "unknown seccomp rule")))
    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5)
    exc = caught.value

    assert len(str(exc).splitlines()) > 1
    assert infra_errors.describe(exc) == "INVALID_ARGUMENT: unknown seccomp rule"
    assert infra_errors.transient_reason(exc) is None


def test_a_real_unavailable_naming_the_tls_handshake_is_waited_out(serve):
    url = serve(_Sandbox(abort=(CODE.UNAVAILABLE, "Ssl handshake failed: certificate verify failed")))
    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([url], "t", pb.JudgeRequest(), timeout=5)

    assert infra_errors.transient_reason(caught.value) == "sandbox"
    assert infra_errors.describe(caught.value) == "UNAVAILABLE: Ssl handshake failed: certificate verify failed"
