import hashlib
import logging
import threading
from types import SimpleNamespace
from unittest.mock import patch

import grpc
import pytest
import requests
from prometheus_client import REGISTRY

import dispatcher
import judge_server_pb2 as pb
import judge_server_pb2_grpc as pb_grpc
import sandbox_health
import sandbox_status
import tls
from tests.rpc_errors import rpc_error

CODE = grpc.StatusCode


def up(sandbox):
    return REGISTRY.get_sample_value("oj_sandbox_up", {"sandbox": sandbox})

HEARTBEAT_URL = "http://submission-service:8000/api/judge_server_heartbeat/"
TOKEN_SHA = hashlib.sha256(b"t").hexdigest()


def _pong(hostname, running_jobs=1, max_jobs=4):
    return pb.PingResponse(hostname=hostname, judger_version="2.1.1", cpu_core=8, cpu=7.5, memory=55.25,
                           running_jobs=running_jobs, max_jobs=max_jobs)


def _beat_body(hostname, url, running_jobs=1, max_jobs=4):
    """The heartbeat JSON for _pong(hostname): QingdaoU's server_info keys, plus the load."""
    return {"hostname": hostname, "judger_version": "2.1.1", "cpu_core": 8, "cpu": 7.5, "memory": 55.25,
            "running_jobs": running_jobs, "max_jobs": max_jobs, "action": "heartbeat", "service_url": url}


class _Sandboxes:
    """dispatcher._stub faked: answers[address] is what that sandbox's Ping returns (a PingResponse) or raises (an
    exception); calls lists every Ping as (address, request, timeout, metadata)."""

    def __init__(self):
        self.answers, self.calls = {}, []

    def stub(self, address):
        def ping(request, timeout=None, metadata=None):
            self.calls.append((address, request, timeout, metadata))
            answer = self.answers[address]
            if isinstance(answer, Exception):
                raise answer
            return answer

        return SimpleNamespace(Ping=ping)

    @property
    def addresses(self):
        return [address for address, *_ in self.calls]


@pytest.fixture
def sandboxes(monkeypatch):
    fake = _Sandboxes()
    monkeypatch.setattr(dispatcher, "_stub", fake.stub)
    return fake


@pytest.fixture(autouse=True)
def _no_cached_channels(monkeypatch):
    """Channels are cached per address for the life of the process: every test starts with none."""
    monkeypatch.setattr(dispatcher, "_channels", {})


@patch("sandbox_status.requests.post")
def test_relays_each_sandbox_ping_as_its_heartbeat(mock_post, sandboxes):
    sandboxes.answers = {"judge-server:8080": _pong("judge-server")}

    sandbox_status.report_once(["http://judge-server:8080"], "t", HEARTBEAT_URL)

    # The ping is gRPC to host:port (no /ping over requests): the sandbox's Ping, with the token as metadata.
    assert sandboxes.calls == [("judge-server:8080", pb.PingRequest(), 3, [("x-judge-server-token", TOKEN_SHA)])]
    beat, = mock_post.call_args_list
    assert beat.args[0] == HEARTBEAT_URL
    assert beat.kwargs["headers"]["X-JUDGE-SERVER-TOKEN"] == TOKEN_SHA
    assert beat.kwargs["json"] == _beat_body("judge-server", "http://judge-server:8080")


@patch("sandbox_status.requests.post")
def test_relay_includes_running_jobs(mock_post, sandboxes):
    sandboxes.answers = {"judge-server:8080": _pong("judge-server", running_jobs=3, max_jobs=8),
                         "judge-server-2:8080": _pong("judge-server-2", running_jobs=0, max_jobs=8)}

    sandbox_status.report_once(["http://judge-server:8080", "http://judge-server-2:8080"], "t", HEARTBEAT_URL)

    busy, idle = (c.kwargs["json"] for c in mock_post.call_args_list)
    assert (busy["running_jobs"], busy["max_jobs"]) == (3, 8)   # the registry reads each sandbox's load...
    assert (idle["running_jobs"], idle["max_jobs"]) == (0, 8)   # ...a 0 included: it is a value, not a missing key


def test_ping_maps_the_response_to_the_server_info_dict(sandboxes):
    sandboxes.answers = {"judge-server:8080": _pong("judge-server", running_jobs=2, max_jobs=4)}

    info = sandbox_status.ping("judge-server:8080", "t", 5)

    assert info == {"hostname": "judge-server", "judger_version": "2.1.1", "cpu_core": 8, "cpu": 7.5,
                    "memory": 55.25, "running_jobs": 2, "max_jobs": 4, "action": "pong"}
    assert sandboxes.calls == [("judge-server:8080", pb.PingRequest(), 5, [("x-judge-server-token", TOKEN_SHA)])]


def _tls_kwargs(tls_dir):
    return {"cert": (str(tls_dir / "tls.crt"), str(tls_dir / "tls.key")), "verify": str(tls_dir / "ca.crt")}


@patch("sandbox_status.requests.post")
def test_heartbeat_uses_tls_kwargs(mock_post, sandboxes, tls_dir):
    sandboxes.answers = {"judge-server:8080": _pong("judge-server")}

    sandbox_status.report_once(["https://judge-server:8080"], "t",
                               "https://submission-service:8000/api/judge_server_heartbeat/",
                               tls_kwargs=tls.requests_kwargs())

    beat, = mock_post.call_args_list
    # submission-service requires a client certificate (CN judge-worker is allow-listed) and is verified against the CA.
    assert beat.kwargs["cert"] == _tls_kwargs(tls_dir)["cert"]
    assert beat.kwargs["verify"] == _tls_kwargs(tls_dir)["verify"]


@patch("sandbox_status.requests.post")
def test_ping_dials_the_sandbox_over_a_channel_with_the_workers_certificate(mock_post, tls_dir):
    credentials = grpc.ssl_channel_credentials()
    with patch("dispatcher.tls.grpc_channel_credentials", return_value=credentials), \
            patch("dispatcher.grpc.secure_channel") as secure, patch("dispatcher.grpc.insecure_channel") as insecure:
        secure.return_value.unary_unary.return_value = lambda request, timeout, metadata: _pong("judge-server")

        sandbox_status.report_once(["https://judge-server:8080"], "t", HEARTBEAT_URL, tls_kwargs=tls.requests_kwargs())

    # The sandbox requires the worker's certificate and is verified against the CA: gRPC, no longer requests' cert/verify.
    insecure.assert_not_called()
    assert secure.call_args.args == ("judge-server:8080", credentials)
    assert up("judge-server") == 1


def test_a_vanished_certificate_marks_every_sandbox_down(tls_dir):
    urls = ["https://judge-server:8080", "https://judge-server-2:8080"]
    for url in urls:
        sandbox_health.mark(url, True)
    settings = tls.requests_kwargs()  # read once at start...
    (tls_dir / "tls.key").unlink()    # ...then the file goes: no channel can be opened, whichever sandbox it is

    sandbox_status.report_once(urls, "t", HEARTBEAT_URL, tls_kwargs=settings)  # no exception, every sandbox tried

    assert up("judge-server") == 0 and up("judge-server-2") == 0  # SandboxDown can fire; the marks do not freeze


@patch("sandbox_status.requests.post")
def test_a_dead_sandbox_is_skipped_and_the_others_still_reported(mock_post, sandboxes):
    sandboxes.answers = {"judge-server:8080": rpc_error(CODE.UNAVAILABLE, "failed to connect to all addresses"),
                         "judge-server-2:8080": _pong("judge-server-2")}

    sandbox_status.report_once(["http://judge-server:8080", "http://judge-server-2:8080"], "t", HEARTBEAT_URL)

    assert sandboxes.addresses == ["judge-server:8080", "judge-server-2:8080"]
    beat, = mock_post.call_args_list   # only the sandbox that answered is reported
    assert beat.args[0] == HEARTBEAT_URL
    assert beat.kwargs["json"]["service_url"] == "http://judge-server-2:8080"
    assert up("judge-server") == 0 and up("judge-server-2") == 1


@pytest.mark.parametrize("code", [CODE.UNAVAILABLE, CODE.DEADLINE_EXCEEDED, CODE.UNAUTHENTICATED, CODE.INTERNAL])
@patch("sandbox_status.requests.post")
def test_ping_rpc_error_marks_down_and_continues(mock_post, sandboxes, caplog, code):
    sandbox_health.mark("http://judge-server:8080", True)
    sandboxes.answers = {"judge-server:8080": rpc_error(code, "boom"), "judge-server-2:8080": _pong("judge-server-2")}

    with caplog.at_level(logging.WARNING, logger="judge-worker"):
        sandbox_status.report_once(["http://judge-server:8080", "http://judge-server-2:8080"], "t", HEARTBEAT_URL)

    assert up("judge-server") == 0 and up("judge-server-2") == 1   # whatever the code: not answering is down
    assert sandboxes.addresses == ["judge-server:8080", "judge-server-2:8080"]   # the round went on
    assert [c.kwargs["json"]["service_url"] for c in mock_post.call_args_list] == ["http://judge-server-2:8080"]
    assert f"{code.name}: boom" in caplog.text   # the warning says why, one line


@patch("sandbox_status.requests.post")
def test_ping_bad_url_marks_down_and_continues(mock_post, sandboxes, caplog):
    sandbox_health.mark("http://judge-server:8080/ping", True)
    sandboxes.answers = {"judge-server-2:8080": _pong("judge-server-2")}

    with caplog.at_level(logging.WARNING, logger="judge-worker"):
        # The old HTTP form of the URL (a path): not an address to dial.
        sandbox_status.report_once(["http://judge-server:8080/ping", "http://judge-server-2:8080"], "t", HEARTBEAT_URL)

    assert sandboxes.addresses == ["judge-server-2:8080"]   # the bad URL is never dialled
    assert up("judge-server") == 0 and up("judge-server-2") == 1
    assert [c.kwargs["json"]["service_url"] for c in mock_post.call_args_list] == ["http://judge-server-2:8080"]
    assert "not a judge-server address" in caplog.text


@patch("sandbox_status.requests.post")
def test_a_registry_outage_never_raises(mock_post, sandboxes):
    sandboxes.answers = {"judge-server:8080": _pong("judge-server")}
    mock_post.side_effect = requests.ConnectionError("submission-service down")

    sandbox_status.report_once(["http://judge-server:8080"], "t", HEARTBEAT_URL)  # no exception
    assert up("judge-server") == 1  # the registry is down, the sandbox is not


def test_disabled_without_sandboxes():
    cfg = SimpleNamespace(SANDBOX_HEARTBEAT_URL=HEARTBEAT_URL, SANDBOX_HEARTBEAT_INTERVAL_SECONDS=10,
                          JUDGE_SERVER_URLS=[], JUDGE_SERVER_TOKEN="t")
    assert sandbox_status.start(cfg) is None


@patch("sandbox_status.requests.post")
def test_without_a_heartbeat_url_the_sandboxes_are_pinged_and_marked_but_nothing_is_reported(mock_post, sandboxes):
    sandboxes.answers = {"judge-server:8080": _pong("judge-server"),
                         "judge-server-2:8080": rpc_error(CODE.UNAVAILABLE, "down")}

    sandbox_status.report_once(["http://judge-server:8080", "http://judge-server-2:8080"], "t", "")

    assert sandboxes.addresses == ["judge-server:8080", "judge-server-2:8080"]
    mock_post.assert_not_called()
    assert sandbox_health.order(["http://judge-server-2:8080", "http://judge-server:8080"]) == [
        "http://judge-server:8080", "http://judge-server-2:8080"]


class _PingingSandbox(pb_grpc.JudgeServerServicer):
    """Answers Ping with `reply`, recording each call's request and metadata."""

    def __init__(self, reply):
        self.reply, self.calls = reply, []

    def Ping(self, request, context):
        self.calls.append((request, dict(context.invocation_metadata())))
        return self.reply


@patch("sandbox_status.requests.post")
def test_a_real_ping_carries_the_token_over_the_wire_and_maps_the_fields(mock_post, serve):
    sandbox = _PingingSandbox(_pong("judge-server", running_jobs=2, max_jobs=4))
    url = serve(sandbox)

    info = sandbox_status.ping(dispatcher.parse_address(url), "secret", 5)

    assert info == {"hostname": "judge-server", "judger_version": "2.1.1", "cpu_core": 8, "cpu": 7.5,
                    "memory": 55.25, "running_jobs": 2, "max_jobs": 4, "action": "pong"}
    request, metadata = sandbox.calls[0]
    assert request == pb.PingRequest()
    assert metadata["x-judge-server-token"] == hashlib.sha256(b"secret").hexdigest()

    sandbox_status.report_once([url], "secret", HEARTBEAT_URL)   # and a whole round over the same wire
    assert up("127.0.0.1") == 1
    assert mock_post.call_args.kwargs["json"] == _beat_body("judge-server", url, running_jobs=2, max_jobs=4)


def test_start_reports_on_a_daemon_thread():
    reported, park = threading.Event(), threading.Event()  # park is never set: the thread stays in its first round
    cfg = SimpleNamespace(SANDBOX_HEARTBEAT_URL=HEARTBEAT_URL, SANDBOX_HEARTBEAT_INTERVAL_SECONDS=0.01,
                          JUDGE_SERVER_URLS=["http://a:8080"], JUDGE_SERVER_TOKEN="t")
    # (Left looping, it would call the real report_once every 10 ms after the test, and later tests' patches.)
    with patch("sandbox_status.report_once", side_effect=lambda *a, **k: (reported.set(), park.wait())) as report:
        thread = sandbox_status.start(cfg)
        assert reported.wait(2)
    assert thread.daemon
    assert report.call_args.args == (["http://a:8080"], "t", HEARTBEAT_URL)


def _cfg():
    return SimpleNamespace(SANDBOX_HEARTBEAT_URL=HEARTBEAT_URL, SANDBOX_HEARTBEAT_INTERVAL_SECONDS=0.01,
                           JUDGE_SERVER_URLS=["https://a:8080"], JUDGE_SERVER_TOKEN="t")


def test_start_fails_closed_without_the_certificate(tls_dir):
    (tls_dir / "tls.key").unlink()

    with patch("sandbox_status.threading.Thread") as thread, pytest.raises(RuntimeError, match="tls.key"):
        sandbox_status.start(_cfg())
    thread.assert_not_called()  # the worker stops at startup, before any round


def test_start_fails_closed_when_the_grpc_credentials_cannot_be_built(tls_dir):
    with patch("sandbox_status.tls.grpc_channel_credentials", side_effect=OSError("tls.key unreadable")), \
            patch("sandbox_status.threading.Thread") as thread, pytest.raises(OSError, match="unreadable"):
        sandbox_status.start(_cfg())
    thread.assert_not_called()  # the channels to the sandboxes read the same files: a bad one stops the worker here too


def test_start_reads_the_tls_settings_once_for_every_round(tls_dir):
    calls, second, park = [], threading.Event(), threading.Event()  # park is never set: round 2 holds the thread

    def round_(*args, **kwargs):
        calls.append(args)
        if len(calls) == 2:
            second.set()
            park.wait()

    with patch("sandbox_status.tls.requests_kwargs", wraps=tls.requests_kwargs) as settings, \
            patch("sandbox_status.tls.grpc_channel_credentials") as credentials, \
            patch("sandbox_status.report_once", side_effect=round_) as report:
        sandbox_status.start(_cfg())
        assert second.wait(2)
    assert settings.call_count == 1
    assert credentials.call_count == 1   # checked once at start; each channel builds its own from the files at first use
    assert [c.kwargs["tls_kwargs"] for c in report.call_args_list] == [_tls_kwargs(tls_dir)] * 2


def test_a_failing_round_is_logged_and_the_thread_keeps_reporting(caplog):
    calls, second, park = [], threading.Event(), threading.Event()  # park is never set: round 2 holds the thread

    def round_(*args, **kwargs):
        calls.append(args)
        if len(calls) == 1:
            raise OSError("tls.key vanished")  # e.g. requests loading a certificate file that is gone
        second.set()
        park.wait()

    with patch("sandbox_status.report_once", side_effect=round_):
        thread = sandbox_status.start(_cfg())
        assert second.wait(2), "the thread died with the first round's exception"
    assert thread.is_alive()
    assert "tls.key vanished" in caplog.text
