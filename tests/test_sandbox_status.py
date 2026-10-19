import hashlib
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

import sandbox_status
from prometheus_client import REGISTRY

import sandbox_health
import tls


def up(sandbox):
    return REGISTRY.get_sample_value("oj_sandbox_up", {"sandbox": sandbox})

HEARTBEAT_URL = "http://submission-service:8000/api/judge_server_heartbeat/"
TOKEN_SHA = hashlib.sha256(b"t").hexdigest()


def _pong(hostname):
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"err": None, "data": {
        "hostname": hostname, "cpu": 7.4, "cpu_core": 8, "memory": 55.4, "judger_version": "2.1.1",
        "action": "pong"}}
    return resp


def _ok():
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    return resp


@patch("sandbox_status.requests.post")
def test_relays_each_sandbox_ping_as_its_heartbeat(mock_post):
    mock_post.side_effect = [_pong("judge-server"), _ok()]

    sandbox_status.report_once(["http://judge-server:8080"], "t", HEARTBEAT_URL)

    ping, beat = mock_post.call_args_list
    assert ping.args[0] == "http://judge-server:8080/ping"
    assert ping.kwargs["headers"]["X-Judge-Server-Token"] == TOKEN_SHA
    assert beat.args[0] == HEARTBEAT_URL
    assert beat.kwargs["headers"]["X-JUDGE-SERVER-TOKEN"] == TOKEN_SHA
    assert beat.kwargs["json"] == {
        "hostname": "judge-server", "cpu": 7.4, "cpu_core": 8, "memory": 55.4, "judger_version": "2.1.1",
        "action": "heartbeat", "service_url": "http://judge-server:8080"}


def _tls_kwargs(tls_dir):
    return {"cert": (str(tls_dir / "tls.crt"), str(tls_dir / "tls.key")), "verify": str(tls_dir / "ca.crt")}


@patch("sandbox_status.requests.post")
def test_heartbeat_uses_tls_kwargs(mock_post, tls_dir):
    mock_post.side_effect = [_pong("judge-server"), _ok()]

    sandbox_status.report_once(["https://judge-server:8080"], "t",
                               "https://submission-service:8000/api/judge_server_heartbeat/",
                               tls_kwargs=tls.requests_kwargs())

    beat = mock_post.call_args_list[1]
    # submission-service requires a client certificate (CN judge-worker is allow-listed) and is verified against the CA.
    assert beat.kwargs["cert"] == _tls_kwargs(tls_dir)["cert"]
    assert beat.kwargs["verify"] == _tls_kwargs(tls_dir)["verify"]


@patch("sandbox_status.requests.post")
def test_ping_passes_tls_kwargs(mock_post, tls_dir):
    mock_post.side_effect = [_pong("judge-server"), _ok()]

    sandbox_status.report_once(["https://judge-server:8080"], "t", HEARTBEAT_URL, tls_kwargs=tls.requests_kwargs())

    ping = mock_post.call_args_list[0]
    # The sandbox requires the worker's certificate (gunicorn --cert-reqs=2) and is verified against the CA.
    assert ping.args[0] == "https://judge-server:8080/ping"
    assert ping.kwargs["cert"] == _tls_kwargs(tls_dir)["cert"]
    assert ping.kwargs["verify"] == _tls_kwargs(tls_dir)["verify"]


@patch("sandbox_status.requests.post")
def test_a_dead_sandbox_is_skipped_and_the_others_still_reported(mock_post):
    mock_post.side_effect = [requests.ConnectionError("down"), _pong("judge-server-2"), _ok()]

    sandbox_status.report_once(["http://judge-server:8080", "http://judge-server-2:8080"], "t", HEARTBEAT_URL)

    assert [c.args[0] for c in mock_post.call_args_list] == [
        "http://judge-server:8080/ping", "http://judge-server-2:8080/ping", HEARTBEAT_URL]
    assert up("judge-server") == 0 and up("judge-server-2") == 1


@patch("sandbox_status.requests.post")
def test_a_registry_outage_never_raises(mock_post):
    mock_post.side_effect = [_pong("judge-server"), requests.ConnectionError("submission-service down")]

    sandbox_status.report_once(["http://judge-server:8080"], "t", HEARTBEAT_URL)  # no exception
    assert up("judge-server") == 1  # the registry is down, the sandbox is not


def test_disabled_without_sandboxes():
    cfg = SimpleNamespace(SANDBOX_HEARTBEAT_URL=HEARTBEAT_URL, SANDBOX_HEARTBEAT_INTERVAL_SECONDS=10,
                          JUDGE_SERVER_URLS=[], JUDGE_SERVER_TOKEN="t")
    assert sandbox_status.start(cfg) is None


@patch("sandbox_status.requests.post")
def test_without_a_heartbeat_url_the_sandboxes_are_pinged_and_marked_but_nothing_is_reported(mock_post):
    mock_post.side_effect = [_pong("judge-server"), requests.ConnectionError("down")]

    sandbox_status.report_once(["http://judge-server:8080", "http://judge-server-2:8080"], "t", "")

    assert [c.args[0] for c in mock_post.call_args_list] == [
        "http://judge-server:8080/ping", "http://judge-server-2:8080/ping"]
    assert sandbox_health.order(["http://judge-server-2:8080", "http://judge-server:8080"]) == [
        "http://judge-server:8080", "http://judge-server-2:8080"]


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


def test_start_reads_the_tls_settings_once_for_every_round(tls_dir):
    calls, second, park = [], threading.Event(), threading.Event()  # park is never set: round 2 holds the thread

    def round_(*args, **kwargs):
        calls.append(args)
        if len(calls) == 2:
            second.set()
            park.wait()

    with patch("sandbox_status.tls.requests_kwargs", wraps=tls.requests_kwargs) as settings, \
            patch("sandbox_status.report_once", side_effect=round_) as report:
        sandbox_status.start(_cfg())
        assert second.wait(2)
    assert settings.call_count == 1
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
