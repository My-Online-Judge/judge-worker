import hashlib
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

import sandbox_status

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


@patch("sandbox_status.requests.post")
def test_a_dead_sandbox_is_skipped_and_the_others_still_reported(mock_post):
    mock_post.side_effect = [requests.ConnectionError("down"), _pong("judge-server-2"), _ok()]

    sandbox_status.report_once(["http://judge-server:8080", "http://judge-server-2:8080"], "t", HEARTBEAT_URL)

    assert [c.args[0] for c in mock_post.call_args_list] == [
        "http://judge-server:8080/ping", "http://judge-server-2:8080/ping", HEARTBEAT_URL]


@patch("sandbox_status.requests.post")
def test_a_registry_outage_never_raises(mock_post):
    mock_post.side_effect = [_pong("judge-server"), requests.ConnectionError("submission-service down")]

    sandbox_status.report_once(["http://judge-server:8080"], "t", HEARTBEAT_URL)  # no exception


def test_disabled_without_a_heartbeat_url():
    cfg = SimpleNamespace(SANDBOX_HEARTBEAT_URL="", SANDBOX_HEARTBEAT_INTERVAL_SECONDS=10,
                          JUDGE_SERVER_URLS=["http://a:8080"], JUDGE_SERVER_TOKEN="t")
    assert sandbox_status.start(cfg) is None


def test_start_reports_on_a_daemon_thread():
    reported = threading.Event()
    cfg = SimpleNamespace(SANDBOX_HEARTBEAT_URL=HEARTBEAT_URL, SANDBOX_HEARTBEAT_INTERVAL_SECONDS=0.01,
                          JUDGE_SERVER_URLS=["http://a:8080"], JUDGE_SERVER_TOKEN="t")
    with patch("sandbox_status.report_once", side_effect=lambda *a, **k: reported.set()) as report:
        thread = sandbox_status.start(cfg)
        assert reported.wait(2)
    assert thread.daemon
    assert report.call_args.args == (["http://a:8080"], "t", HEARTBEAT_URL)
