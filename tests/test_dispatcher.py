import hashlib
from unittest.mock import MagicMock, patch

import pytest
import requests

import dispatcher


def test_token_header_is_sha256_hex():
    assert dispatcher.token_header("abc") == hashlib.sha256(b"abc").hexdigest()


def test_build_judge_body_picks_judge_fields():
    event = {
        "submissionId": "sub-1",
        "src": "code",
        "language_config": {"compile": {}, "run": {}},
        "max_cpu_time": 1000,
        "max_memory": 268435456,
        "test_case_id": "prob",
        "output": True,
    }
    body = dispatcher.build_judge_body(event)
    assert body == {
        "src": "code",
        "language_config": {"compile": {}, "run": {}},
        "max_cpu_time": 1000,
        "max_memory": 268435456,
        "test_case_id": "prob",
        "output": True,
    }
    assert "submissionId" not in body


@patch("dispatcher.requests.post")
def test_call_judge_server_posts_with_token_header(mock_post):
    resp = MagicMock()
    resp.json.return_value = {"err": None, "data": []}
    resp.raise_for_status.return_value = None
    mock_post.return_value = resp

    out = dispatcher.call_judge_server("http://js:8080", "secret", {"src": "x"}, timeout=30)

    assert out == {"err": None, "data": []}
    args, kwargs = mock_post.call_args
    assert args[0] == "http://js:8080/judge"
    assert kwargs["headers"]["X-Judge-Server-Token"] == hashlib.sha256(b"secret").hexdigest()
    assert kwargs["json"] == {"src": "x"}
    assert kwargs["timeout"] == 30


# --- T1-3: multi judge-server dispatch (round-robin + failover) ---

def _http_error(status):
    return requests.HTTPError(response=MagicMock(status_code=status))


@patch("dispatcher.call_judge_server")
def test_dispatch_uses_first_url_on_success(mock_call):
    dispatcher.reset_round_robin()
    mock_call.return_value = {"err": None, "data": []}
    out = dispatcher.dispatch_judge(["http://a:8080", "http://b:8080"], "t", {"src": "x"}, timeout=5)
    assert out == {"err": None, "data": []}
    assert mock_call.call_count == 1
    assert mock_call.call_args.args[0] == "http://a:8080"


@patch("dispatcher.call_judge_server")
def test_dispatch_fails_over_on_connection_error(mock_call):
    dispatcher.reset_round_robin()
    mock_call.side_effect = [requests.ConnectionError("refused"), {"err": None, "data": []}]
    out = dispatcher.dispatch_judge(["http://a:8080", "http://b:8080"], "t", {}, timeout=5)
    assert out == {"err": None, "data": []}
    assert mock_call.call_count == 2
    assert mock_call.call_args_list[1].args[0] == "http://b:8080"


@patch("dispatcher.call_judge_server")
def test_dispatch_fails_over_on_5xx(mock_call):
    dispatcher.reset_round_robin()
    mock_call.side_effect = [_http_error(503), {"err": None, "data": []}]
    out = dispatcher.dispatch_judge(["http://a:8080", "http://b:8080"], "t", {}, timeout=5)
    assert out == {"err": None, "data": []}
    assert mock_call.call_count == 2


@patch("dispatcher.call_judge_server")
def test_dispatch_does_not_fail_over_on_4xx(mock_call):
    # A 4xx is a deterministic client error — the same request fails everywhere, so raise now.
    dispatcher.reset_round_robin()
    mock_call.side_effect = [_http_error(400), {"err": None, "data": []}]
    with pytest.raises(requests.HTTPError):
        dispatcher.dispatch_judge(["http://a:8080", "http://b:8080"], "t", {}, timeout=5)
    assert mock_call.call_count == 1


@patch("dispatcher.call_judge_server")
def test_dispatch_raises_when_all_urls_fail(mock_call):
    dispatcher.reset_round_robin()
    mock_call.side_effect = requests.ConnectionError("down")
    with pytest.raises(requests.ConnectionError):
        dispatcher.dispatch_judge(["http://a:8080", "http://b:8080"], "t", {}, timeout=5)
    assert mock_call.call_count == 2


@patch("dispatcher.call_judge_server")
def test_dispatch_round_robin_rotates_start(mock_call):
    dispatcher.reset_round_robin()
    mock_call.return_value = {"err": None, "data": []}
    urls = ["http://a:8080", "http://b:8080", "http://c:8080"]
    dispatcher.dispatch_judge(urls, "t", {}, timeout=5)
    dispatcher.dispatch_judge(urls, "t", {}, timeout=5)
    dispatcher.dispatch_judge(urls, "t", {}, timeout=5)
    starts = [c.args[0] for c in mock_call.call_args_list]
    assert starts == ["http://a:8080", "http://b:8080", "http://c:8080"]


def test_dispatch_empty_urls_raises():
    with pytest.raises(ValueError):
        dispatcher.dispatch_judge([], "t", {}, timeout=5)
