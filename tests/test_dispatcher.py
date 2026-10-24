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


# --- SP13: a checker problem's request carries qduoj's four spj fields (submission-service sends all four or none) ---

_PLAIN_EVENT = {
    "submissionId": "sub-1",
    "src": "code",
    "language_config": {"compile": {}, "run": {}},
    "max_cpu_time": 1000,
    "max_memory": 268435456,
    "test_case_id": "prob__abc",
    "output": True,
    "rejudgeCount": 0,
}
_PLAIN_BODY = {
    "src": "code",
    "language_config": {"compile": {}, "run": {}},
    "max_cpu_time": 1000,
    "max_memory": 268435456,
    "test_case_id": "prob__abc",
    "output": True,
}
_SPJ = {
    "spj_version": "ab12",
    "spj_config": {"exe_name": "spj-{spj_version}", "command": "{exe_path} {in_file_path} {user_out_file_path}",
                   "seccomp_rule": "c_cpp"},
    "spj_compile_config": {"env": [], "src_name": "spj-{spj_version}.cpp", "exe_name": "spj-{spj_version}",
                           "max_cpu_time": 10000, "max_real_time": 20000, "max_memory": 1073741824,
                           "compile_command": "/usr/bin/g++ -DONLINE_JUDGE -O2 -w -fmax-errors=3 -std=c++17 "
                                              "{src_path} -lm -o {exe_path}"},
    "spj_src": "int main(){}",
}


def test_build_judge_body_passes_spj_fields():
    body = dispatcher.build_judge_body({**_PLAIN_EVENT, **_SPJ})
    # qduoj's JudgeServer.judge(**body) takes these four as keyword arguments, verbatim.
    assert body == {**_PLAIN_BODY, **_SPJ}


def test_build_judge_body_omits_absent_spj_fields():
    assert dispatcher.build_judge_body(_PLAIN_EVENT) == _PLAIN_BODY
    assert dispatcher.build_judge_body({**_PLAIN_EVENT, "spj_version": None}) == _PLAIN_BODY


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
    assert "cert" not in kwargs and "verify" not in kwargs  # TLS off: requests' defaults


@patch("dispatcher.requests.post")
def test_judge_call_passes_tls_kwargs(mock_post, tls_dir):
    mock_post.return_value.json.return_value = {"err": None, "data": []}

    dispatcher.call_judge_server("https://judge-server:8080", "secret", {"src": "x"}, timeout=30)

    kwargs = mock_post.call_args.kwargs
    # The sandbox requires the worker's certificate and is verified against the CA (requests checks the host name too).
    assert kwargs["cert"] == (str(tls_dir / "tls.crt"), str(tls_dir / "tls.key"))
    assert kwargs["verify"] == str(tls_dir / "ca.crt")


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
def test_dispatch_does_not_fail_over_on_read_timeout(mock_call):
    # The sandbox accepted the request and is still judging: re-sending it would judge the same
    # submission twice, on the other sandbox, exactly when both are loaded.
    dispatcher.reset_round_robin()
    mock_call.side_effect = [requests.ReadTimeout("still judging"), {"err": None, "data": []}]
    with pytest.raises(requests.ReadTimeout):
        dispatcher.dispatch_judge(["http://a:8080", "http://b:8080"], "t", {}, timeout=5)
    assert mock_call.call_count == 1


@patch("dispatcher.call_judge_server")
def test_dispatch_fails_over_on_connect_timeout(mock_call):
    # ConnectTimeout is a Timeout too, but the sandbox never got the request — still worth failing over.
    dispatcher.reset_round_robin()
    mock_call.side_effect = [requests.ConnectTimeout("unreachable"), {"err": None, "data": []}]
    out = dispatcher.dispatch_judge(["http://a:8080", "http://b:8080"], "t", {}, timeout=5)
    assert out == {"err": None, "data": []}
    assert mock_call.call_count == 2


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


# --- sub-project 6: healthy sandboxes first, dispatch counts ---

from prometheus_client import REGISTRY  # noqa: E402

import sandbox_health  # noqa: E402

A, B = "http://a:8080", "http://b:8080"


def dispatches(sandbox, outcome):
    return REGISTRY.get_sample_value("oj_judge_dispatch_total", {"sandbox": sandbox, "outcome": outcome}) or 0


@patch("dispatcher.call_judge_server")
def test_a_sandbox_that_cannot_be_reached_is_marked_down(mock_call):
    dispatcher.reset_round_robin()
    mock_call.side_effect = [requests.ConnectionError("refused"), {"err": None, "data": []}]
    dispatcher.dispatch_judge([A, B], "t", {}, timeout=5)
    assert sandbox_health.order([A, B]) == [B, A]


@patch("dispatcher.call_judge_server")
def test_after_a_sandbox_is_marked_down_no_dispatch_goes_to_it(mock_call):
    # E4 c3: the round-robin start landed on the dead sandbox for every other submission.
    dispatcher.reset_round_robin()
    sandbox_health.mark(A, False)
    mock_call.return_value = {"err": None, "data": []}
    for _ in range(4):
        dispatcher.dispatch_judge([A, B], "t", {}, timeout=5)
    assert [c.args[0] for c in mock_call.call_args_list] == [B] * 4


@patch("dispatcher.call_judge_server")
def test_when_every_sandbox_is_marked_down_they_are_still_tried(mock_call):
    dispatcher.reset_round_robin()
    sandbox_health.mark(A, False)
    sandbox_health.mark(B, False)
    mock_call.return_value = {"err": None, "data": []}
    assert dispatcher.dispatch_judge([A, B], "t", {}, timeout=5) == {"err": None, "data": []}
    assert mock_call.call_args.args[0] == A
    assert sandbox_health.order([B, A]) == [A, B]   # A judged it, so A is healthy again


@patch("dispatcher.call_judge_server")
def test_a_4xx_or_a_read_timeout_does_not_mark_the_sandbox_down(mock_call):
    # Both mean the sandbox answered: it is alive.
    dispatcher.reset_round_robin()
    mock_call.side_effect = [_http_error(400)]
    with pytest.raises(requests.HTTPError):
        dispatcher.dispatch_judge([A, B], "t", {}, timeout=5)
    dispatcher.reset_round_robin()
    mock_call.side_effect = [requests.ReadTimeout("still judging")]
    with pytest.raises(requests.ReadTimeout):
        dispatcher.dispatch_judge([A, B], "t", {}, timeout=5)
    assert sandbox_health.order([A, B]) == [A, B]


@patch("dispatcher.call_judge_server")
def test_dispatches_are_counted_by_sandbox_and_outcome(mock_call):
    dispatcher.reset_round_robin()
    mock_call.side_effect = [requests.ConnectionError("refused"), {"err": None, "data": []}]
    ok_b, failed_a = dispatches("b", "ok"), dispatches("a", "failed")
    dispatcher.dispatch_judge([A, B], "t", {}, timeout=5)
    assert dispatches("b", "ok") == ok_b + 1
    assert dispatches("a", "failed") == failed_a + 1


def test_build_judge_body_sends_inline_cases_instead_of_a_bundle():
    # A run (Run code design §8): the cases travel in the request, so qduoj's judge() gets test_case and no test_case_id.
    event = {**_PLAIN_EVENT, "runId": "r1",
             "test_case": [{"input": "1 2\n", "output": "3\n"}, {"input": "5\n", "output": ""}]}
    del event["test_case_id"]
    body = dispatcher.build_judge_body(event)
    assert body["test_case"] == event["test_case"]
    assert "test_case_id" not in body
    assert body["output"] is True
