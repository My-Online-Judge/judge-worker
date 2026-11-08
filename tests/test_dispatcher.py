import hashlib
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import grpc
import pytest
from prometheus_client import REGISTRY

import dispatcher
import handler
import judge_server_pb2 as pb
import sandbox_health
import verdict
from tests.rpc_errors import rpc_error

CODE = grpc.StatusCode

# A judge response carries every case's capped output: room for the biggest bundle. A sandbox that was restarted is
# dialled again within seconds (gRPC's own reconnect backoff would grow to 120 s).
CHANNEL_OPTIONS = [("grpc.max_receive_message_length", 64 * 1024 * 1024),
                   ("grpc.initial_reconnect_backoff_ms", 1000), ("grpc.max_reconnect_backoff_ms", 5000)]


def test_token_header_is_sha256_hex():
    assert dispatcher.token_header("abc") == hashlib.sha256(b"abc").hexdigest()


# --- parse_address: the configured URL (JUDGE_SERVER_URL) as a gRPC target ---

@pytest.mark.parametrize("url, address", [
    ("https://judge-server:8080", "judge-server:8080"),
    ("http://judge-server:8080", "judge-server:8080"),
    ("judge-server:8080", "judge-server:8080"),
])
def test_parse_address(url, address):
    assert dispatcher.parse_address(url) == address


@pytest.mark.parametrize("url", ["https://judge-server:8080/judge", "http://a:8080/", "http://", "https://:8080", "",
                                 "http://a:notaport"])
def test_parse_address_rejects_a_path_an_empty_host_or_a_bad_port(url):
    with pytest.raises(ValueError):
        dispatcher.parse_address(url)


def test_parse_address_is_the_helper_config_validates_with():
    # One parser for both: Config.validate refuses at start what dispatch_judge would refuse per submission.
    import addresses
    assert dispatcher.parse_address is addresses.parse_address


# --- build_judge_request: the event's qduoj-shaped fields as the proto's JudgeRequest ---

_LANGUAGE_CONFIG = {
    "compile": {"src_name": "main.cpp", "exe_name": "main", "max_cpu_time": 3000, "max_real_time": 5000,
                "max_memory": -1, "compile_command": "/usr/bin/g++ {src_path} -o {exe_path}",
                "env": ["LANG=en_US.UTF-8"]},
    "run": {"command": "{exe_path}", "seccomp_rule": "c_cpp", "env": ["LANG=en_US.UTF-8", "A=b"],
            "memory_limit_check_only": 0},
}
_PLAIN_EVENT = {
    "submissionId": "sub-1",
    "src": "code",
    "language_config": _LANGUAGE_CONFIG,
    "max_cpu_time": 1000,
    "max_memory": 268435456,
    "test_case_id": "prob__abc",
    "output": True,
    "rejudgeCount": 0,
}
# A checker problem's checker, under qduoj's four spj field names (submission-service sends all four or none).
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


def test_build_judge_request_maps_fields():
    request = dispatcher.build_judge_request(_PLAIN_EVENT)
    assert isinstance(request, pb.JudgeRequest)
    assert request.src == "code"
    assert request.max_cpu_time_ms == 1000
    assert request.max_memory_bytes == 268435456
    assert request.test_case_id == "prob__abc"
    assert request.WhichOneof("cases") == "test_case_id"
    assert request.output is True
    assert request.HasField("spj") is False


def test_build_judge_request_maps_the_language_config():
    lc = dispatcher.build_judge_request(_PLAIN_EVENT).language_config
    # Times in ms, memory in bytes; a -1 (no limit) stays -1.
    assert (lc.compile.src_name, lc.compile.exe_name) == ("main.cpp", "main")
    assert (lc.compile.max_cpu_time_ms, lc.compile.max_real_time_ms, lc.compile.max_memory_bytes) == (3000, 5000, -1)
    assert lc.compile.compile_command == "/usr/bin/g++ {src_path} -o {exe_path}"
    assert list(lc.compile.env) == ["LANG=en_US.UTF-8"]
    assert (lc.run.command, lc.run.seccomp_rule) == ("{exe_path}", "c_cpp")
    assert list(lc.run.env) == ["LANG=en_US.UTF-8", "A=b"]
    assert lc.run.memory_limit_check_only is False


@pytest.mark.parametrize("flag, expected", [(1, True), (0, False), (None, False)])
def test_build_judge_request_memory_limit_check_only_is_a_bool(flag, expected):
    event = {**_PLAIN_EVENT, "language_config": {"run": {"command": "java", "memory_limit_check_only": flag}}}
    assert dispatcher.build_judge_request(event).language_config.run.memory_limit_check_only is expected


def test_build_judge_request_no_seccomp_rule_is_an_empty_string():
    # qduoj's java/js configs say seccomp_rule null: the proto's "no filter" is "".
    event = {**_PLAIN_EVENT, "language_config": {"run": {"command": "node x", "seccomp_rule": None}}}
    assert dispatcher.build_judge_request(event).language_config.run.seccomp_rule == ""


def test_build_judge_request_tolerates_an_empty_language_config():
    request = dispatcher.build_judge_request({**_PLAIN_EVENT, "language_config": {}})
    assert request.src == "code"
    assert request.language_config.compile.compile_command == ""


@pytest.mark.parametrize("event, expected", [({}, True), ({"output": True}, True), ({"output": False}, False)])
def test_build_judge_request_output_defaults_to_true(event, expected):
    base = {k: v for k, v in _PLAIN_EVENT.items() if k != "output"}
    assert dispatcher.build_judge_request({**base, **event}).output is expected


def test_build_judge_request_spj():
    request = dispatcher.build_judge_request({**_PLAIN_EVENT, **_SPJ})
    assert request.HasField("spj") is True
    assert request.spj.version == "ab12"
    assert request.spj.src == "int main(){}"
    assert request.spj.compile.src_name == "spj-{spj_version}.cpp"
    assert request.spj.compile.exe_name == "spj-{spj_version}"
    assert (request.spj.compile.max_cpu_time_ms, request.spj.compile.max_real_time_ms,
            request.spj.compile.max_memory_bytes) == (10000, 20000, 1073741824)
    assert request.spj.compile.compile_command.startswith("/usr/bin/g++ -DONLINE_JUDGE")
    assert request.spj.exe_name == "spj-{spj_version}"
    assert request.spj.command == "{exe_path} {in_file_path} {user_out_file_path}"
    assert request.spj.seccomp_rule == "c_cpp"


def test_build_judge_request_omits_an_absent_checker():
    assert dispatcher.build_judge_request({**_PLAIN_EVENT, "spj_version": None}).HasField("spj") is False


def test_build_judge_request_inline():
    # A run (Run code design §8): the cases travel in the request, and there is no bundle to name.
    event = {**_PLAIN_EVENT, "runId": "r1",
             "test_case": [{"input": "1 2\n", "output": "3\n"}, {"input": "5\n", "output": "10\n"}]}
    del event["test_case_id"]
    request = dispatcher.build_judge_request(event)
    assert request.WhichOneof("cases") == "test_case"
    assert [(c.input, c.output) for c in request.test_case.cases] == [("1 2\n", "3\n"), ("5\n", "10\n")]
    assert request.test_case.cases[1].output == "10\n"
    assert request.output is True


def test_build_judge_request_inline_case_without_an_output_has_an_empty_one():
    cases = [{"input": "1"}, {"input": "2", "output": None}]
    request = dispatcher.build_judge_request({**_PLAIN_EVENT, "test_case": cases})
    assert [(c.input, c.output) for c in request.test_case.cases] == [("1", ""), ("2", "")]


def test_build_judge_request_empty_inline_cases_still_choose_inline():
    # The sandbox answers INVALID_ARGUMENT "test_case has no cases", not "exactly one of ... is required".
    request = dispatcher.build_judge_request({**_PLAIN_EVENT, "test_case": []})
    assert request.WhichOneof("cases") == "test_case"
    assert len(request.test_case.cases) == 0


@pytest.mark.parametrize("missing", ["src", "language_config", "max_cpu_time", "max_memory"])
def test_build_judge_request_requires_the_judge_fields(missing):
    event = {k: v for k, v in _PLAIN_EVENT.items() if k != missing}
    with pytest.raises(KeyError):
        dispatcher.build_judge_request(event)


# --- response_to_dict: JudgeResponse as the dict handler.build_judged_event reads ---

def _case(**kwargs):
    return pb.CaseResult(**{"test_case": "1", "output_md5": "d41d8cd98f00b204e9800998ecf8427e", **kwargs})


def _results(*cases):
    return pb.JudgeResponse(results=pb.CaseResults(cases=cases))


def test_response_to_dict_results():
    resp = _results(
        _case(test_case="1", result=0, error=0, cpu_time_ms=5, real_time_ms=6, memory_bytes=1024, signal=0, exit_code=0,
              output=b"3\n"),
        _case(test_case="2", result=-1, error=0, cpu_time_ms=9, real_time_ms=12, memory_bytes=2048, signal=11,
              exit_code=1, output=b"4\n", output_truncated=True))
    out = dispatcher.response_to_dict(resp)
    assert out["err"] is None
    assert len(out["data"]) == 2
    assert out["data"][0] == {"cpu_time": 5, "real_time": 6, "memory": 1024, "signal": 0, "exit_code": 0, "error": 0,
                              "result": 0, "test_case": "1", "output_md5": "d41d8cd98f00b204e9800998ecf8427e",
                              "output": "3\n"}
    second = out["data"][1]
    assert (second["result"], second["signal"], second["exit_code"], second["output"]) == (-1, 11, 1, "4\n")
    assert all(isinstance(second[k], int) for k in
               ("cpu_time", "real_time", "memory", "signal", "exit_code", "error", "result"))
    assert isinstance(second["output"], str)


def test_response_to_dict_without_output_has_an_empty_string():
    assert dispatcher.response_to_dict(_results(_case(result=0)))["data"][0]["output"] == ""


def test_response_to_dict_non_utf8_output():
    out = dispatcher.response_to_dict(_results(_case(result=0, output=b"\xff\xfe")))["data"][0]["output"]
    assert isinstance(out, str)
    assert "�" in out


def test_response_to_dict_compile_error():
    resp = pb.JudgeResponse(compile_error=pb.CompileError(message="msg"))
    assert dispatcher.response_to_dict(resp) == {"err": "CompileError", "data": "msg"}


def test_response_to_dict_spj_compile_error():
    resp = pb.JudgeResponse(spj_compile_error=pb.CompileError(message="spj.cpp:3: error"))
    assert dispatcher.response_to_dict(resp) == {"err": "SPJCompileError", "data": "spj.cpp:3: error"}


def test_response_to_dict_keeps_short_result_list():
    # 1 of 3: the converter does not pad or reject; the verdict layer refuses an incomplete judgement.
    d = dispatcher.response_to_dict(_results(_case(result=0)))
    assert len(d["data"]) == 1
    assert verdict.incomplete_reason(d["data"], 3) is not None


def test_response_to_dict_empty_results_is_an_empty_list():
    assert dispatcher.response_to_dict(_results()) == {"err": None, "data": []}


def test_response_to_dict_unset_outcome_is_unexpected():
    out = dispatcher.response_to_dict(pb.JudgeResponse())
    assert out["err"] == "UnexpectedResponse"
    assert isinstance(out["data"], str) and out["data"]


def test_the_handler_reads_the_converted_responses_unchanged():
    ok = handler.build_judged_event("s", dispatcher.response_to_dict(_results(
        _case(result=0, cpu_time_ms=5, real_time_ms=6, memory_bytes=100),
        _case(test_case="2", result=0, cpu_time_ms=9, real_time_ms=2, memory_bytes=400))), expected_count=2)
    assert (ok["status"], ok["cpu_time"], ok["real_time"], ok["memory"]) == (verdict.ACCEPTED, 9, 6, 400)
    assert ok["error_message"] is None and len(ok["details"]) == 2

    compile_error = handler.build_judged_event("s", dispatcher.response_to_dict(
        pb.JudgeResponse(compile_error=pb.CompileError(message="line 3: syntax error"))))
    assert compile_error["status"] == verdict.COMPILE_ERROR
    assert compile_error["error_message"] == "CompileError: line 3: syntax error"

    spj = handler.build_judged_event("s", dispatcher.response_to_dict(
        pb.JudgeResponse(spj_compile_error=pb.CompileError(message="spj.cpp:3: error"))))
    assert spj["status"] == verdict.SYSTEM_ERROR
    assert "spj.cpp:3" not in spj["error_message"]

    unset = handler.build_judged_event("s", dispatcher.response_to_dict(pb.JudgeResponse()))
    assert unset["status"] == verdict.SYSTEM_ERROR


# --- the channel and the call ---

@pytest.fixture(autouse=True)
def _no_cached_channels(monkeypatch):
    """Channels are cached per address for the life of the process: every test starts with none."""
    monkeypatch.setattr(dispatcher, "_channels", {})


def fake_stubs(monkeypatch, replies):
    """Patch dispatcher._stub(address): replies maps an address to what its Judge returns, or raises when it is an
    exception. Returns each address's Judge mock."""
    judges = {}
    for address, reply in replies.items():
        judge = MagicMock(name=f"Judge@{address}")
        if isinstance(reply, Exception):
            judge.side_effect = reply
        else:
            judge.return_value = reply
        judges[address] = judge
    monkeypatch.setattr(dispatcher, "_stub", lambda address: SimpleNamespace(Judge=judges[address]))
    return judges


def test_call_sends_token_metadata(monkeypatch):
    judges = fake_stubs(monkeypatch, {"js:8080": _results(_case(result=0))})
    request = pb.JudgeRequest(src="x")

    out = dispatcher.call_judge_server("js:8080", "secret", request, timeout=7)

    assert out["err"] is None and out["data"][0]["result"] == 0
    assert judges["js:8080"].call_args == call(
        request, timeout=7, metadata=[("x-judge-server-token", hashlib.sha256(b"secret").hexdigest())])


def test_call_judge_server_raises_the_rpc_error(monkeypatch):
    fake_stubs(monkeypatch, {"js:8080": rpc_error(CODE.UNAVAILABLE, "down")})
    with pytest.raises(grpc.RpcError):
        dispatcher.call_judge_server("js:8080", "t", pb.JudgeRequest(), timeout=7)


def test_stub_keeps_one_plaintext_channel_per_address():
    with patch("dispatcher.grpc.insecure_channel") as insecure, patch("dispatcher.grpc.secure_channel") as secure:
        dispatcher._stub("a:8080")
        dispatcher._stub("a:8080")
        dispatcher._stub("b:8080")
    assert [c.args[0] for c in insecure.call_args_list] == ["a:8080", "b:8080"]   # TLS off: plaintext
    secure.assert_not_called()
    assert insecure.call_args.kwargs["options"] == CHANNEL_OPTIONS


def test_stub_uses_a_secure_channel_with_the_workers_certificate(tls_dir):
    with patch("dispatcher.grpc.secure_channel") as secure, patch("dispatcher.grpc.insecure_channel") as insecure:
        dispatcher._stub("judge-server:8080")
        dispatcher._stub("judge-server:8080")
    insecure.assert_not_called()                       # TLS on never falls back to plaintext
    assert secure.call_count == 1
    address, credentials = secure.call_args.args
    assert address == "judge-server:8080"
    assert isinstance(credentials, grpc.ChannelCredentials)
    assert secure.call_args.kwargs["options"] == CHANNEL_OPTIONS


def test_stub_with_a_missing_certificate_raises_and_caches_nothing(tls_dir):
    (tls_dir / "tls.key").unlink()
    with pytest.raises(RuntimeError, match="tls.key"):
        dispatcher._stub("judge-server:8080")
    assert dispatcher._channels == {}


# --- which gRPC failures are worth failing over ---

@pytest.mark.parametrize("code, retryable, marks_down", [
    (CODE.UNAVAILABLE, True, True),             # not up, TLS handshake failed
    (CODE.INTERNAL, True, True),                # judger failure before any case ran
    (CODE.RESOURCE_EXHAUSTED, True, False),     # busy, not down: try the next one, leave this one in rotation
    (CODE.DEADLINE_EXCEEDED, False, False),     # still judging (the server cancels it): a retry would judge it twice
    (CODE.INVALID_ARGUMENT, False, False),      # the same request fails everywhere
    (CODE.NOT_FOUND, False, False),
    (CODE.UNAUTHENTICATED, False, False),
    (CODE.PERMISSION_DENIED, False, False),
    (CODE.CANCELLED, False, False),
])
def test_retryable_and_marks_down_by_status_code(code, retryable, marks_down):
    exc = rpc_error(code)
    assert dispatcher._is_retryable(exc) is retryable
    assert dispatcher._marks_down(exc) is marks_down


@pytest.mark.parametrize("exc", [ValueError("x"), RuntimeError("tls"), grpc.RpcError()])
def test_anything_but_a_coded_rpc_error_is_neither(exc):
    assert dispatcher._is_retryable(exc) is False
    assert dispatcher._marks_down(exc) is False


# --- T1-3: multi judge-server dispatch (round-robin + failover) ---

A, B, C = "http://a:8080", "http://b:8080", "http://c:8080"
ADDR_A, ADDR_B = "a:8080", "b:8080"
OK = {"err": None, "data": []}
REQUEST = pb.JudgeRequest(src="x")


@patch("dispatcher.call_judge_server")
def test_dispatch_uses_first_url_on_success(mock_call):
    dispatcher.reset_round_robin()
    mock_call.return_value = OK
    out = dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)
    assert out == OK
    # The sandbox's address, not its URL: the call is made over gRPC.
    assert mock_call.call_args_list == [call(ADDR_A, "t", REQUEST, timeout=5)]


@patch("dispatcher.call_judge_server")
def test_dispatch_accepts_scheme_less_addresses(mock_call):
    dispatcher.reset_round_robin()
    mock_call.return_value = OK
    dispatcher.dispatch_judge(["judge-server:8080"], "t", REQUEST, timeout=5)
    assert mock_call.call_args.args[0] == "judge-server:8080"


def test_dispatch_fails_over_on_unavailable(monkeypatch):
    dispatcher.reset_round_robin()
    judges = fake_stubs(monkeypatch, {ADDR_A: rpc_error(CODE.UNAVAILABLE, "refused"),
                                      ADDR_B: _results(_case(result=0))})

    out = dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)

    assert out["data"][0]["result"] == 0
    assert judges[ADDR_A].call_count == 1 and judges[ADDR_B].call_count == 1
    assert sandbox_health.order([A, B]) == [B, A]      # the first is marked down


def test_dispatch_fails_over_on_internal(monkeypatch):
    dispatcher.reset_round_robin()
    judges = fake_stubs(monkeypatch, {ADDR_A: rpc_error(CODE.INTERNAL, "judger"), ADDR_B: _results(_case(result=0))})
    dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)
    assert judges[ADDR_B].call_count == 1
    assert sandbox_health.order([A, B]) == [B, A]


def test_dispatch_busy_tries_next_without_marking_down(monkeypatch):
    dispatcher.reset_round_robin()
    judges = fake_stubs(monkeypatch, {ADDR_A: rpc_error(CODE.RESOURCE_EXHAUSTED, "queue full"),
                                      ADDR_B: _results(_case(result=0))})

    out = dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)

    assert out["data"][0]["result"] == 0
    assert judges[ADDR_A].call_count == 1 and judges[ADDR_B].call_count == 1
    assert sandbox_health.order([A, B]) == [A, B]      # a busy sandbox is alive: it stays first in line


def test_dispatch_invalid_argument_raises_at_once(monkeypatch):
    dispatcher.reset_round_robin()
    judges = fake_stubs(monkeypatch, {ADDR_A: rpc_error(CODE.INVALID_ARGUMENT, "unknown seccomp rule"),
                                      ADDR_B: _results(_case(result=0))})

    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)

    assert caught.value.code() == CODE.INVALID_ARGUMENT
    assert judges[ADDR_B].call_count == 0               # the same request fails everywhere
    assert sandbox_health.order([A, B]) == [A, B]       # the sandbox answered: it is alive


def test_dispatch_deadline_exceeded_raises_at_once(monkeypatch):
    # The sandbox has the request and is still judging (it cancels the job when the call ends): re-sending it would
    # judge the same submission twice, on the other sandbox, exactly when both are loaded.
    dispatcher.reset_round_robin()
    judges = fake_stubs(monkeypatch, {ADDR_A: rpc_error(CODE.DEADLINE_EXCEEDED), ADDR_B: _results(_case(result=0))})

    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)

    assert caught.value.code() == CODE.DEADLINE_EXCEEDED
    assert judges[ADDR_B].call_count == 0
    assert sandbox_health.order([A, B]) == [A, B]


@pytest.mark.parametrize("code", [CODE.UNAUTHENTICATED, CODE.PERMISSION_DENIED, CODE.NOT_FOUND])
def test_dispatch_raises_at_once_on_any_other_status(monkeypatch, code):
    dispatcher.reset_round_robin()
    judges = fake_stubs(monkeypatch, {ADDR_A: rpc_error(code), ADDR_B: _results(_case(result=0))})
    with pytest.raises(grpc.RpcError):
        dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)
    assert judges[ADDR_B].call_count == 0


@pytest.mark.parametrize("code", [CODE.UNAVAILABLE, CODE.RESOURCE_EXHAUSTED])
def test_dispatch_raises_the_last_error_when_every_sandbox_fails(monkeypatch, code):
    dispatcher.reset_round_robin()
    judges = fake_stubs(monkeypatch, {ADDR_A: rpc_error(code, "a"), ADDR_B: rpc_error(code, "b")})

    with pytest.raises(grpc.RpcError) as caught:
        dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)

    assert caught.value.details() == "b"
    assert judges[ADDR_A].call_count == 1 and judges[ADDR_B].call_count == 1


def test_dispatch_does_not_catch_what_is_not_an_rpc_error(monkeypatch):
    dispatcher.reset_round_robin()
    judges = fake_stubs(monkeypatch, {ADDR_A: RuntimeError("OJ_TLS_DIR is set but x is missing"),
                                      ADDR_B: _results(_case(result=0))})
    with pytest.raises(RuntimeError):
        dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)
    assert judges[ADDR_B].call_count == 0


@patch("dispatcher.call_judge_server")
def test_dispatch_round_robin_rotates_start(mock_call):
    dispatcher.reset_round_robin()
    mock_call.return_value = OK
    for _ in range(3):
        dispatcher.dispatch_judge([A, B, C], "t", REQUEST, timeout=5)
    assert [c.args[0] for c in mock_call.call_args_list] == [ADDR_A, ADDR_B, "c:8080"]


def test_dispatch_empty_urls_raises():
    with pytest.raises(ValueError):
        dispatcher.dispatch_judge([], "t", REQUEST, timeout=5)


@patch("dispatcher.call_judge_server")
def test_dispatch_rejects_a_malformed_url_before_calling_any_sandbox(mock_call):
    # A misconfiguration, found the same way whatever the round-robin start is.
    mock_call.return_value = OK
    for _ in range(2):
        with pytest.raises(ValueError):
            dispatcher.dispatch_judge([A, "http://b:8080/judge"], "t", REQUEST, timeout=5)
    mock_call.assert_not_called()


# --- sub-project 6: healthy sandboxes first, dispatch counts ---

def dispatches(sandbox, outcome):
    return REGISTRY.get_sample_value("oj_judge_dispatch_total", {"sandbox": sandbox, "outcome": outcome}) or 0


@patch("dispatcher.call_judge_server")
def test_after_a_sandbox_is_marked_down_no_dispatch_goes_to_it(mock_call):
    # E4 c3: the round-robin start landed on the dead sandbox for every other submission.
    dispatcher.reset_round_robin()
    sandbox_health.mark(A, False)
    mock_call.return_value = OK
    for _ in range(4):
        dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)
    assert [c.args[0] for c in mock_call.call_args_list] == [ADDR_B] * 4


@patch("dispatcher.call_judge_server")
def test_when_every_sandbox_is_marked_down_they_are_still_tried(mock_call):
    dispatcher.reset_round_robin()
    sandbox_health.mark(A, False)
    sandbox_health.mark(B, False)
    mock_call.return_value = OK
    assert dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5) == OK
    assert mock_call.call_args.args[0] == ADDR_A
    assert sandbox_health.order([B, A]) == [A, B]   # A judged it, so A is healthy again


def test_dispatches_are_counted_by_sandbox_and_outcome(monkeypatch):
    dispatcher.reset_round_robin()
    fake_stubs(monkeypatch, {ADDR_A: rpc_error(CODE.UNAVAILABLE, "refused"), ADDR_B: _results(_case(result=0))})
    ok_b, failed_a = dispatches("b", "ok"), dispatches("a", "failed")
    dispatcher.dispatch_judge([A, B], "t", REQUEST, timeout=5)
    assert dispatches("b", "ok") == ok_b + 1
    assert dispatches("a", "failed") == failed_a + 1


def test_a_refused_request_is_counted_as_failed_too(monkeypatch):
    dispatcher.reset_round_robin()
    fake_stubs(monkeypatch, {ADDR_A: rpc_error(CODE.INVALID_ARGUMENT, "bad")})
    failed_a = dispatches("a", "failed")
    with pytest.raises(grpc.RpcError):
        dispatcher.dispatch_judge([A], "t", REQUEST, timeout=5)
    assert dispatches("a", "failed") == failed_a + 1
