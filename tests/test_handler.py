import json
import logging
from unittest.mock import patch

import handler
import verdict


def test_build_judged_event_success_array():
    resp = {"err": None, "data": [
        {"error": 0, "result": 0, "cpu_time": 5, "real_time": 6, "memory": 100},
        {"error": 0, "result": 0, "cpu_time": 9, "real_time": 2, "memory": 400},
    ]}
    out = handler.build_judged_event("sub-1", resp)
    assert out["submissionId"] == "sub-1"
    assert out["status"] == verdict.ACCEPTED
    assert out["result"] == 0
    assert out["cpu_time"] == 9
    assert out["real_time"] == 6
    assert out["memory"] == 400
    assert out["error_message"] is None
    assert len(out["details"]) == 2


def test_compile_error_is_still_a_compile_error():
    # The contestant's own program: their verdict, with their compiler's output.
    resp = {"err": "CompileError", "data": "line 3: syntax error"}
    out = handler.build_judged_event("sub-2", resp)
    assert out["status"] == verdict.COMPILE_ERROR
    assert out["error_message"] == "CompileError: line 3: syntax error"
    assert out["details"] == []


def test_spj_compile_error_is_a_system_error_without_the_checker_output(caplog):
    # qduoj's data is the checker's compiler output, which quotes the checker's source.
    resp = {"err": "SPJCompileError", "data": "spj.cpp:3: error: 'expected' was not declared in this scope"}
    with caplog.at_level(logging.WARNING, logger="judge-worker"):
        out = handler.build_judged_event("sub-4", resp)
    assert out["status"] == verdict.SYSTEM_ERROR == 5
    assert out["error_message"] == "SPJCompileError: the problem's checker does not compile"
    assert "spj.cpp:3" not in json.dumps(out)
    assert out["details"] == []
    # The output stays in the worker's log, at WARN.
    assert [r.levelno for r in caplog.records if "spj.cpp:3" in r.getMessage()] == [logging.WARNING]


def test_build_judged_event_unexpected_data_is_system_error():
    resp = {"err": None, "data": None}
    out = handler.build_judged_event("sub-3", resp)
    assert out["status"] == verdict.SYSTEM_ERROR
    assert out["details"] == []


def test_process_event_dispatches_to_judge_servers(monkeypatch):
    event = {"submissionId": "sub-9", "src": "x", "language_config": {},
             "max_cpu_time": 1000, "max_memory": 1, "test_case_id": "p", "output": True}

    class Cfg:
        JUDGE_SERVER_URLS = ["http://js1:8080", "http://js2:8080"]
        JUDGE_SERVER_TOKEN = "t"
        JUDGE_TIMEOUT_SECONDS = 30

    with patch("handler.ensure_present"), \
         patch("handler.dispatch_judge", return_value={"err": None, "data": []}) as m:
        out = handler.process_event(event, Cfg)
    m.assert_called_once()
    # process_event hands the full URL list to the dispatcher (which does round-robin/failover)
    assert m.call_args.args[0] == ["http://js1:8080", "http://js2:8080"]
    assert out["submissionId"] == "sub-9"
    assert out["status"] == verdict.ACCEPTED


def test_checker_requests_get_the_compile_allowance():
    # qduoj compiles a checker inside the first request that needs it on each sandbox (up to 20 s real), on top of the
    # judging itself; a read timeout would end the submission in SYSTEM_ERROR.
    plain = {"submissionId": "sub-10", "src": "x", "language_config": {}, "max_cpu_time": 1000, "max_memory": 1,
             "test_case_id": "p__h", "output": True}
    checker = {**plain, "submissionId": "sub-11", "spj_version": "ab12", "spj_config": {}, "spj_compile_config": {},
               "spj_src": "int main(){}"}

    class Cfg:
        JUDGE_SERVER_URLS = ["http://js1:8080"]
        JUDGE_SERVER_TOKEN = "t"
        JUDGE_TIMEOUT_SECONDS = 30
        SPJ_COMPILE_ALLOWANCE_SECONDS = 20

    with patch("handler.ensure_present"), \
         patch("handler.dispatch_judge", return_value={"err": None, "data": []}) as m:
        handler.process_event(checker, Cfg)
        handler.process_event(plain, Cfg)
    assert [(c.args[2].get("spj_version"), c.kwargs["timeout"]) for c in m.call_args_list] == [("ab12", 50), (None, 30)]


class _Cfg:
    JUDGE_SERVER_URLS = ["http://js1:8080"]
    JUDGE_SERVER_TOKEN = "t"
    JUDGE_TIMEOUT_SECONDS = 30
    SPJ_COMPILE_ALLOWANCE_SECONDS = 20


def test_job_identity_prefers_the_submission_id_then_the_run_id():
    assert handler.job_identity({"submissionId": "s1", "runId": "r1"}) == ("submissionId", "s1")
    assert handler.job_identity({"runId": "r1"}) == ("runId", "r1")
    assert handler.job_identity({"submissionId": ""}) is None
    assert handler.job_identity({}) is None


def test_an_inline_run_neither_syncs_a_bundle_nor_announces_a_start():
    from unittest.mock import MagicMock
    event = {"runId": "r9", "src": "x", "language_config": {}, "max_cpu_time": 1000, "max_memory": 1,
             "test_case": [{"input": "1\n", "output": "1\n"}], "output": True}
    on_started = MagicMock()
    with patch("handler.ensure_present") as ensure, patch("handler.maybe_sweep") as sweep, \
         patch("handler.dispatch_judge", return_value={"err": None, "data": [
             {"error": 0, "result": 0, "cpu_time": 1, "real_time": 1, "memory": 1, "test_case": "1"}]}) as m:
        out = handler.process_event(event, _Cfg, on_started=on_started)
    ensure.assert_not_called()
    sweep.assert_not_called()
    on_started.assert_not_called()
    assert m.call_args.args[2]["test_case"] == event["test_case"]
    assert "test_case_id" not in m.call_args.args[2]
    assert out["runId"] == "r9"
    assert "submissionId" not in out
    assert out["status"] == verdict.ACCEPTED


def test_a_submission_still_syncs_its_bundle_and_announces_its_start():
    from unittest.mock import MagicMock
    event = {"submissionId": "sub-9", "src": "x", "language_config": {}, "max_cpu_time": 1000, "max_memory": 1,
             "test_case_id": "p", "output": True}
    on_started = MagicMock()
    with patch("handler.ensure_present") as ensure, patch("handler.maybe_sweep"), \
         patch("handler.dispatch_judge", return_value={"err": None, "data": []}):
        out = handler.process_event(event, _Cfg, on_started=on_started)
    ensure.assert_called_once_with("p")
    on_started.assert_called_once()
    assert out["submissionId"] == "sub-9"
    assert "runId" not in out
