import json
import logging
from unittest.mock import patch

import pytest

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
        JUDGE_TIMEOUT_MAX_SECONDS = 300

    with patch("handler.ensure_present"), \
         patch("handler.dispatch_judge", return_value={"err": None, "data": [
             {"error": 0, "result": 0, "cpu_time": 1, "real_time": 1, "memory": 1}]}) as m:
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
        JUDGE_TIMEOUT_MAX_SECONDS = 300
        SPJ_COMPILE_ALLOWANCE_SECONDS = 20

    with patch("handler.ensure_present"), \
         patch("handler.dispatch_judge", return_value={"err": None, "data": []}) as m:
        handler.process_event(checker, Cfg)
        handler.process_event(plain, Cfg)
    # 30 s + 3 x 1 s of CPU for the one case that is counted when the bundle's size is unknown (+ 20 s for the checker)
    assert [(c.args[2].HasField("spj"), c.kwargs["timeout"]) for c in m.call_args_list] == [(True, 53), (False, 33)]


class _Cfg:
    JUDGE_SERVER_URLS = ["http://js1:8080"]
    JUDGE_SERVER_TOKEN = "t"
    JUDGE_TIMEOUT_SECONDS = 30
    JUDGE_TIMEOUT_MAX_SECONDS = 300
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
    request = m.call_args.args[2]
    assert [(c.input, c.output) for c in request.test_case.cases] == [("1\n", "1\n")]
    assert request.WhichOneof("cases") == "test_case"      # the cases travel in the request: no bundle is named
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


@pytest.mark.parametrize("event", [[], 1, "x", None])
def test_job_identity_of_a_non_object_event_is_none(event):
    assert handler.job_identity(event) is None


def test_an_empty_result_array_is_a_system_error_with_a_reason():
    out = handler.build_judged_event("s", {"err": None, "data": []})
    assert out["status"] == verdict.SYSTEM_ERROR
    assert "0 results" in out["error_message"]


def test_fewer_results_than_the_bundle_has_cases_is_a_system_error_with_a_reason():
    resp = {"err": None, "data": [{"error": 0, "result": 0, "cpu_time": 1, "real_time": 1, "memory": 1}]}
    out = handler.build_judged_event("s", resp, expected_count=3)
    assert out["status"] == verdict.SYSTEM_ERROR
    assert "1 of 3" in out["error_message"]


def _spy_dispatch(monkeypatch, results):
    monkeypatch.setattr(handler, "maybe_sweep", lambda: None)
    monkeypatch.setattr(handler, "dispatch_judge", lambda *a, **k: {"err": None, "data": results})


def test_process_event_reads_the_case_count_from_the_synced_bundle(monkeypatch, tmp_path):
    (tmp_path / "info").write_text('{"test_case_number": 2}')
    monkeypatch.setattr(handler, "ensure_present", lambda tcid: str(tmp_path))
    _spy_dispatch(monkeypatch, [{"error": 0, "result": 0, "cpu_time": 1, "real_time": 1, "memory": 1}])
    event = {"submissionId": "s", "test_case_id": "p__h", "src": "x", "language_config": {}, "max_cpu_time": 1,
             "max_memory": 1}
    out = handler.process_event(event, _Cfg)
    assert out["status"] == verdict.SYSTEM_ERROR            # 1 result for a 2-case bundle


def test_process_event_of_a_run_counts_its_inline_cases(monkeypatch):
    _spy_dispatch(monkeypatch, [{"error": 0, "result": 0, "cpu_time": 1, "real_time": 1, "memory": 1}])
    event = {"runId": "r", "test_case": [{"input": "1"}, {"input": "2"}], "src": "x", "language_config": {},
             "max_cpu_time": 1, "max_memory": 1}
    out = handler.process_event(event, _Cfg)
    assert out["runId"] == "r" and out["status"] == verdict.SYSTEM_ERROR


def test_an_unreadable_bundle_info_does_not_fail_a_complete_judgement(monkeypatch, tmp_path):
    (tmp_path / "info").write_text("not json")
    monkeypatch.setattr(handler, "ensure_present", lambda tcid: str(tmp_path))
    _spy_dispatch(monkeypatch, [{"error": 0, "result": 0, "cpu_time": 1, "real_time": 1, "memory": 1}])
    event = {"submissionId": "s", "test_case_id": "p__h", "src": "x", "language_config": {}, "max_cpu_time": 1,
             "max_memory": 1}
    out = handler.process_event(event, _Cfg)
    assert out["status"] == verdict.ACCEPTED                # count unknown: only an empty array is refused


def test_every_verdict_names_the_worker_that_judged_it(monkeypatch):
    import config
    monkeypatch.setattr(config.Config, "WORKER_ID", "judge-abc12")
    ok = {"err": None, "data": [{"error": 0, "result": 0, "cpu_time": 1, "real_time": 1, "memory": 1}]}
    assert handler.build_judged_event("s", ok)["judgedBy"] == "judge-abc12"
    assert handler.build_judged_event("s", {"err": "CompileError", "data": "x"})["judgedBy"] == "judge-abc12"
    assert handler.build_judged_event("s", {"err": None, "data": None})["judgedBy"] == "judge-abc12"   # malformed
    assert handler.system_error_event("s", "boom")["judgedBy"] == "judge-abc12"
    assert handler.system_error_event("r", "boom", id_key="runId")["judgedBy"] == "judge-abc12"


# --- the judge deadline scales with the bundle (Config.JUDGE_TIMEOUT_MAX_SECONDS caps it) ---

class _DeadlineCfg:
    JUDGE_TIMEOUT_SECONDS = 30
    SPJ_COMPILE_ALLOWANCE_SECONDS = 20
    JUDGE_TIMEOUT_MAX_SECONDS = 300


def test_deadline_scales_with_cases():
    # 30 s base + 20 cases x 3 x 1 s of CPU
    assert handler.judge_deadline_seconds(_DeadlineCfg, 1000, 20, False) == 90


def test_deadline_capped():
    # 30 + 1000 cases x 3 x 5 s = 15030 s, held at the cap
    assert handler.judge_deadline_seconds(_DeadlineCfg, 5000, 1000, False) == 300


def test_deadline_adds_spj_allowance():
    assert handler.judge_deadline_seconds(_DeadlineCfg, 1000, 20, True) == 110
    assert handler.judge_deadline_seconds(_DeadlineCfg, 5000, 1000, True) == 300      # the cap holds with a checker


@pytest.mark.parametrize("case_count", [None, 0, 1])
def test_deadline_counts_an_unknown_or_empty_bundle_as_one_case(case_count):
    assert handler.judge_deadline_seconds(_DeadlineCfg, 1000, case_count, False) == 33


def test_deadline_rounds_up_to_a_whole_second():
    # 7 cases x 3 x 0.1 s = 2.1 s -> 3 s
    assert handler.judge_deadline_seconds(_DeadlineCfg, 100, 7, False) == 33


def test_deadline_never_drops_below_the_base_for_a_bad_cpu_limit():
    # a negative limit must not shrink the deadline: the sandbox refuses it by name (INVALID_ARGUMENT), not as a timeout
    assert handler.judge_deadline_seconds(_DeadlineCfg, -1000, 20, False) == 30


def test_deadline_cap_wins_over_a_base_above_it():
    class Misconfigured(_DeadlineCfg):
        JUDGE_TIMEOUT_SECONDS = 500

    assert handler.judge_deadline_seconds(Misconfigured, 1000, 1, False) == 300


def test_process_event_passes_scaled_deadline():
    inline = {"runId": "r1", "src": "x", "language_config": {}, "max_cpu_time": 2000, "max_memory": 1, "output": True,
              "test_case": [{"input": "1", "output": "1"}] * 10}
    with patch("handler.dispatch_judge", return_value={"err": None, "data": []}) as m:
        handler.process_event(inline, _Cfg)
    assert m.call_args.kwargs["timeout"] == 30 + 10 * 3 * 2          # 10 inline cases at 2 s of CPU each


def test_process_event_scales_the_deadline_by_the_bundles_case_count(monkeypatch, tmp_path):
    (tmp_path / "info").write_text('{"test_case_number": 20}')
    monkeypatch.setattr(handler, "ensure_present", lambda tcid: str(tmp_path))
    event = {"submissionId": "s", "test_case_id": "p__h", "src": "x", "language_config": {}, "max_cpu_time": 1000,
             "max_memory": 1}
    with patch("handler.maybe_sweep"), patch("handler.dispatch_judge", return_value={"err": None, "data": []}) as m:
        handler.process_event(event, _Cfg)
    assert m.call_args.kwargs["timeout"] == 90                       # 30 + 20 x 3 x 1


def test_process_event_adds_the_checker_allowance_to_the_scaled_deadline(monkeypatch, tmp_path):
    (tmp_path / "info").write_text('{"test_case_number": 20}')
    monkeypatch.setattr(handler, "ensure_present", lambda tcid: str(tmp_path))
    event = {"submissionId": "s", "test_case_id": "p__h", "src": "x", "language_config": {}, "max_cpu_time": 1000,
             "max_memory": 1, "spj_version": "ab12", "spj_config": {}, "spj_compile_config": {},
             "spj_src": "int main(){}"}
    with patch("handler.maybe_sweep"), patch("handler.dispatch_judge", return_value={"err": None, "data": []}) as m:
        handler.process_event(event, _Cfg)
    assert m.call_args.kwargs["timeout"] == 110
