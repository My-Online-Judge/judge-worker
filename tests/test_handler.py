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


def test_build_judged_event_compile_error_envelope():
    resp = {"err": "CompileError", "data": "line 3: syntax error"}
    out = handler.build_judged_event("sub-2", resp)
    assert out["status"] == verdict.COMPILE_ERROR
    assert out["error_message"] == "CompileError: line 3: syntax error"
    assert out["details"] == []


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

    with patch("handler.dispatch_judge", return_value={"err": None, "data": []}) as m:
        out = handler.process_event(event, Cfg)
    m.assert_called_once()
    # process_event hands the full URL list to the dispatcher (which does round-robin/failover)
    assert m.call_args.args[0] == ["http://js1:8080", "http://js2:8080"]
    assert out["submissionId"] == "sub-9"
    assert out["status"] == verdict.ACCEPTED
