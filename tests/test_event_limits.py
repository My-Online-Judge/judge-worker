import copy

import handler
from event_limits import (MAX_ERROR_CHARS, MAX_OUTPUT_BYTES, TRUNCATION_MARK,
                          limit_event)


def _ev(details=None, error_message=None):
    return {"submissionId": "s", "status": 0, "error_message": error_message,
            "details": details if details is not None else []}


def test_short_output_untouched():
    ev = _ev([{"output": "hello", "result": 0}])
    assert limit_event(ev)["details"][0]["output"] == "hello"


def test_output_cut_at_4096_bytes_with_mark():
    out = limit_event(_ev([{"output": "a" * 10000}]))["details"][0]["output"]
    assert out.endswith(TRUNCATION_MARK)
    assert len(out[:-len(TRUNCATION_MARK)].encode("utf-8")) <= MAX_OUTPUT_BYTES


def test_cut_never_splits_utf8():
    out = limit_event(_ev([{"output": "a" * 4095 + "é"}]))["details"][0]["output"]
    body = out[:-len(TRUNCATION_MARK)]
    assert body == "a" * 4095
    out.encode("utf-8").decode("utf-8")


def test_error_message_capped():
    msg = limit_event(_ev(error_message="x" * 70000))["error_message"]
    assert msg == "x" * MAX_ERROR_CHARS + TRUNCATION_MARK


def test_drops_all_outputs_when_event_still_too_big():
    cases = [{"output": "b" * 4096, "result": 0, "cpu_time": i} for i in range(300)]
    out = limit_event(_ev(cases))
    assert len(out["details"]) == 300
    for i, c in enumerate(out["details"]):
        assert "output" not in c
        assert c["result"] == 0 and c["cpu_time"] == i


def test_input_not_mutated():
    ev = _ev([{"output": "a" * 10000}], error_message="x" * 70000)
    before = copy.deepcopy(ev)
    limit_event(ev)
    assert ev == before


def test_build_judged_event_applies_limits():
    resp = {"err": None, "data": [{"error": 0, "result": 0, "cpu_time": 1,
                                   "real_time": 1, "memory": 1, "output": "z" * 9000}]}
    out = handler.build_judged_event("s", resp)
    assert out["details"][0]["output"].endswith(TRUNCATION_MARK)
