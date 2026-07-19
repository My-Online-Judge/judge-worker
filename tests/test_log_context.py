import logging


def test_filter_injects_contextvar_value():
    from log_context import submission_id_var, SubmissionIdFilter
    f = SubmissionIdFilter()
    rec = logging.LogRecord("n", logging.INFO, __file__, 1, "msg", None, None)

    token = submission_id_var.set("abc-123")
    try:
        assert f.filter(rec) is True
        assert rec.submission_id == "abc-123"
    finally:
        submission_id_var.reset(token)


def test_filter_defaults_to_dash_when_unset():
    from log_context import SubmissionIdFilter
    f = SubmissionIdFilter()
    rec = logging.LogRecord("n", logging.INFO, __file__, 1, "msg", None, None)
    f.filter(rec)
    assert rec.submission_id == "-"


def test_process_event_sets_and_resets_contextvar(monkeypatch):
    import handler
    from log_context import submission_id_var
    # stub the judging pipeline so process_event runs without a live stack
    monkeypatch.setattr(handler, "maybe_sweep", lambda: None)
    monkeypatch.setattr(handler, "ensure_present", lambda tcid: None)
    monkeypatch.setattr(handler, "build_judge_body", lambda e: {})
    monkeypatch.setattr(handler, "dispatch_judge", lambda *a, **k: {"err": None, "data": []})

    class Cfg:
        JUDGE_SERVER_URLS = ["x"]; JUDGE_SERVER_TOKEN = "t"; JUDGE_TIMEOUT_SECONDS = 1
    event = {"submissionId": "sub-9", "test_case_id": "slug__h"}

    handler.process_event(event, Cfg())
    assert submission_id_var.get() == "-"  # reset after processing
