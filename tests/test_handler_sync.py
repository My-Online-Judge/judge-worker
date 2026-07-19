import handler


def test_process_event_syncs_bundle_before_dispatch(monkeypatch):
    order = []

    monkeypatch.setattr(handler, "ensure_present",
                        lambda tcid: order.append(("sync", tcid)))
    monkeypatch.setattr(handler, "dispatch_judge",
                        lambda urls, token, body, timeout: order.append(("dispatch",)) or {"data": []})

    class Cfg:
        JUDGE_SERVER_URLS = ["http://js:8080"]
        JUDGE_SERVER_TOKEN = "t"
        JUDGE_TIMEOUT_SECONDS = 30

    event = {"submissionId": "s1", "src": "x", "language_config": {}, "max_cpu_time": 1,
             "max_memory": 1, "test_case_id": "simple-a-plus-b__abc123", "output": True}

    handler.process_event(event, Cfg)

    assert order == [("sync", "simple-a-plus-b__abc123"), ("dispatch",)]
