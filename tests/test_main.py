import importlib
import sys
from unittest.mock import MagicMock


def test_build_consumer_applies_poll_tuning(monkeypatch):
    for v in ("JUDGE_TIMEOUT_SECONDS", "MAX_POLL_RECORDS", "MAX_POLL_INTERVAL_MS",
              "SESSION_TIMEOUT_MS", "HEARTBEAT_INTERVAL_MS"):
        monkeypatch.delenv(v, raising=False)
    import config
    importlib.reload(config)

    # kafka-python 2.0.2 does not import under Python 3.12 (six.moves), but the worker runs on
    # 3.11. Stub the module so this stays a pure unit test of build_consumer's kwargs.
    fake_kafka = MagicMock()
    monkeypatch.setitem(sys.modules, "kafka", fake_kafka)
    monkeypatch.delitem(sys.modules, "main", raising=False)
    import main

    main.build_consumer(config.Config)

    kwargs = main.KafkaConsumer.call_args.kwargs
    # slow, one-at-a-time processing so a fetched batch can't exceed the poll interval
    assert kwargs["max_poll_records"] == 1
    assert kwargs["max_poll_interval_ms"] >= config.Config.JUDGE_TIMEOUT_SECONDS * 1000 + 30000
    assert kwargs["session_timeout_ms"] == config.Config.SESSION_TIMEOUT_MS
    assert kwargs["heartbeat_interval_ms"] == config.Config.HEARTBEAT_INTERVAL_MS
    # commit stays manual (after publish) — must not regress to auto-commit
    assert kwargs["enable_auto_commit"] is False
