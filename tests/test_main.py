import importlib
import sys
from unittest.mock import MagicMock

import pytest

import worker  # noqa: F401  (loads kafka.errors before a test replaces the kafka package with a stub)


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
    # an idle pass ends after a second so serve() can notice a SIGTERM between passes
    assert kwargs["consumer_timeout_ms"] == 1000


def _main_with_stub_kafka(monkeypatch):
    monkeypatch.setitem(sys.modules, "kafka", MagicMock())
    monkeypatch.delitem(sys.modules, "main", raising=False)
    import main
    return main


def _tls_kwargs(tls_dir):
    return {"security_protocol": "SSL", "ssl_cafile": str(tls_dir / "ca.crt"),
            "ssl_certfile": str(tls_dir / "tls.crt"), "ssl_keyfile": str(tls_dir / "tls.key"),
            "ssl_check_hostname": True}


def test_build_consumer_merges_tls_kwargs(monkeypatch, tls_dir):
    import config
    main = _main_with_stub_kafka(monkeypatch)

    main.build_consumer(config.Config)

    args, kwargs = main.KafkaConsumer.call_args
    assert args == (config.Config.REQUESTED_TOPIC,)
    assert {k: kwargs.get(k) for k in _tls_kwargs(tls_dir)} == _tls_kwargs(tls_dir)
    # the consumer's own settings stay
    assert kwargs["bootstrap_servers"] == config.Config.KAFKA_BROKERS.split(",")
    assert kwargs["group_id"] == config.Config.CONSUMER_GROUP
    assert kwargs["enable_auto_commit"] is False


def test_build_producer_merges_tls_kwargs(monkeypatch, tls_dir):
    import config
    main = _main_with_stub_kafka(monkeypatch)

    main.build_producer(config.Config)

    kwargs = main.KafkaProducer.call_args.kwargs
    assert {k: kwargs.get(k) for k in _tls_kwargs(tls_dir)} == _tls_kwargs(tls_dir)
    assert kwargs["bootstrap_servers"] == config.Config.KAFKA_BROKERS.split(",")
    assert kwargs["key_serializer"]("42") == b"42"
    assert kwargs["value_serializer"]({"submissionId": 42}) == b'{"submissionId": 42}'


def test_main_builds_the_kafka_clients_through_connect(monkeypatch):
    monkeypatch.setitem(sys.modules, "kafka", MagicMock())
    monkeypatch.delitem(sys.modules, "main", raising=False)
    import main
    for name in ("metrics", "sandbox_status", "tracing"):
        monkeypatch.setattr(main, name, MagicMock())
    connect = MagicMock(side_effect=lambda build, cfg, stop=None: f"built by {build.__name__}")
    serve = MagicMock()
    shutdown = MagicMock()
    shutdown.install.return_value = shutdown          # no real signal handlers in the pytest process
    monkeypatch.setattr(main, "connect", connect)
    monkeypatch.setattr(main, "serve", serve)
    monkeypatch.setattr(main, "Shutdown", MagicMock(return_value=shutdown))

    main.main()

    assert [c.args[0] for c in connect.call_args_list] == [main.build_consumer, main.build_producer]
    serve.assert_called_once_with(main.Config, "built by build_consumer", "built by build_producer", shutdown)
    shutdown.install.assert_called_once_with()


def test_main_validates_the_config_before_starting_anything(monkeypatch):
    main = _main_with_stub_kafka(monkeypatch)
    for name in ("metrics", "sandbox_status", "tracing", "connect", "serve", "Shutdown"):
        monkeypatch.setattr(main, name, MagicMock())
    monkeypatch.setattr(main.Config, "JUDGE_SERVER_TOKEN", "")

    with pytest.raises(SystemExit, match="missing required env: JUDGE_SERVER_TOKEN"):
        main.main()

    main.tracing.init_tracing.assert_not_called()
    main.metrics.start.assert_not_called()
    main.connect.assert_not_called()
