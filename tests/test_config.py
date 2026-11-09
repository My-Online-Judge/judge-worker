import importlib

import pytest


def test_config_reads_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BROKERS", "broker:9092")
    monkeypatch.setenv("JUDGE_SERVER_URL", "http://js:8080")
    monkeypatch.setenv("JUDGE_SERVER_TOKEN", "secret")
    import config
    importlib.reload(config)
    c = config.Config
    assert c.KAFKA_BROKERS == "broker:9092"
    assert c.JUDGE_SERVER_URL == "http://js:8080"
    assert c.JUDGE_SERVER_TOKEN == "secret"
    assert c.REQUESTED_TOPIC == "submission.requested"
    assert c.JUDGED_TOPIC == "submission.judged"
    assert c.CONSUMER_GROUP == "judge-workers"


def test_judge_timeout_seconds_bad_value_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("JUDGE_TIMEOUT_SECONDS", "not-a-number")
    import config
    importlib.reload(config)
    assert config.Config.JUDGE_TIMEOUT_SECONDS == 30


def test_judge_timeout_seconds_good_value_is_parsed(monkeypatch):
    monkeypatch.setenv("JUDGE_TIMEOUT_SECONDS", "45")
    import config
    importlib.reload(config)
    assert config.Config.JUDGE_TIMEOUT_SECONDS == 45


def test_judge_timeout_max_seconds_defaults_to_300(monkeypatch):
    monkeypatch.delenv("JUDGE_TIMEOUT_MAX_SECONDS", raising=False)
    import config
    importlib.reload(config)
    assert config.Config.JUDGE_TIMEOUT_MAX_SECONDS == 300


def test_judge_timeout_max_seconds_is_read_from_the_env(monkeypatch):
    import config
    try:
        monkeypatch.setenv("JUDGE_TIMEOUT_MAX_SECONDS", "120")
        importlib.reload(config)
        assert config.Config.JUDGE_TIMEOUT_MAX_SECONDS == 120
    finally:
        monkeypatch.undo()
        importlib.reload(config)  # later tests get the default back


def test_judge_timeout_max_seconds_bad_value_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("JUDGE_TIMEOUT_MAX_SECONDS", "not-a-number")
    import config
    importlib.reload(config)
    assert config.Config.JUDGE_TIMEOUT_MAX_SECONDS == 300


# --- T2-5: consumer poll / rebalance tuning ---

def _reload_config_clean(monkeypatch, *keep):
    for v in ("JUDGE_TIMEOUT_SECONDS", "JUDGE_TIMEOUT_MAX_SECONDS", "SPJ_COMPILE_ALLOWANCE_SECONDS", "MINIO_TIMEOUT_SECONDS", "MINIO_RETRIES", "MAX_POLL_RECORDS", "MAX_POLL_INTERVAL_MS",
              "SESSION_TIMEOUT_MS", "HEARTBEAT_INTERVAL_MS"):
        if v not in keep:
            monkeypatch.delenv(v, raising=False)
    import config
    importlib.reload(config)
    return config.Config


def test_consumer_tuning_defaults(monkeypatch):
    c = _reload_config_clean(monkeypatch)
    # one submission at a time so a big batch can't blow past the poll interval
    assert c.MAX_POLL_RECORDS == 1
    # a single (slowest) judge plus margin must fit before Kafka evicts the consumer
    assert c.MAX_POLL_INTERVAL_MS >= c.JUDGE_TIMEOUT_SECONDS * 1000 + 30000
    # heartbeat must be strictly below the session timeout (Kafka requirement)
    assert c.HEARTBEAT_INTERVAL_MS < c.SESSION_TIMEOUT_MS


def test_max_poll_interval_scales_with_judge_timeout(monkeypatch):
    monkeypatch.setenv("JUDGE_TIMEOUT_SECONDS", "60")
    c = _reload_config_clean(monkeypatch, "JUDGE_TIMEOUT_SECONDS")
    assert c.MAX_POLL_INTERVAL_MS >= 60 * 1000 + 30000


def test_max_poll_interval_default_900s(monkeypatch):
    c = _reload_config_clean(monkeypatch)
    assert c.MAX_POLL_INTERVAL_MS == 900_000


def test_max_poll_interval_covers_one_attempt_that_does_not_poll(monkeypatch):
    # process_event is one stretch with no poll(): the MinIO sync (connect or read timeout, each try) and then one
    # judge per sandbox (a failover tries them all), each up to the deadline cap. Kafka evicts the consumer if that
    # exceeds the interval. With the defaults and two sandboxes: 3 x 60 + 2 x 300 = 780 s, under 900 s.
    monkeypatch.setenv("JUDGE_SERVER_URL", "https://a:8080,https://b:8080")
    c = _reload_config_clean(monkeypatch, "JUDGE_SERVER_URL")
    sync = c.MINIO_TIMEOUT_SECONDS * (c.MINIO_RETRIES + 1)
    judge = c.JUDGE_TIMEOUT_MAX_SECONDS * len(c.JUDGE_SERVER_URLS)
    assert sync + judge == 780
    assert c.MAX_POLL_INTERVAL_MS >= (sync + judge) * 1000


def test_minio_timeout_defaults(monkeypatch):
    c = _reload_config_clean(monkeypatch)
    assert (c.MINIO_TIMEOUT_SECONDS, c.MINIO_RETRIES) == (60, 2)


def test_consumer_tuning_env_override(monkeypatch):
    monkeypatch.setenv("MAX_POLL_INTERVAL_MS", "123456")
    monkeypatch.setenv("MAX_POLL_RECORDS", "5")
    c = _reload_config_clean(monkeypatch, "MAX_POLL_INTERVAL_MS", "MAX_POLL_RECORDS")
    assert c.MAX_POLL_INTERVAL_MS == 123456
    assert c.MAX_POLL_RECORDS == 5


# --- T1-3: multiple judge-server URLs ---

def test_judge_server_urls_splits_comma_list(monkeypatch):
    monkeypatch.setenv("JUDGE_SERVER_URL", "http://a:8080, http://b:8080 ,http://c:8080")
    import config
    importlib.reload(config)
    assert config.Config.JUDGE_SERVER_URLS == ["http://a:8080", "http://b:8080", "http://c:8080"]


def test_judge_server_urls_single(monkeypatch):
    monkeypatch.setenv("JUDGE_SERVER_URL", "http://only:8080")
    import config
    importlib.reload(config)
    assert config.Config.JUDGE_SERVER_URLS == ["http://only:8080"]


def test_sandbox_heartbeat_relay_is_off_by_default_every_10s_when_on(monkeypatch):
    monkeypatch.delenv("SANDBOX_HEARTBEAT_URL", raising=False)
    monkeypatch.delenv("SANDBOX_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    import config
    importlib.reload(config)
    assert config.Config.SANDBOX_HEARTBEAT_URL == ""
    assert config.Config.SANDBOX_HEARTBEAT_INTERVAL_SECONDS == 10


def test_metrics_port_defaults_to_9100(monkeypatch):
    monkeypatch.delenv("METRICS_PORT", raising=False)
    import config
    importlib.reload(config)
    assert config.Config.METRICS_PORT == 9100


def test_validate_requires_token(monkeypatch):
    # No default token: a worker started without JUDGE_SERVER_TOKEN (or with an empty one) refuses to start.
    import config
    try:
        for unset in (lambda: monkeypatch.delenv("JUDGE_SERVER_TOKEN", raising=False),
                      lambda: monkeypatch.setenv("JUDGE_SERVER_TOKEN", "")):
            unset()
            importlib.reload(config)
            with pytest.raises(SystemExit, match="^missing required env: JUDGE_SERVER_TOKEN$"):
                config.Config.validate()
        monkeypatch.setenv("JUDGE_SERVER_TOKEN", "secret")
        importlib.reload(config)
        assert config.Config.validate() is None
    finally:
        monkeypatch.undo()
        importlib.reload(config)  # later tests get the session's token back


@pytest.mark.parametrize("value, bad", [
    ("https://judge-server:8080/judge", "https://judge-server:8080/judge"),
    ("http://a:8080/", "http://a:8080/"),
    ("https://:8080", "https://:8080"),
    ("a:notaport", "a:notaport"),
    ("a:0", "a:0"),
    # the first bad entry is the one named, wherever it sits in the list
    ("https://a:8080, http://b:8080/ping ,http://c:8080/x", "http://b:8080/ping"),
])
def test_validate_rejects_a_malformed_judge_server_url(monkeypatch, value, bad):
    # A malformed address would fail every submission (dispatch_judge raises ValueError): the worker refuses to start.
    import config
    try:
        monkeypatch.setenv("JUDGE_SERVER_TOKEN", "secret")
        monkeypatch.setenv("JUDGE_SERVER_URL", value)
        importlib.reload(config)
        with pytest.raises(SystemExit) as exc_info:
            config.Config.validate()
        message = str(exc_info.value)
        assert message.startswith(f"invalid JUDGE_SERVER_URL entry: {bad} (")
        assert message.endswith(")")
    finally:
        monkeypatch.undo()
        importlib.reload(config)


@pytest.mark.parametrize("value", [
    "judge-server:8080",
    "https://judge-server:8080",
    "http://localhost:8080",
    "https://judge-server:8080,judge-server-2:8080, http://10.0.0.3:9090",
])
def test_validate_accepts_both_address_forms(monkeypatch, value):
    # host:port and https://host:port (compose) are both a gRPC target; mixing them in one list is fine.
    import config
    try:
        monkeypatch.setenv("JUDGE_SERVER_TOKEN", "secret")
        monkeypatch.setenv("JUDGE_SERVER_URL", value)
        importlib.reload(config)
        assert config.Config.validate() is None
    finally:
        monkeypatch.undo()
        importlib.reload(config)


def test_validate_checks_the_token_before_the_addresses(monkeypatch):
    import config
    try:
        monkeypatch.delenv("JUDGE_SERVER_TOKEN", raising=False)
        monkeypatch.setenv("JUDGE_SERVER_URL", "http://a:8080/judge")
        importlib.reload(config)
        with pytest.raises(SystemExit, match="^missing required env: JUDGE_SERVER_TOKEN$"):
            config.Config.validate()
    finally:
        monkeypatch.undo()
        importlib.reload(config)


def test_worker_id_prefers_pod_name_then_worker_id_then_hostname(monkeypatch):
    import socket
    monkeypatch.setenv("POD_NAME", "judge-7c9f-abcde")
    monkeypatch.setenv("WORKER_ID", "w-explicit")
    import config
    importlib.reload(config)
    assert config.Config.WORKER_ID == "judge-7c9f-abcde"
    monkeypatch.delenv("POD_NAME")
    importlib.reload(config)
    assert config.Config.WORKER_ID == "w-explicit"
    monkeypatch.delenv("WORKER_ID")
    importlib.reload(config)
    assert config.Config.WORKER_ID == socket.gethostname()
