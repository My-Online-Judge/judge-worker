import importlib


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


# --- T2-5: consumer poll / rebalance tuning ---

def _reload_config_clean(monkeypatch, *keep):
    for v in ("JUDGE_TIMEOUT_SECONDS", "MAX_POLL_RECORDS", "MAX_POLL_INTERVAL_MS",
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
