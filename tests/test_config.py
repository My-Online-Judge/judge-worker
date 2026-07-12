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
