import os

DEFAULT_JUDGE_TIMEOUT_SECONDS = 30


def _int_env(name, default):
    """Parse an int env var, falling back to default on a missing/non-numeric value."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class Config:
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
    JUDGE_SERVER_URL = os.getenv("JUDGE_SERVER_URL", "http://localhost:8080")
    JUDGE_SERVER_TOKEN = os.getenv("JUDGE_SERVER_TOKEN", "default_token")
    REQUESTED_TOPIC = os.getenv("REQUESTED_TOPIC", "submission.requested")
    JUDGED_TOPIC = os.getenv("JUDGED_TOPIC", "submission.judged")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "judge-workers")
    JUDGE_TIMEOUT_SECONDS = _int_env("JUDGE_TIMEOUT_SECONDS", DEFAULT_JUDGE_TIMEOUT_SECONDS)
