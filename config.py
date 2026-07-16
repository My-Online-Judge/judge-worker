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

    # --- Consumer poll / rebalance tuning ---
    # Judging is slow and blocking, so fetch a single submission per poll: a large batch would
    # make the gap between poll() calls exceed max.poll.interval.ms and get us evicted mid-judge.
    MAX_POLL_RECORDS = _int_env("MAX_POLL_RECORDS", 1)
    # Give one (slowest) judge plus a 30s margin (compile, judge-server round-trip, delivery ack)
    # before Kafka decides we are stuck and rebalances our partitions away.
    MAX_POLL_INTERVAL_MS = _int_env(
        "MAX_POLL_INTERVAL_MS", JUDGE_TIMEOUT_SECONDS * 1000 + 30000
    )
    # Liveness heartbeat runs on a background thread (independent of poll); keep the heartbeat
    # comfortably below the session timeout as Kafka requires.
    SESSION_TIMEOUT_MS = _int_env("SESSION_TIMEOUT_MS", 30000)
    HEARTBEAT_INTERVAL_MS = _int_env("HEARTBEAT_INTERVAL_MS", 10000)
