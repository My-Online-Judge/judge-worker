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
    # Comma-separated list of judge-server base URLs; the dispatcher round-robins across them and
    # fails over to the next on connection error / 5xx. A single URL is the common case.
    JUDGE_SERVER_URLS = [u.strip() for u in JUDGE_SERVER_URL.split(",") if u.strip()]
    JUDGE_SERVER_TOKEN = os.getenv("JUDGE_SERVER_TOKEN", "default_token")
    REQUESTED_TOPIC = os.getenv("REQUESTED_TOPIC", "submission.requested")
    JUDGED_TOPIC = os.getenv("JUDGED_TOPIC", "submission.judged")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "judge-workers")
    JUDGE_TIMEOUT_SECONDS = _int_env("JUDGE_TIMEOUT_SECONDS", DEFAULT_JUDGE_TIMEOUT_SECONDS)
    # submission-service's judge-server registry. The sandboxes cannot reach it (internal network), so
    # the worker relays each one's /ping there. Empty = no relay.
    SANDBOX_HEARTBEAT_URL = os.getenv("SANDBOX_HEARTBEAT_URL", "")
    # Below the registry's 30 s liveness window, so one missed report does not flip a sandbox offline.
    SANDBOX_HEARTBEAT_INTERVAL_SECONDS = _int_env("SANDBOX_HEARTBEAT_INTERVAL_SECONDS", 10)

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

    # --- Test-case object storage (MinIO) ---
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "test-cases")
    # Shared volume the sandbox mounts at /test_case; the worker syncs bundles here before judging.
    TEST_CASE_CACHE_DIR = os.getenv("TEST_CASE_CACHE_DIR", "/test_case")
    # Age-based GC of the cache volume: evict installed bundle dirs unused for this long...
    TEST_CASE_CACHE_TTL_SECONDS = int(os.getenv("TEST_CASE_CACHE_TTL_SECONDS", "86400"))
    # ...checked at most this often (rate-limited via a `.last_sweep` marker file).
    TEST_CASE_CACHE_SWEEP_INTERVAL_SECONDS = int(os.getenv("TEST_CASE_CACHE_SWEEP_INTERVAL_SECONDS", "3600"))
