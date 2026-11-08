import os
import socket

from addresses import parse_address

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
    # Internal TLS (sub-project 10): the directory holding ca.crt, tls.crt and tls.key (compose mounts the tls volume's
    # judge-worker subpath at /etc/oj/tls). Unset = plaintext, as in development and the unit tests.
    OJ_TLS_DIR = os.getenv("OJ_TLS_DIR", "")
    TLS_CA_FILE = os.path.join(OJ_TLS_DIR, "ca.crt") if OJ_TLS_DIR else ""
    TLS_CERT_FILE = os.path.join(OJ_TLS_DIR, "tls.crt") if OJ_TLS_DIR else ""
    TLS_KEY_FILE = os.path.join(OJ_TLS_DIR, "tls.key") if OJ_TLS_DIR else ""

    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
    JUDGE_SERVER_URL = os.getenv("JUDGE_SERVER_URL", "http://localhost:8080")
    # Comma-separated list of the sandboxes' gRPC addresses, each `host:port` or `https://host:port` (an http:// or
    # https:// scheme is dropped: OJ_TLS_DIR, not the scheme, picks TLS; addresses.parse_address, which validate()
    # applies to every entry). The dispatcher round-robins across them and fails over to the next on the gRPC codes
    # UNAVAILABLE, INTERNAL and RESOURCE_EXHAUSTED (grpc_errors.RETRYABLE_CODES). A single address is the common case.
    JUDGE_SERVER_URLS = [u.strip() for u in JUDGE_SERVER_URL.split(",") if u.strip()]
    # The sandboxes' shared secret (its sha256 goes in the x-judge-server-token metadata of every gRPC call, and in the
    # X-JUDGE-SERVER-TOKEN header of the heartbeat relay). No default: validate() stops the worker.
    JUDGE_SERVER_TOKEN = os.getenv("JUDGE_SERVER_TOKEN", "")
    REQUESTED_TOPIC = os.getenv("REQUESTED_TOPIC", "submission.requested")
    JUDGED_TOPIC = os.getenv("JUDGED_TOPIC", "submission.judged")
    # Announced just before each dispatch (sub-project 11): submission-service starts its judging-timeout clock then.
    STARTED_TOPIC = os.getenv("STARTED_TOPIC", "submission.started")
    # Who judged: the Kubernetes pod name (Downward API POD_NAME) when there is one, so a verdict, a log line and a start
    # event name the pod that produced them; else WORKER_ID, else the container's hostname (compose).
    WORKER_ID = os.getenv("POD_NAME") or os.getenv("WORKER_ID") or socket.gethostname()
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "judge-workers")
    # The base of the deadline (the gRPC timeout) of one Judge call; handler.judge_deadline_seconds adds the per-case
    # and the checker's share to it, up to JUDGE_TIMEOUT_MAX_SECONDS.
    JUDGE_TIMEOUT_SECONDS = _int_env("JUDGE_TIMEOUT_SECONDS", DEFAULT_JUDGE_TIMEOUT_SECONDS)
    # The most one Judge call may wait, however big the bundle: handler.judge_deadline_seconds adds 3 x the CPU limit
    # per case to JUDGE_TIMEOUT_SECONDS and stops here. Keep it under MAX_POLL_INTERVAL_MS minus the MinIO sync budget
    # (process_event runs without poll(), and a failover judges once more per sandbox; see MAX_POLL_INTERVAL_MS) and
    # under the containers' stop_grace_period (a SIGTERM waits for the judging in progress, then for the publish).
    JUDGE_TIMEOUT_MAX_SECONDS = _int_env("JUDGE_TIMEOUT_MAX_SECONDS", 300)
    # Added to that timeout for a request that carries a checker (SP13): the sandbox compiles the checker inside the
    # first such request on each sandbox for a spj_version (10 s CPU / 20 s real) before it judges.
    SPJ_COMPILE_ALLOWANCE_SECONDS = _int_env("SPJ_COMPILE_ALLOWANCE_SECONDS", 20)
    # How long one verdict publish waits for Kafka's acknowledgement before it is sent again (sub-project 7 §3.2).
    PUBLISH_TIMEOUT_SECONDS = _int_env("PUBLISH_TIMEOUT_SECONDS", 10)
    # How long MinIO or every sandbox may stay away before the submission ends in a SYSTEM_ERROR (sub-project 11):
    # the judging is started again after 2 s, 4 s, 8 s … capped at 60 s.
    JUDGE_INFRA_RETRY_SECONDS = _int_env("JUDGE_INFRA_RETRY_SECONDS", 300)
    # submission-service's judge-server registry. The sandboxes cannot reach it (internal network), so
    # the worker relays each one's Ping answer (gRPC) there. Empty = no relay.
    SANDBOX_HEARTBEAT_URL = os.getenv("SANDBOX_HEARTBEAT_URL", "")
    # Below the registry's 30 s liveness window, so one missed report does not flip a sandbox offline.
    SANDBOX_HEARTBEAT_INTERVAL_SECONDS = _int_env("SANDBOX_HEARTBEAT_INTERVAL_SECONDS", 10)
    # Prometheus scrapes /metrics here (job judge-worker): sandbox health and dispatch counts. Not published on the
    # host; being scrapable at all is the worker's liveness signal (alert JudgeWorkerDown).
    METRICS_PORT = _int_env("METRICS_PORT", 9100)

    # --- Consumer poll / rebalance tuning ---
    # Judging is slow and blocking, so fetch a single submission per poll: a large batch would
    # make the gap between poll() calls exceed max.poll.interval.ms and get us evicted mid-judge.
    MAX_POLL_RECORDS = _int_env("MAX_POLL_RECORDS", 1)
    # Before Kafka decides we are stuck and rebalances our partitions away (a second worker then judges the same
    # submission). process_event is one stretch with no poll(): the MinIO sync (up to MINIO_TIMEOUT_SECONDS per try,
    # MINIO_RETRIES + 1 tries: 3 x 60 = 180 s) and then, when a failover happens, one Judge call per sandbox, each up
    # to JUDGE_TIMEOUT_MAX_SECONDS (a sandbox that dies mid-judge fails its call late, and the next sandbox judges
    # again). With the defaults and two sandboxes that is 180 + 2 x 300 = 780 s; 15 minutes leaves room for a slow
    # node. Each further sandbox in JUDGE_SERVER_URL adds up to JUDGE_TIMEOUT_MAX_SECONDS: raise this, or lower the
    # cap, with three. The waits for Kafka, MinIO or a sandbox that is away poll() in 1 s slices (worker.run), so they
    # do not count against this.
    MAX_POLL_INTERVAL_MS = _int_env("MAX_POLL_INTERVAL_MS", 900000)
    # Liveness heartbeat runs on a background thread (independent of poll); keep the heartbeat
    # comfortably below the session timeout as Kafka requires.
    SESSION_TIMEOUT_MS = _int_env("SESSION_TIMEOUT_MS", 30000)
    HEARTBEAT_INTERVAL_MS = _int_env("HEARTBEAT_INTERVAL_MS", 10000)

    # --- Test-case object storage (MinIO) ---
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "test-cases")
    # One MinIO request's connect and read timeout, and how many times urllib3 retries it (so a try takes at most
    # MINIO_TIMEOUT_SECONDS x (MINIO_RETRIES + 1)). minio-py's own 5 min x 6 would hold the consumer past its poll
    # interval in a MinIO brownout; a failure beyond this is retried by judge_with_infra_retry, which polls meanwhile.
    MINIO_TIMEOUT_SECONDS = _int_env("MINIO_TIMEOUT_SECONDS", 60)
    MINIO_RETRIES = _int_env("MINIO_RETRIES", 2)
    # Shared volume the sandbox mounts at /test_case; the worker syncs bundles here before judging.
    TEST_CASE_CACHE_DIR = os.getenv("TEST_CASE_CACHE_DIR", "/test_case")
    # Age-based GC of the cache volume: evict installed bundle dirs unused for this long...
    TEST_CASE_CACHE_TTL_SECONDS = int(os.getenv("TEST_CASE_CACHE_TTL_SECONDS", "86400"))
    # ...checked at most this often (rate-limited via a `.last_sweep` marker file).
    TEST_CASE_CACHE_SWEEP_INTERVAL_SECONDS = int(os.getenv("TEST_CASE_CACHE_SWEEP_INTERVAL_SECONDS", "3600"))

    @classmethod
    def validate(cls) -> None:
        """Refuse to start without the settings that have no safe default or that cannot work as written."""
        if not cls.JUDGE_SERVER_TOKEN:
            raise SystemExit("missing required env: JUDGE_SERVER_TOKEN")
        # A malformed address fails every submission (dispatch_judge raises ValueError on it): stop here instead.
        for url in cls.JUDGE_SERVER_URLS:
            try:
                parse_address(url)
            except ValueError as exc:
                raise SystemExit(f"invalid JUDGE_SERVER_URL entry: {url} ({exc})") from exc
