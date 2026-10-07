"""The worker's Prometheus endpoint (sub-project 6 §3.1): sandbox health and dispatch counts, scraped as job
judge-worker. Its being scrapable at all is the worker's liveness signal (alert JudgeWorkerDown)."""
from prometheus_client import start_http_server


def start(config):
    """Serve /metrics on METRICS_PORT from a daemon thread."""
    start_http_server(config.METRICS_PORT)
