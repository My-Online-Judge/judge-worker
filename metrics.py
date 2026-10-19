"""The worker's Prometheus endpoint (sub-project 6 §3.1): sandbox health and dispatch counts, scraped as job
judge-worker. Its being scrapable at all is the worker's liveness signal (alert JudgeWorkerDown)."""
from prometheus_client import start_http_server

import tls


def start(config):
    """Serve /metrics on METRICS_PORT from a daemon thread; with TLS on, over HTTPS to clients presenting a certificate
    signed by the CA (sub-project 10)."""
    start_http_server(config.METRICS_PORT, **tls.metrics_kwargs())
