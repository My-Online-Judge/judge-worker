import socket
import urllib.request
from types import SimpleNamespace

import metrics
import sandbox_health


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_metrics_endpoint_serves_the_sandbox_gauge():
    port = free_port()
    metrics.start(SimpleNamespace(METRICS_PORT=port))
    sandbox_health.mark("http://judge-server:8080", True)

    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()

    assert 'oj_sandbox_up{sandbox="judge-server"} 1.0' in body
