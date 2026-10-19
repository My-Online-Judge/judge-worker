import shutil
import socket
import ssl
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import pytest

import metrics
import sandbox_health

FIXTURES = Path(__file__).parent / "fixtures" / "tls"


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


def test_metrics_server_requires_client_cert(tls_dir):
    # The worker's identity as tls-init installs it (here the fixture server leaf: SAN localhost, 127.0.0.1).
    for src, dst in (("ca.crt", "ca.crt"), ("server.crt", "tls.crt"), ("server.key", "tls.key")):
        shutil.copy(FIXTURES / src, tls_dir / dst)
    port = free_port()
    metrics.start(SimpleNamespace(METRICS_PORT=port))
    sandbox_health.mark("http://judge-server:8080", True)
    url = f"https://127.0.0.1:{port}/metrics"

    # TLS 1.3 reports a missing client certificate after the handshake: the server's certificate_required alert, or
    # the reset that cuts the request off when it lands first. (Not any error: a plaintext server fails differently.)
    no_cert = ssl.create_default_context(cafile=FIXTURES / "ca.crt")
    with pytest.raises(OSError) as refused:
        urllib.request.urlopen(url, context=no_cert, timeout=5).read()
    error = refused.value.reason if isinstance(refused.value, urllib.error.URLError) else refused.value
    assert (isinstance(error, ConnectionResetError)
            or getattr(error, "reason", "") == "TLSV13_ALERT_CERTIFICATE_REQUIRED")

    probe = ssl.create_default_context(cafile=FIXTURES / "ca.crt")
    probe.load_cert_chain(FIXTURES / "tls-probe.crt", FIXTURES / "tls-probe.key")
    with urllib.request.urlopen(url, context=probe, timeout=5) as response:
        assert response.status == 200
        assert 'oj_sandbox_up{sandbox="judge-server"} 1.0' in response.read().decode()
