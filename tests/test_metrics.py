import shutil
import socket
import ssl
import time
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


def start_tls_metrics(tls_dir):
    """The worker's identity as tls-init installs it (here the fixture server leaf: SAN localhost, 127.0.0.1), and
    /metrics started with it. Returns the port."""
    for src, dst in (("ca.crt", "ca.crt"), ("server.crt", "tls.crt"), ("server.key", "tls.key")):
        shutil.copy(FIXTURES / src, tls_dir / dst)
    port = free_port()
    metrics.start(SimpleNamespace(METRICS_PORT=port))
    return port


def scrape_as_probe(port, timeout=5):
    """GET /metrics presenting the tls-probe certificate."""
    probe = ssl.create_default_context(cafile=FIXTURES / "ca.crt")
    probe.load_cert_chain(FIXTURES / "tls-probe.crt", FIXTURES / "tls-probe.key")
    return urllib.request.urlopen(f"https://127.0.0.1:{port}/metrics", context=probe, timeout=timeout)


def test_metrics_server_requires_client_cert(tls_dir):
    port = start_tls_metrics(tls_dir)
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

    with scrape_as_probe(port) as response:
        assert response.status == 200
        assert 'oj_sandbox_up{sandbox="judge-server"} 1.0' in response.read().decode()


def test_a_silent_client_does_not_stall_tls_scrapes(tls_dir):
    port = start_tls_metrics(tls_dir)

    # Connected first and never sends a byte (anything on oj-net or sandbox-net can do this): a scrape behind it is
    # still answered, well inside the handshake timeout.
    with socket.create_connection(("127.0.0.1", port)):
        started = time.monotonic()
        with scrape_as_probe(port, timeout=3) as response:
            assert response.status == 200
        assert time.monotonic() - started < metrics.TLS_HANDSHAKE_TIMEOUT_SECONDS


def test_a_silent_client_is_dropped_after_the_handshake_timeout(tls_dir, monkeypatch):
    monkeypatch.setattr(metrics, "TLS_HANDSHAKE_TIMEOUT_SECONDS", 0.5, raising=False)
    port = start_tls_metrics(tls_dir)

    with socket.create_connection(("127.0.0.1", port)) as silent:
        silent.settimeout(3)
        assert silent.recv(1) == b""  # the server hung up: a silent client holds a thread for the timeout, no longer


def test_a_client_silent_after_the_handshake_is_dropped_without_a_traceback(tls_dir, monkeypatch, capfd):
    monkeypatch.setattr(metrics, "TLS_HANDSHAKE_TIMEOUT_SECONDS", 0.5, raising=False)
    port = start_tls_metrics(tls_dir)
    probe = ssl.create_default_context(cafile=FIXTURES / "ca.crt")
    probe.load_cert_chain(FIXTURES / "tls-probe.crt", FIXTURES / "tls-probe.key")

    # The handshake completes with an accepted certificate, then no request comes: the server times out reading it.
    with probe.wrap_socket(socket.create_connection(("127.0.0.1", port)), server_hostname="localhost") as client:
        client.settimeout(3)
        try:
            assert client.recv(1) == b""  # the server hung up after the timeout
        except (ssl.SSLError, ConnectionResetError):
            pass  # ...without close_notify, or with a reset: hung up all the same

    # Dropped like a failed handshake: socketserver's handle_error would have printed a traceback by now (it runs
    # before the connection is closed).
    assert capfd.readouterr().err == ""
