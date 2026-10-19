import importlib
import re

import pytest

import config
import tls


def test_disabled_returns_empty(monkeypatch):
    monkeypatch.delenv("OJ_TLS_DIR", raising=False)
    importlib.reload(config)

    assert tls.enabled() is False
    assert tls.kafka_kwargs() == {}


def test_enabled_returns_ssl_kwargs(tls_dir):
    ca, cert, key = (str(tls_dir / name) for name in ("ca.crt", "tls.crt", "tls.key"))

    assert tls.enabled() is True
    assert tls.paths() == (ca, cert, key)
    assert tls.kafka_kwargs() == {"security_protocol": "SSL", "ssl_cafile": ca, "ssl_certfile": cert,
                                  "ssl_keyfile": key, "ssl_check_hostname": True}


@pytest.mark.parametrize("kwargs", ["kafka_kwargs", "requests_kwargs", "metrics_kwargs"])
def test_missing_file_raises_with_name(tls_dir, kwargs):
    (tls_dir / "tls.key").unlink()

    # With TLS on, a missing certificate stops the worker; it never falls back to plaintext.
    with pytest.raises(RuntimeError, match=re.escape(str(tls_dir / "tls.key"))):
        getattr(tls, kwargs)()


def test_requests_kwargs_disabled(monkeypatch):
    monkeypatch.delenv("OJ_TLS_DIR", raising=False)
    importlib.reload(config)

    assert tls.requests_kwargs() == {}


def test_requests_kwargs_enabled(tls_dir):
    ca, cert, key = (str(tls_dir / name) for name in ("ca.crt", "tls.crt", "tls.key"))

    # The worker's certificate presented; the service verified against the CA (requests also checks the host name).
    assert tls.requests_kwargs() == {"cert": (cert, key), "verify": ca}


def test_metrics_kwargs_disabled(monkeypatch):
    monkeypatch.delenv("OJ_TLS_DIR", raising=False)
    importlib.reload(config)

    assert tls.metrics_kwargs() == {}


def test_metrics_kwargs_enabled(tls_dir):
    ca, cert, key = (str(tls_dir / name) for name in ("ca.crt", "tls.crt", "tls.key"))

    assert tls.metrics_kwargs() == {"certfile": cert, "keyfile": key, "client_cafile": ca,
                                    "client_auth_required": True}
