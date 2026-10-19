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


def test_missing_file_raises_with_name(tls_dir):
    (tls_dir / "tls.key").unlink()

    # With TLS on, a missing certificate stops the worker; it never falls back to plaintext.
    with pytest.raises(RuntimeError, match=re.escape(str(tls_dir / "tls.key"))):
        tls.kafka_kwargs()
