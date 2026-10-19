import importlib

import pytest

import config
import sandbox_health


@pytest.fixture(autouse=True)
def _fresh_sandbox_health():
    """The health table is process-wide, like the dispatcher's round-robin counter: start every test without marks."""
    sandbox_health.reset()
    yield
    sandbox_health.reset()


@pytest.fixture
def tls_dir(monkeypatch, tmp_path):
    """TLS on: OJ_TLS_DIR names a directory holding ca.crt, tls.crt and tls.key. Config is reloaded with it, and
    reloaded again without it afterwards so no later test inherits TLS."""
    for name in ("ca.crt", "tls.crt", "tls.key"):
        (tmp_path / name).write_text(name)
    monkeypatch.setenv("OJ_TLS_DIR", str(tmp_path))
    importlib.reload(config)
    yield tmp_path
    monkeypatch.delenv("OJ_TLS_DIR", raising=False)
    importlib.reload(config)
