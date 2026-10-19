import importlib
import os

import pytest

# The worker refuses to start without its judge-server token (Config.validate); config reads it at import, so every test
# has one unless it removes it.
os.environ.setdefault("JUDGE_SERVER_TOKEN", "test-token")

import config  # noqa: E402
import sandbox_health  # noqa: E402


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
