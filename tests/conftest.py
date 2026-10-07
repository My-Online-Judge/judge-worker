import pytest

import sandbox_health


@pytest.fixture(autouse=True)
def _fresh_sandbox_health():
    """The health table is process-wide, like the dispatcher's round-robin counter: start every test without marks."""
    sandbox_health.reset()
    yield
    sandbox_health.reset()
