"""Which sandboxes are answering (sub-project 6 §3.2 C). The ping thread (sandbox_status) and every dispatch write
it; dispatch reads it to try the healthy sandboxes first, and oj_sandbox_up exposes it. Process-wide, like the
dispatcher's round-robin counter."""
import threading
from urllib.parse import urlparse

from prometheus_client import Gauge

SANDBOX_UP = Gauge("oj_sandbox_up", "1 if the sandbox answered its last ping or dispatch, 0 if it did not",
                   ["sandbox"])

_lock = threading.Lock()
_down = set()


def sandbox_label(url):
    """The sandbox's hostname (judge-server-2), as the registry and the alerts name it."""
    return urlparse(url).hostname or url


def mark(url, up):
    with _lock:
        if up:
            _down.discard(url)
        else:
            _down.add(url)
    SANDBOX_UP.labels(sandbox=sandbox_label(url)).set(1 if up else 0)


def order(urls):
    """The healthy sandboxes first, in the given order, then the ones marked down: a wrong mark never stops judging."""
    with _lock:
        down = set(_down)
    return [u for u in urls if u not in down] + [u for u in urls if u in down]


def reset():
    """Forget every mark (tests)."""
    with _lock:
        _down.clear()
    SANDBOX_UP.clear()
