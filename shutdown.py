"""Graceful shutdown (sub-project 15): Kubernetes and `docker stop` send SIGTERM, and the worker must not drop a
submission it is judging. The handler only raises a flag; run() checks it between records and inside wait(), so the
current submission is judged, published and committed first."""
import logging
import signal
import threading

log = logging.getLogger("judge-worker")


class Shutdown:
    def __init__(self):
        self._event = threading.Event()

    @property
    def requested(self):
        return self._event.is_set()

    def request(self):
        self._event.set()

    def install(self, signals=(signal.SIGTERM, signal.SIGINT)):
        """Turn each signal into the flag. Must run in the main thread."""
        for sig in signals:
            signal.signal(sig, self._on_signal)
        return self

    def _on_signal(self, signum, frame):
        log.warning("Received signal %s: finishing the current submission, then exiting", signal.Signals(signum).name)
        self.request()
