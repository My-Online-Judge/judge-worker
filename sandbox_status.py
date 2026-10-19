"""Ping every sandbox, keep sandbox_health current, and report each answer to submission-service's judge-server
registry on the sandbox's behalf.

The sandboxes run untrusted code, so they sit on an internal network with no route to submission-service. The worker
reaches both: it asks each sandbox for its /ping (the same server_info QingdaoU's own heartbeat sends), marks the
sandbox up or down in sandbox_health (dispatch tries the healthy ones first; oj_sandbox_up reads the same marks), and
forwards the answer as that sandbox's heartbeat when SANDBOX_HEARTBEAT_URL is set. A sandbox that stops answering is
marked down at once and simply stops being reported; the registry marks it offline after its 30 s window.
"""
import logging
import threading
import time

import requests

import sandbox_health
import tls
from dispatcher import token_header

log = logging.getLogger("judge-worker")


def report_once(urls, token, heartbeat_url, timeout=3, *, tls_kwargs=None):
    """One round. tls_kwargs: tls.requests_kwargs(), read once by start(); the sandboxes and submission-service both
    require the worker's certificate."""
    tls_kwargs = tls_kwargs or {}
    digest = token_header(token)
    for url in urls:
        try:
            pong = requests.post(f"{url}/ping", json={}, headers={"X-Judge-Server-Token": digest}, timeout=timeout,
                                 **tls_kwargs)
            pong.raise_for_status()
            info = dict(pong.json()["data"])
        # OSError: requests' RequestException is one, and so is what requests raises for a certificate file that is gone
        # (no request sent). Either way this sandbox is unreachable: marked down, and the round goes on to the next.
        except (OSError, ValueError, KeyError, TypeError) as exc:
            sandbox_health.mark(url, False)
            log.warning("sandbox %s: no answer to /ping (%s)", url, exc)
            continue
        sandbox_health.mark(url, True)
        if not heartbeat_url:
            continue
        info.update(action="heartbeat", service_url=url)
        try:
            # Over mutual TLS with the worker's certificate: submission-service allow-lists the CN judge-worker.
            requests.post(heartbeat_url, json=info, headers={"X-JUDGE-SERVER-TOKEN": digest},
                          timeout=timeout, **tls_kwargs).raise_for_status()
        except requests.RequestException as exc:
            log.warning("sandbox %s: status not reported (%s)", url, exc)


def start(config):
    """Ping every SANDBOX_HEARTBEAT_INTERVAL_SECONDS on a daemon thread; None when no sandbox is configured. With TLS on
    and a certificate file missing, raises here: the worker does not start (never falls back to plaintext)."""
    if not config.JUDGE_SERVER_URLS:
        return None
    tls_kwargs = tls.requests_kwargs()

    def loop():
        while True:
            try:
                report_once(config.JUDGE_SERVER_URLS, config.JUDGE_SERVER_TOKEN, config.SANDBOX_HEARTBEAT_URL,
                            tls_kwargs=tls_kwargs)
            except Exception:  # e.g. a certificate file gone since start: one lost round, never the thread
                log.exception("sandbox status round failed")
            time.sleep(config.SANDBOX_HEARTBEAT_INTERVAL_SECONDS)

    thread = threading.Thread(target=loop, name="sandbox-status", daemon=True)
    thread.start()
    return thread
