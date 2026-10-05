"""Report each sandbox's liveness to submission-service's judge-server registry, on the sandbox's behalf.

The sandboxes run untrusted code, so they sit on an internal network with no route to
submission-service. The worker reaches both: it asks each sandbox for its /ping (the same server_info
QingdaoU's own heartbeat sends) and forwards it as that sandbox's heartbeat. A sandbox that stops
answering simply stops being reported, and the registry marks it offline after its 30 s window.
"""
import logging
import threading
import time

import requests

from dispatcher import token_header

log = logging.getLogger("judge-worker")


def report_once(urls, token, heartbeat_url, timeout=3):
    digest = token_header(token)
    for url in urls:
        try:
            pong = requests.post(f"{url}/ping", json={}, headers={"X-Judge-Server-Token": digest}, timeout=timeout)
            pong.raise_for_status()
            info = pong.json()["data"]
            info.update(action="heartbeat", service_url=url)
            requests.post(heartbeat_url, json=info, headers={"X-JUDGE-SERVER-TOKEN": digest},
                          timeout=timeout).raise_for_status()
        except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
            log.warning("sandbox %s: status not reported (%s)", url, exc)


def start(config):
    """Report every SANDBOX_HEARTBEAT_INTERVAL_SECONDS on a daemon thread; None when no URL is configured."""
    if not config.SANDBOX_HEARTBEAT_URL:
        return None

    def loop():
        while True:
            report_once(config.JUDGE_SERVER_URLS, config.JUDGE_SERVER_TOKEN, config.SANDBOX_HEARTBEAT_URL)
            time.sleep(config.SANDBOX_HEARTBEAT_INTERVAL_SECONDS)

    thread = threading.Thread(target=loop, name="sandbox-status", daemon=True)
    thread.start()
    return thread
