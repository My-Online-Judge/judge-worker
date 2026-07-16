import hashlib
import itertools
import logging

import requests

log = logging.getLogger("judge-worker")

_JUDGE_BODY_FIELDS = ("src", "language_config", "max_cpu_time", "max_memory", "test_case_id")

# Rotating counter for round-robin start selection across configured judge-servers.
_round_robin = itertools.count()


def reset_round_robin():
    """Reset the round-robin counter (used by tests for deterministic start selection)."""
    global _round_robin
    _round_robin = itertools.count()


def token_header(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def build_judge_body(event):
    body = {field: event[field] for field in _JUDGE_BODY_FIELDS}
    body["output"] = event.get("output", True)
    return body


def call_judge_server(base_url, token, body, timeout=30):
    resp = requests.post(
        f"{base_url}/judge",
        json=body,
        headers={
            "X-Judge-Server-Token": token_header(token),
            "Content-Type": "application/json",
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def _is_retryable(exc):
    """A dead/unhealthy sandbox is worth failing over: connection errors, timeouts and 5xx.
    A 4xx is a deterministic client error (same request fails everywhere) — do not fail over."""
    if isinstance(exc, requests.HTTPError):
        resp = exc.response
        return resp is not None and 500 <= resp.status_code < 600
    return isinstance(exc, requests.RequestException)


def dispatch_judge(urls, token, body, timeout=30):
    """Send the judge request to one of `urls`, starting from a round-robin offset and failing
    over to the next candidate on a retryable error. Raises the last error if every candidate
    fails (or ValueError if no URLs are configured)."""
    if not urls:
        raise ValueError("no judge-server URLs configured")
    start = next(_round_robin) % len(urls)
    ordered = urls[start:] + urls[:start]
    last_exc = None
    for url in ordered:
        try:
            return call_judge_server(url, token, body, timeout=timeout)
        except requests.RequestException as exc:
            if not _is_retryable(exc):
                raise
            last_exc = exc
            log.warning("judge-server %s failed (%s), trying next", url, exc)
    raise last_exc
