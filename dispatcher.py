import hashlib
import itertools
import logging

import requests
from prometheus_client import Counter

import sandbox_health
import tls

log = logging.getLogger("judge-worker")

_JUDGE_BODY_FIELDS = ("src", "language_config", "max_cpu_time", "max_memory", "test_case_id")
# A checker problem's checker, under the names qduoj's JudgeServer.judge takes (SP13). submission-service sends all four
# or none.
_SPJ_FIELDS = ("spj_version", "spj_config", "spj_compile_config", "spj_src")

# Judge requests sent to each sandbox, by outcome. E4's c3 criterion counts the failed ones (sub-project 6 §3.2 C).
DISPATCHES = Counter("oj_judge_dispatch", "Judge requests sent to a sandbox, by outcome", ["sandbox", "outcome"])

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
    if event.get("spj_version"):
        body.update({field: event[field] for field in _SPJ_FIELDS})
    return body


def call_judge_server(base_url, token, body, timeout=30):
    # Over mutual TLS with the worker's certificate: the sandbox's gunicorn requires one signed by the CA.
    resp = requests.post(
        f"{base_url}/judge",
        json=body,
        headers={
            "X-Judge-Server-Token": token_header(token),
            "Content-Type": "application/json",
        },
        timeout=timeout,
        **tls.requests_kwargs(),
    )
    resp.raise_for_status()
    return resp.json()


def _is_retryable(exc):
    """A dead/unhealthy sandbox is worth failing over: connection errors (incl. connect timeouts) and 5xx.
    A 4xx is a deterministic client error (same request fails everywhere) — do not fail over.
    Nor on a read timeout: the sandbox has the request and is still judging, so a second sandbox would
    judge the same submission again — double the CPU exactly when the sandboxes are busiest."""
    if isinstance(exc, requests.HTTPError):
        resp = exc.response
        return resp is not None and 500 <= resp.status_code < 600
    if isinstance(exc, requests.ReadTimeout):
        return False
    return isinstance(exc, requests.RequestException)


def dispatch_judge(urls, token, body, timeout=30):
    """Send the judge request to one of `urls`: the healthy sandboxes first, each list in round-robin order, failing
    over to the next candidate on a retryable error. A retryable error marks that sandbox down and a success marks it
    up (sandbox_health). Raises the last error if every candidate fails (or ValueError if no URLs are configured)."""
    if not urls:
        raise ValueError("no judge-server URLs configured")
    start = next(_round_robin) % len(urls)
    ordered = sandbox_health.order(urls[start:] + urls[:start])
    last_exc = None
    for url in ordered:
        label = sandbox_health.sandbox_label(url)
        try:
            result = call_judge_server(url, token, body, timeout=timeout)
        except requests.RequestException as exc:
            DISPATCHES.labels(sandbox=label, outcome="failed").inc()
            if not _is_retryable(exc):
                raise
            sandbox_health.mark(url, False)
            last_exc = exc
            log.warning("judge-server %s failed (%s), trying next", url, exc)
            continue
        DISPATCHES.labels(sandbox=label, outcome="ok").inc()
        sandbox_health.mark(url, True)
        return result
    raise last_exc
