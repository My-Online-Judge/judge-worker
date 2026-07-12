import hashlib

import requests

_JUDGE_BODY_FIELDS = ("src", "language_config", "max_cpu_time", "max_memory", "test_case_id")


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
