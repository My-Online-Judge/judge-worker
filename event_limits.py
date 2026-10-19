"""Size caps that keep a judged event inside one Kafka message."""
import json

MAX_OUTPUT_BYTES = 4096
MAX_ERROR_CHARS = 65536
MAX_EVENT_BYTES = 900_000
TRUNCATION_MARK = "\n…[truncated]"


def _cut_output(text):
    raw = text.encode("utf-8")
    if len(raw) <= MAX_OUTPUT_BYTES:
        return text
    return raw[:MAX_OUTPUT_BYTES].decode("utf-8", errors="ignore") + TRUNCATION_MARK


def _size(event):
    return len(json.dumps(event).encode("utf-8"))


def limit_event(event):
    """Return a copy of event with per-case output, error text and total size capped."""
    out = dict(event)
    msg = out.get("error_message")
    if isinstance(msg, str) and len(msg) > MAX_ERROR_CHARS:
        out["error_message"] = msg[:MAX_ERROR_CHARS] + TRUNCATION_MARK

    details = out.get("details")
    if isinstance(details, list):
        cases = []
        for case in details:
            if isinstance(case, dict) and isinstance(case.get("output"), str):
                case = {**case, "output": _cut_output(case["output"])}
            cases.append(case)
        out["details"] = cases
        if _size(out) > MAX_EVENT_BYTES:
            out["details"] = [
                {k: v for k, v in c.items() if k != "output"} if isinstance(c, dict) else c
                for c in cases
            ]
    return out
