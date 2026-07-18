import verdict
from dispatcher import build_judge_body, dispatch_judge
from test_case_sync import ensure_present


def build_judged_event(submission_id, judge_response):
    err = judge_response.get("err")
    data = judge_response.get("data")

    # Case 1: envelope-level error, e.g. {"err":"CompileError","data":"..."}.
    if err is not None:
        extra = "" if data is None else f": {data}"
        return _event(submission_id, verdict.map_envelope_error(err),
                      None, 0, 0, 0, f"{err}{extra}", [])

    # Case 2: per-testcase result array.
    if isinstance(data, list):
        status = verdict.resolve_verdict(data)
        max_cpu, max_real, max_mem, first_fail = verdict.compute_stats(data)
        return _event(submission_id, status, first_fail,
                      max_cpu, max_real, max_mem, None, data)

    # Case 3: anything else is a malformed response.
    return _event(submission_id, verdict.SYSTEM_ERROR,
                  None, 0, 0, 0, "Unexpected judge response", [])


def _event(submission_id, status, result, cpu, real, mem, error_message, details):
    return {
        "submissionId": submission_id,
        "status": status,
        "result": result,
        "cpu_time": cpu,
        "real_time": real,
        "memory": mem,
        "error_message": error_message,
        "details": details,
    }


def system_error_event(submission_id, message):
    """Build a SYSTEM_ERROR judged-event envelope (shared with worker.py)."""
    return _event(submission_id, verdict.SYSTEM_ERROR, None, 0, 0, 0, message, [])


def process_event(event, config):
    ensure_present(event["test_case_id"])
    body = build_judge_body(event)
    judge_response = dispatch_judge(
        config.JUDGE_SERVER_URLS, config.JUDGE_SERVER_TOKEN, body,
        timeout=config.JUDGE_TIMEOUT_SECONDS,
    )
    return build_judged_event(event["submissionId"], judge_response)
