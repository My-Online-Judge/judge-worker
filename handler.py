import logging
import verdict
from event_limits import limit_event
from dispatcher import build_judge_body, dispatch_judge
from log_context import submission_id_var
from test_case_sync import ensure_present, maybe_sweep

log = logging.getLogger("judge-worker")

# What the submitter sees when the problem's checker does not compile. qduoj's data is then the checker's compiler
# output, which quotes the checker's source: it stays in the worker's log.
SPJ_COMPILE_ERROR_MESSAGE = "SPJCompileError: the problem's checker does not compile"

# A record names its job by one of these (Run code design §8): a submission by submissionId, a run by runId. The
# verdict echoes the one the request used, under the same name.
JOB_ID_KEYS = ("submissionId", "runId")


def job_identity(event):
    """(key, id) of the job the event names — ("submissionId", …) or ("runId", …) — or None when it names neither,
    or is not a JSON object at all (`[]`, `1`, `"x"` parse fine but name nothing)."""
    if not isinstance(event, dict):
        return None
    for key in JOB_ID_KEYS:
        value = event.get(key)
        if value:
            return key, str(value)
    return None


def build_judged_event(submission_id, judge_response, id_key="submissionId"):
    err = judge_response.get("err")
    data = judge_response.get("data")

    # Case 1: envelope-level error, e.g. {"err":"CompileError","data":"..."}.
    if err is not None:
        if err == "SPJCompileError":
            log.warning("Submission %s: the problem's checker does not compile: %s", submission_id, data)
            message = SPJ_COMPILE_ERROR_MESSAGE
        else:
            message = err if data is None else f"{err}: {data}"
        return _event(submission_id, verdict.map_envelope_error(err),
                      None, 0, 0, 0, message, [], id_key)

    # Case 2: per-testcase result array.
    if isinstance(data, list):
        status = verdict.resolve_verdict(data)
        max_cpu, max_real, max_mem, first_fail = verdict.compute_stats(data)
        return _event(submission_id, status, first_fail,
                      max_cpu, max_real, max_mem, None, data, id_key)

    # Case 3: anything else is a malformed response.
    return _event(submission_id, verdict.SYSTEM_ERROR,
                  None, 0, 0, 0, "Unexpected judge response", [], id_key)


def _event(submission_id, status, result, cpu, real, mem, error_message, details, id_key="submissionId"):
    return limit_event({
        id_key: submission_id,
        "status": status,
        "result": result,
        "cpu_time": cpu,
        "real_time": real,
        "memory": mem,
        "error_message": error_message,
        "details": details,
    })


def system_error_event(submission_id, message, id_key="submissionId"):
    """Build a SYSTEM_ERROR judged-event envelope (shared with worker.py), named as the request was."""
    return _event(submission_id, verdict.SYSTEM_ERROR, None, 0, 0, 0, message, [], id_key)


def process_event(event, config, on_started=None):
    key, job_id = job_identity(event) or ("submissionId", "-")
    token = submission_id_var.set(job_id)
    try:
        # A run (Run code design §8) carries its cases inline: no bundle to sync, no cache to sweep, and no start to
        # announce (there is no submission row to move to JUDGING). A submission names its bundle.
        inline = "test_case" in event
        log.info("Judging %s %s", "run" if inline else "submission", job_id)
        if not inline:
            maybe_sweep()  # rate-limited; prunes stale cached bundles
            ensure_present(event["test_case_id"])
            if on_started is not None:
                on_started()
        body = build_judge_body(event)
        timeout = config.JUDGE_TIMEOUT_SECONDS
        if "spj_version" in body:
            # The sandbox may first have to compile the checker (Config.SPJ_COMPILE_ALLOWANCE_SECONDS).
            timeout += config.SPJ_COMPILE_ALLOWANCE_SECONDS
        judge_response = dispatch_judge(
            config.JUDGE_SERVER_URLS, config.JUDGE_SERVER_TOKEN, body, timeout=timeout,
        )
        return build_judged_event(job_id, judge_response, id_key=key)
    finally:
        submission_id_var.reset(token)
