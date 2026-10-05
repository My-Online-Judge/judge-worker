"""Verdict codes — MUST match submission-service's SubmissionResult enum exactly."""

COMPILE_ERROR = -2
WRONG_ANSWER = -1
ACCEPTED = 0
CPU_TIME_LIMIT_EXCEEDED = 1
REAL_TIME_LIMIT_EXCEEDED = 2
MEMORY_LIMIT_EXCEEDED = 3
RUNTIME_ERROR = 4
SYSTEM_ERROR = 5


def resolve_verdict(details):
    """Resolve verdict from testcase details. The logic judge-api's resolveVerdict had; the worker now owns it.

    Rules (in order):
    1. Sandbox error (error != 0) anywhere -> SYSTEM_ERROR (overrides all)
    2. First failing testcase (result != 0) wins
    3. All pass -> ACCEPTED
    """
    verdict_code = ACCEPTED
    for item in details:
        error = item.get("error")
        if error is not None and error != 0:
            return SYSTEM_ERROR
        result = item.get("result")
        if verdict_code == ACCEPTED and result is not None and result != 0:
            verdict_code = result
    return verdict_code


def compute_stats(details):
    """Compute statistics from testcase details.

    Returns: (max_cpu_time, max_real_time, max_memory, first_fail_result)
    Treats None values as 0.
    """
    if not details:
        return 0, 0, 0, 0
    max_cpu = max((d.get("cpu_time") or 0) for d in details)
    max_real = max((d.get("real_time") or 0) for d in details)
    max_mem = max((d.get("memory") or 0) for d in details)
    first_fail = 0
    for d in details:
        r = d.get("result")
        if r is not None and r != 0:
            first_fail = r
            break
    return max_cpu, max_real, max_mem, first_fail


def map_envelope_error(err):
    """Map envelope error string to verdict code.

    CompileError or SPJCompileError -> COMPILE_ERROR
    Any other error -> SYSTEM_ERROR
    """
    if err in ("CompileError", "SPJCompileError"):
        return COMPILE_ERROR
    return SYSTEM_ERROR
