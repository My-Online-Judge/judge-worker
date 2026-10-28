"""Verdict codes — MUST match submission-service's SubmissionResult enum exactly."""

COMPILE_ERROR = -2
WRONG_ANSWER = -1
ACCEPTED = 0
CPU_TIME_LIMIT_EXCEEDED = 1
REAL_TIME_LIMIT_EXCEEDED = 2
MEMORY_LIMIT_EXCEEDED = 3
RUNTIME_ERROR = 4
SYSTEM_ERROR = 5


def incomplete_reason(details, expected_count=None):
    """Why details cannot be a full judgement, or None. A sandbox that returns fewer results than the bundle has cases
    (or none) has not judged the submission, so "no case failed" proves nothing: it must never read as ACCEPTED."""
    if not details:
        return "Judge returned 0 results"
    if expected_count is not None and len(details) != expected_count:
        return f"Judge returned {len(details)} of {expected_count} test case results"
    return None


def resolve_verdict(details, expected_count=None):
    """Resolve verdict from testcase details. The logic judge-api's resolveVerdict had; the worker now owns it.

    Rules (in order):
    0. No results, or fewer than expected_count (the bundle's test_case_number, a run's inline cases) -> SYSTEM_ERROR
    1. Sandbox error (error != 0) anywhere -> SYSTEM_ERROR (overrides all)
    2. A case without a result is not a pass -> SYSTEM_ERROR
    3. First failing testcase (result != 0) wins
    4. All pass -> ACCEPTED
    """
    if incomplete_reason(details, expected_count):
        return SYSTEM_ERROR
    verdict_code = ACCEPTED
    for item in details:
        error = item.get("error")
        if error is not None and error != 0:
            return SYSTEM_ERROR
        result = item.get("result")
        if result is None:
            return SYSTEM_ERROR
        if verdict_code == ACCEPTED and result != 0:
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

    CompileError -> COMPILE_ERROR
    Any other error -> SYSTEM_ERROR, SPJCompileError included: a checker that does not compile is the problem's
    fault, not the contestant's.
    """
    if err == "CompileError":
        return COMPILE_ERROR
    return SYSTEM_ERROR
