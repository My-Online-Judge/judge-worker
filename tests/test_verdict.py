import verdict


def test_all_pass_is_accepted():
    details = [{"error": 0, "result": 0, "cpu_time": 5, "real_time": 6, "memory": 100},
               {"error": 0, "result": 0, "cpu_time": 3, "real_time": 4, "memory": 90}]
    assert verdict.resolve_verdict(details) == verdict.ACCEPTED


def test_first_failing_result_wins():
    details = [{"error": 0, "result": 0},
               {"error": 0, "result": verdict.WRONG_ANSWER},
               {"error": 0, "result": verdict.MEMORY_LIMIT_EXCEEDED}]
    assert verdict.resolve_verdict(details) == verdict.WRONG_ANSWER


def test_sandbox_error_overrides_everything():
    details = [{"error": 0, "result": verdict.WRONG_ANSWER},
               {"error": -1, "result": 0}]
    assert verdict.resolve_verdict(details) == verdict.SYSTEM_ERROR


def test_compute_stats_takes_maxima_and_first_fail():
    details = [{"error": 0, "result": 0, "cpu_time": 5, "real_time": 6, "memory": 100},
               {"error": 0, "result": 3, "cpu_time": 9, "real_time": 2, "memory": 400}]
    assert verdict.compute_stats(details) == (9, 6, 400, 3)


def test_compute_stats_handles_none_values():
    details = [{"cpu_time": None, "real_time": None, "memory": None, "result": 0}]
    assert verdict.compute_stats(details) == (0, 0, 0, 0)


def test_map_envelope_error():
    assert verdict.map_envelope_error("CompileError") == verdict.COMPILE_ERROR
    assert verdict.map_envelope_error("SPJCompileError") == verdict.COMPILE_ERROR
    assert verdict.map_envelope_error("TokenVerificationFailed") == verdict.SYSTEM_ERROR
