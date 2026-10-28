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
    # A checker that does not compile is the problem's fault, not the contestant's (SP13).
    assert verdict.map_envelope_error("SPJCompileError") == verdict.SYSTEM_ERROR
    assert verdict.map_envelope_error("TokenVerificationFailed") == verdict.SYSTEM_ERROR


def test_no_results_is_system_error_not_accepted():
    assert verdict.resolve_verdict([]) == verdict.SYSTEM_ERROR


def test_fewer_results_than_test_cases_is_system_error():
    assert verdict.resolve_verdict([{"error": 0, "result": 0}], expected_count=2) == verdict.SYSTEM_ERROR


def test_a_wrong_answer_is_still_reported_when_every_case_has_a_result():
    details = [{"error": 0, "result": 0}, {"error": 0, "result": -1}]
    assert verdict.resolve_verdict(details, expected_count=2) == verdict.WRONG_ANSWER


def test_all_cases_present_and_passing_is_accepted():
    assert verdict.resolve_verdict([{"error": 0, "result": 0}] * 2, expected_count=2) == verdict.ACCEPTED


def test_a_case_without_a_result_does_not_count_as_a_pass():
    assert verdict.resolve_verdict([{"error": 0}], expected_count=1) == verdict.SYSTEM_ERROR
