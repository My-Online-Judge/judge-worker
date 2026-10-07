from prometheus_client import REGISTRY

import sandbox_health

A, B, C = "http://judge-server:8080", "http://judge-server-2:8080", "http://judge-server-3:8080"


def up(sandbox):
    return REGISTRY.get_sample_value("oj_sandbox_up", {"sandbox": sandbox})


def test_every_sandbox_counts_as_healthy_until_marked_down():
    assert sandbox_health.order([A, B, C]) == [A, B, C]


def test_a_sandbox_marked_down_is_tried_last():
    sandbox_health.mark(A, False)
    assert sandbox_health.order([A, B, C]) == [B, C, A]


def test_all_marked_down_keeps_the_given_order():
    for url in (A, B, C):
        sandbox_health.mark(url, False)
    assert sandbox_health.order([B, C, A]) == [B, C, A]


def test_a_sandbox_marked_up_again_is_tried_in_turn():
    sandbox_health.mark(A, False)
    sandbox_health.mark(A, True)
    assert sandbox_health.order([A, B]) == [A, B]


def test_the_gauge_follows_the_marks_by_hostname():
    sandbox_health.mark(A, True)
    sandbox_health.mark(B, False)
    assert up("judge-server") == 1
    assert up("judge-server-2") == 0
