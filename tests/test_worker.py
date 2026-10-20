import itertools
from unittest.mock import ANY, MagicMock, patch

import pytest
from kafka.errors import (CommitFailedError, KafkaError, KafkaTimeoutError, MessageSizeTooLargeError,
                          NoBrokersAvailable)
from kafka.structs import TopicPartition
from urllib3.exceptions import MaxRetryError
import requests
from prometheus_client import REGISTRY

import tls
import verdict
import worker


class Msg:
    """Stand-in for kafka-python's ConsumerRecord: the worker reads .value and .headers."""

    def __init__(self, value, headers=None):
        self.value = value
        self.headers = headers or []


def test_system_error_event_shape():
    e = worker.system_error_event("s1", "boom")
    assert e["submissionId"] == "s1"
    assert e["status"] == verdict.SYSTEM_ERROR
    assert e["error_message"] == "boom"
    assert e["details"] == []


def test_handle_one_publishes_result():
    producer = MagicMock()
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", JUDGE_TIMEOUT_SECONDS=30, PUBLISH_TIMEOUT_SECONDS=10)
    event = {"submissionId": "s2"}
    with patch("worker.process_event", return_value={"submissionId": "s2", "status": 0}):
        worker.handle_one(event, cfg, producer)
    producer.send.assert_called_once_with("submission.judged", key="s2",
                                           value={"submissionId": "s2", "status": 0},
                                           headers=ANY)
    producer.send.return_value.get.assert_called_once_with(timeout=10)


def test_handle_one_on_judge_failure_publishes_system_error():
    producer = MagicMock()
    cfg = MagicMock(JUDGED_TOPIC="submission.judged")
    event = {"submissionId": "s3"}
    with patch("worker.process_event", side_effect=RuntimeError("judge down")):
        worker.handle_one(event, cfg, producer)
    sent = producer.send.call_args.kwargs["value"]
    assert sent["status"] == verdict.SYSTEM_ERROR
    assert "judge down" in sent["error_message"]


def test_handle_one_raises_when_delivery_fails():
    producer = MagicMock()
    producer.send.return_value.get.side_effect = RuntimeError("broker rejected batch")
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", JUDGE_TIMEOUT_SECONDS=30)
    event = {"submissionId": "s5"}
    with patch("worker.process_event", return_value={"submissionId": "s5", "status": 0}):
        with pytest.raises(RuntimeError):
            worker.handle_one(event, cfg, producer)


@pytest.mark.parametrize("event", [{}, {"submissionId": None}, {"submissionId": ""}])
def test_handle_one_missing_submission_id_skips_without_publishing_or_raising(event):
    producer = MagicMock()
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", JUDGE_TIMEOUT_SECONDS=30)
    worker.handle_one(event, cfg, producer)  # must not raise
    producer.send.assert_not_called()


def test_run_commits_after_publish():
    manager = MagicMock()
    producer = manager.producer
    consumer = manager.consumer
    consumer.__iter__.return_value = [Msg({"submissionId": "s4"})]
    cfg = MagicMock(JUDGED_TOPIC="submission.judged")
    with patch("worker.process_event", return_value={"submissionId": "s4", "status": 0}):
        worker.run(cfg, consumer, producer)
    names = [c[0] for c in manager.mock_calls]
    assert "producer.send" in names and "consumer.commit" in names
    assert names.index("producer.send") < names.index("consumer.commit")


def test_handle_one_skips_none_event_without_publishing():
    producer = MagicMock()
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", JUDGE_TIMEOUT_SECONDS=30)
    worker.handle_one(None, cfg, producer)  # malformed message deserialized to None
    producer.send.assert_not_called()


def test_run_commits_after_skipping_malformed_message():
    manager = MagicMock()
    producer = manager.producer
    consumer = manager.consumer
    consumer.__iter__.return_value = [Msg(None)]  # poison pill: value is None
    cfg = MagicMock(JUDGED_TOPIC="submission.judged")
    worker.run(cfg, consumer, producer)
    names = [c[0] for c in manager.mock_calls]
    assert "producer.send" not in names   # nothing published for a poison pill
    assert "consumer.commit" in names      # but the offset advances past it


def test_safe_json_deserialize_valid():
    assert worker.safe_json_deserialize(b'{"submissionId": "s1"}') == {"submissionId": "s1"}


def test_safe_json_deserialize_malformed_returns_none():
    assert worker.safe_json_deserialize(b"not json") is None
    assert worker.safe_json_deserialize(b"\xff\xfe") is None  # invalid utf-8
    assert worker.safe_json_deserialize(None) is None  # null value (e.g. tombstone)


def retries():
    return REGISTRY.get_sample_value("oj_verdict_publish_retries_total") or 0


def test_backoff_doubles_from_one_second_and_stops_at_ten():
    assert list(itertools.islice(worker.backoff_delays(), 6)) == [1, 2, 4, 8, 10, 10]


def test_a_verdict_kafka_did_not_take_is_sent_again_not_judged_again():
    producer = MagicMock()
    producer.send.return_value.get.side_effect = [KafkaTimeoutError(), KafkaTimeoutError(), None]
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", PUBLISH_TIMEOUT_SECONDS=10)
    waits, before = [], retries()
    with patch("worker.process_event", return_value={"submissionId": "s6", "status": 0}) as judge:
        worker.handle_one({"submissionId": "s6"}, cfg, producer, wait=waits.append)
    assert judge.call_count == 1
    assert producer.send.call_count == 3
    assert {c.kwargs["value"]["submissionId"] for c in producer.send.call_args_list} == {"s6"}
    assert waits == [1, 2]
    assert retries() - before == 2


def test_a_send_that_cannot_even_start_is_retried():
    # kafka-python raises from send() itself when it gets no metadata within max_block_ms.
    producer = MagicMock()
    producer.send.side_effect = [KafkaTimeoutError(), MagicMock()]
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", PUBLISH_TIMEOUT_SECONDS=10)
    waits = []
    with patch("worker.process_event", return_value={"submissionId": "s10", "status": 0}):
        worker.handle_one({"submissionId": "s10"}, cfg, producer, wait=waits.append)
    assert producer.send.call_count == 2
    assert waits == [1]


def test_run_pauses_while_kafka_is_away_and_commits_only_after_the_send():
    manager = MagicMock()
    producer, consumer = manager.producer, manager.consumer
    consumer.__iter__.return_value = [Msg({"submissionId": "s7"})]
    producer.send.return_value.get.side_effect = [KafkaTimeoutError(), None]
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", PUBLISH_TIMEOUT_SECONDS=10)
    with patch("worker.process_event", return_value={"submissionId": "s7", "status": 0}):
        worker.run(cfg, consumer, producer)
    names = [c[0] for c in manager.mock_calls]
    assert names.index("consumer.pause") < names.index("consumer.poll") < names.index("consumer.resume") \
        < names.index("consumer.commit")
    assert consumer.poll.call_args.kwargs == {"timeout_ms": 1000}
    last_get = max(i for i, n in enumerate(names) if n == "producer.send().get")
    assert last_get < names.index("consumer.commit")


def test_a_poll_that_fails_while_kafka_is_away_does_not_stop_the_resend():
    manager = MagicMock()
    producer, consumer = manager.producer, manager.consumer
    consumer.__iter__.return_value = [Msg({"submissionId": "s11"})]
    consumer.poll.side_effect = KafkaError("coordinator unavailable")
    producer.send.return_value.get.side_effect = [KafkaTimeoutError(), None]
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", PUBLISH_TIMEOUT_SECONDS=10)
    with patch("worker.process_event", return_value={"submissionId": "s11", "status": 0}), \
            patch("worker.time.sleep") as slept:
        worker.run(cfg, consumer, producer)
    assert producer.send.call_count == 2
    slept.assert_called_once_with(1)
    assert consumer.commit.call_count == 1


def test_a_failed_commit_does_not_stop_the_worker():
    manager = MagicMock()
    consumer = manager.consumer
    consumer.__iter__.return_value = [Msg({"submissionId": "s8"}), Msg({"submissionId": "s9"})]
    consumer.commit.side_effect = [CommitFailedError(), None]
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", PUBLISH_TIMEOUT_SECONDS=10)
    with patch("worker.process_event", side_effect=lambda e, c, **kw: {"submissionId": e["submissionId"], "status": 0}):
        worker.run(cfg, consumer, manager.producer)
    assert manager.producer.send.call_count == 2
    assert consumer.commit.call_count == 2


def test_connect_waits_for_a_broker_instead_of_exiting():
    build = MagicMock(side_effect=[NoBrokersAvailable(), NoBrokersAvailable(), "consumer"])
    slept = []
    assert worker.connect(build, "cfg", sleep=slept.append) == "consumer"
    assert slept == [1, 2]
    build.assert_called_with("cfg")


def test_connect_does_not_retry_a_tls_misconfiguration(tls_dir):
    """A missing certificate is not Kafka being away: it leaves connect() at once and stops the worker (fail closed)."""
    (tls_dir / "tls.key").unlink()
    calls, slept = [], []

    def build(cfg):
        calls.append(cfg)
        # a retry would get a client here, so an except widened past NoBrokersAvailable fails this test, not hangs it
        return tls.kafka_kwargs() if len(calls) == 1 else "consumer"

    with pytest.raises(RuntimeError, match="tls.key"):
        worker.connect(build, "cfg", sleep=slept.append)
    assert calls == ["cfg"]
    assert slept == []


def test_a_record_the_wait_s_poll_fetches_is_rewound_not_dropped():
    # kafka-python 2.0.2 drops the paused flag when a rebalance completes inside poll(), which may then fetch a record
    # of another partition and advance its position: the wait must rewind it, or the next commit skips a submission.
    manager = MagicMock()
    producer, consumer = manager.producer, manager.consumer
    consumer.__iter__.return_value = [Msg({"submissionId": "s12"})]
    tp = TopicPartition("submission.requested", 3)
    consumer.poll.return_value = {tp: [MagicMock(offset=41)]}
    producer.send.return_value.get.side_effect = [KafkaTimeoutError(), None]
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", PUBLISH_TIMEOUT_SECONDS=10)
    with patch("worker.process_event", return_value={"submissionId": "s12", "status": 0}):
        worker.run(cfg, consumer, producer)
    consumer.seek.assert_called_once_with(tp, 41)
    names = [c[0] for c in manager.mock_calls]
    assert names.index("consumer.seek") < names.index("consumer.commit")


def test_a_verdict_kafka_refuses_for_good_is_replaced_by_a_system_error():
    # MessageSizeTooLargeError: the verdict carries the program's whole output; Kafka will never take it.
    producer = MagicMock()
    producer.send.return_value.get.side_effect = [MessageSizeTooLargeError(), None]
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", PUBLISH_TIMEOUT_SECONDS=10)
    waits = []
    with patch("worker.process_event", return_value={"submissionId": "s13", "status": 0, "details": ["x" * 10]}):
        worker.handle_one({"submissionId": "s13"}, cfg, producer, wait=waits.append)
    sent = producer.send.call_args_list[-1].kwargs["value"]
    assert sent["submissionId"] == "s13" and sent["status"] == verdict.SYSTEM_ERROR and sent["details"] == []
    assert waits == []


def test_a_system_error_kafka_also_refuses_is_raised_not_looped():
    producer = MagicMock()
    producer.send.return_value.get.side_effect = [MessageSizeTooLargeError(), MessageSizeTooLargeError()]
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", PUBLISH_TIMEOUT_SECONDS=10)
    with patch("worker.process_event", return_value={"submissionId": "s14", "status": 0}):
        with pytest.raises(MessageSizeTooLargeError):
            worker.handle_one({"submissionId": "s14"}, cfg, producer, wait=lambda s: None)


def test_the_seconds_spent_waiting_to_resend_are_counted():
    # oj_verdict_publish_retries_total reads 1-2 per outage (the first wait lasts the whole outage): the wait time is
    # what says how long verdicts waited for Kafka (follow-up 16).
    producer = MagicMock()
    producer.send.return_value.get.side_effect = [KafkaTimeoutError(), None]
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", PUBLISH_TIMEOUT_SECONDS=10)
    before = REGISTRY.get_sample_value("oj_verdict_publish_wait_seconds_total") or 0
    with patch("worker.process_event", return_value={"submissionId": "s15", "status": 0}), \
            patch("worker.time.monotonic", side_effect=[0.0, 100.0, 107.5]):
        worker.handle_one({"submissionId": "s15"}, cfg, producer, wait=lambda s: None)
    assert (REGISTRY.get_sample_value("oj_verdict_publish_wait_seconds_total") or 0) - before == 7.5


def test_started_event_shape():
    import re
    producer = MagicMock()
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", STARTED_TOPIC="submission.started", WORKER_ID="w1",
                    PUBLISH_TIMEOUT_SECONDS=10)

    def fake_process(event, config, on_started=None):
        on_started()
        return {"submissionId": "s20", "status": 0}

    with patch("worker.process_event", side_effect=fake_process):
        worker.handle_one({"submissionId": "s20"}, cfg, producer)

    started = [c for c in producer.send.call_args_list if c.args[0] == "submission.started"]
    assert len(started) == 1
    assert started[0].kwargs["key"] == "s20"
    value = started[0].kwargs["value"]
    assert set(value) == {"submissionId", "worker", "startedAt"}
    assert value["submissionId"] == "s20" and value["worker"] == "w1"
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", value["startedAt"])
    # advisory: the start's future is never waited on (only the verdict's is)
    producer.send.return_value.get.assert_called_once_with(timeout=10)


def test_a_failed_start_publish_does_not_stop_judging():
    producer = MagicMock()

    def send(topic, **kw):
        if topic == "submission.started":
            raise KafkaTimeoutError()
        return MagicMock()

    producer.send.side_effect = send
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", STARTED_TOPIC="submission.started", WORKER_ID="w1",
                    PUBLISH_TIMEOUT_SECONDS=10)

    def fake_process(event, config, on_started=None):
        on_started()
        return {"submissionId": "s21", "status": 0}

    with patch("worker.process_event", side_effect=fake_process):
        worker.handle_one({"submissionId": "s21"}, cfg, producer)

    judged = [c for c in producer.send.call_args_list if c.args[0] == "submission.judged"]
    assert len(judged) == 1


# --- verdict generations (SP12): the request's rejudgeCount comes back on the start and the verdict ---

def generation_cfg():
    return MagicMock(JUDGED_TOPIC="submission.judged", STARTED_TOPIC="submission.started", WORKER_ID="w1",
                     PUBLISH_TIMEOUT_SECONDS=10, JUDGE_INFRA_RETRY_SECONDS=300)


def sent_on(producer, topic):
    return [c.kwargs["value"] for c in producer.send.call_args_list if c.args[0] == topic]


def judged_after_start(submission_id):
    def fake_process(event, config, on_started=None):
        on_started()
        return {"submissionId": submission_id, "status": 0}
    return fake_process


@pytest.mark.parametrize("generation", [0, 3])
def test_started_and_verdict_echo_rejudge_count(generation):
    producer = MagicMock()
    with patch("worker.process_event", side_effect=judged_after_start("s40")):
        worker.handle_one({"submissionId": "s40", "rejudgeCount": generation}, generation_cfg(), producer)
    [started] = sent_on(producer, "submission.started")
    [judged] = sent_on(producer, "submission.judged")
    assert started["rejudgeCount"] == generation
    assert judged == {"submissionId": "s40", "status": 0, "rejudgeCount": generation}


def test_a_judging_failure_s_system_error_echoes_rejudge_count():
    producer = MagicMock()
    with patch("worker.process_event", side_effect=RuntimeError("judge down")):
        worker.handle_one({"submissionId": "s41", "rejudgeCount": 2}, generation_cfg(), producer)
    [judged] = sent_on(producer, "submission.judged")
    assert judged["status"] == verdict.SYSTEM_ERROR and judged["rejudgeCount"] == 2


def test_no_rejudge_count_key_when_the_request_had_none():
    # A request queued before generations existed: submission-service applies the start and verdict unguarded.
    producer = MagicMock()
    with patch("worker.process_event", side_effect=judged_after_start("s42")):
        worker.handle_one({"submissionId": "s42"}, generation_cfg(), producer)
    [started] = sent_on(producer, "submission.started")
    [judged] = sent_on(producer, "submission.judged")
    assert set(started) == {"submissionId", "worker", "startedAt"}
    assert "rejudgeCount" not in judged


def test_unpublishable_verdict_replacement_keeps_rejudge_count():
    producer = MagicMock()
    producer.send.return_value.get.side_effect = [MessageSizeTooLargeError(), None]
    with patch("worker.process_event", return_value={"submissionId": "s43", "status": 0, "details": ["x" * 10]}):
        worker.handle_one({"submissionId": "s43", "rejudgeCount": 4}, generation_cfg(), producer,
                          wait=lambda s: None)
    replacement = sent_on(producer, "submission.judged")[-1]
    assert replacement["status"] == verdict.SYSTEM_ERROR and replacement["details"] == []
    assert replacement["rejudgeCount"] == 4


# --- infrastructure retries (sub-project 11) ---

def infra_retries(reason):
    return REGISTRY.get_sample_value("oj_judge_infra_retries_total", {"reason": reason}) or 0


def infra_cfg(budget=300):
    return MagicMock(JUDGED_TOPIC="submission.judged", STARTED_TOPIC="submission.started", WORKER_ID="w1",
                     PUBLISH_TIMEOUT_SECONDS=10, JUDGE_INFRA_RETRY_SECONDS=budget)


def verdicts(producer):
    return [c.kwargs["value"] for c in producer.send.call_args_list if c.args[0] == "submission.judged"]


def test_a_minio_outage_is_retried_then_judged():
    producer, waits, before = MagicMock(), [], infra_retries("minio")
    outcomes = [MaxRetryError(None, "/b"), MaxRetryError(None, "/b"), {"submissionId": "s30", "status": 0}]
    with patch("worker.process_event", side_effect=outcomes):
        worker.handle_one({"submissionId": "s30"}, infra_cfg(), producer, wait=waits.append)
    assert waits == [2, 4]
    assert [v["status"] for v in verdicts(producer)] == [0]
    assert infra_retries("minio") - before == 2


def test_sandboxes_refusing_are_retried():
    producer, waits, before = MagicMock(), [], infra_retries("sandbox")
    with patch("worker.process_event",
               side_effect=[requests.ConnectionError("refused"), {"submissionId": "s31", "status": 0}]):
        worker.handle_one({"submissionId": "s31"}, infra_cfg(), producer, wait=waits.append)
    assert waits == [2]
    assert infra_retries("sandbox") - before == 1
    assert [v["status"] for v in verdicts(producer)] == [0]


def test_a_read_timeout_is_not_retried():
    producer, waits = MagicMock(), []
    with patch("worker.process_event", side_effect=requests.ReadTimeout("slow")):
        worker.handle_one({"submissionId": "s32"}, infra_cfg(), producer, wait=waits.append)
    assert waits == []
    assert [v["status"] for v in verdicts(producer)] == [verdict.SYSTEM_ERROR]


def test_a_certificate_failure_is_not_retried():
    producer, waits = MagicMock(), []
    with patch("worker.process_event", side_effect=requests.exceptions.SSLError("certificate verify failed")):
        worker.handle_one({"submissionId": "s36"}, infra_cfg(), producer, wait=waits.append)
    assert waits == []
    sent = verdicts(producer)
    assert [v["status"] for v in sent] == [verdict.SYSTEM_ERROR]
    assert "certificate verify failed" in sent[0]["error_message"]


def test_the_retry_budget_ends_in_a_system_error():
    producer, waits, clock = MagicMock(), [], [1000.0]

    def wait(seconds):
        waits.append(seconds)
        clock[0] += seconds

    with patch("worker.process_event", side_effect=MaxRetryError(None, "/b")), \
            patch("worker.time.monotonic", side_effect=lambda: clock[0]):
        worker.handle_one({"submissionId": "s33"}, infra_cfg(300), producer, wait=wait)
    assert waits == [2, 4, 8, 16, 32, 60, 60, 60]
    sent = verdicts(producer)
    assert len(sent) == 1
    assert sent[0]["status"] == verdict.SYSTEM_ERROR
    assert sent[0]["error_message"] == "Judge infrastructure unavailable"


def test_each_retry_announces_the_start_again():
    producer = MagicMock()
    calls = itertools.count()

    def fake_process(event, config, on_started=None):
        on_started()
        if next(calls) < 2:
            raise requests.ConnectionError("refused")
        return {"submissionId": "s34", "status": 0}

    with patch("worker.process_event", side_effect=fake_process):
        worker.handle_one({"submissionId": "s34"}, infra_cfg(), producer, wait=lambda s: None)
    started = [c for c in producer.send.call_args_list if c.args[0] == "submission.started"]
    assert len(started) == 3


def test_run_commits_only_after_the_retried_verdict():
    manager = MagicMock()
    producer, consumer = manager.producer, manager.consumer
    consumer.__iter__.return_value = [Msg({"submissionId": "s35"})]
    with patch("worker.process_event",
               side_effect=[requests.ConnectionError("refused"), {"submissionId": "s35", "status": 0}]):
        worker.run(infra_cfg(), consumer, producer)
    names = [c[0] for c in manager.mock_calls]
    assert names.index("consumer.pause") < names.index("consumer.poll") < names.index("producer.send") \
        < names.index("consumer.commit")
    assert consumer.poll.call_args.kwargs == {"timeout_ms": 2000}
    assert names.count("consumer.commit") == 1
