import itertools
from unittest.mock import ANY, MagicMock, patch

import pytest
from kafka.errors import (CommitFailedError, KafkaError, KafkaTimeoutError, MessageSizeTooLargeError,
                          NoBrokersAvailable)
from kafka.structs import TopicPartition
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
    with patch("worker.process_event", side_effect=lambda e, c: {"submissionId": e["submissionId"], "status": 0}):
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
            patch("worker.time.monotonic", side_effect=[100.0, 107.5]):
        worker.handle_one({"submissionId": "s15"}, cfg, producer, wait=lambda s: None)
    assert (REGISTRY.get_sample_value("oj_verdict_publish_wait_seconds_total") or 0) - before == 7.5
