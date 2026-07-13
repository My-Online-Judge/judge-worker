from unittest.mock import MagicMock, patch

import pytest

import verdict
import worker


class Msg:
    def __init__(self, value):
        self.value = value


def test_system_error_event_shape():
    e = worker.system_error_event("s1", "boom")
    assert e["submissionId"] == "s1"
    assert e["status"] == verdict.SYSTEM_ERROR
    assert e["error_message"] == "boom"
    assert e["details"] == []


def test_handle_one_publishes_result():
    producer = MagicMock()
    cfg = MagicMock(JUDGED_TOPIC="submission.judged", JUDGE_TIMEOUT_SECONDS=30)
    event = {"submissionId": "s2"}
    with patch("worker.process_event", return_value={"submissionId": "s2", "status": 0}):
        worker.handle_one(event, cfg, producer)
    producer.send.assert_called_once_with("submission.judged", key="s2",
                                           value={"submissionId": "s2", "status": 0})
    producer.send.return_value.get.assert_called_once_with(timeout=30)


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
