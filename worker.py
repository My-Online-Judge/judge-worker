import logging

from handler import process_event, system_error_event  # noqa: F401 (re-exported for callers/tests)

log = logging.getLogger("judge-worker")


def handle_one(event, config, producer):
    submission_id = event.get("submissionId")
    if not submission_id:
        log.error("Received event without submissionId, skipping: %r", event)
        return
    try:
        judged = process_event(event, config)
    except Exception as exc:  # judge-server down / timeout / bad response
        log.exception("Judging failed for submission %s", submission_id)
        judged = system_error_event(submission_id, str(exc))
    future = producer.send(config.JUDGED_TOPIC, key=submission_id, value=judged)
    future.get(timeout=config.JUDGE_TIMEOUT_SECONDS)  # block for delivery ack; raises on failure


def run(config, consumer, producer):
    for msg in consumer:
        handle_one(msg.value, config, producer)
        consumer.commit()  # AFTER publish → at-least-once
