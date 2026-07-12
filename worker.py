import logging

import verdict
from handler import process_event

log = logging.getLogger("judge-worker")


def system_error_event(submission_id, message):
    return {
        "submissionId": submission_id,
        "status": verdict.SYSTEM_ERROR,
        "result": None,
        "cpu_time": 0,
        "real_time": 0,
        "memory": 0,
        "error_message": message,
        "details": [],
    }


def handle_one(event, config, producer):
    submission_id = event.get("submissionId")
    try:
        judged = process_event(event, config)
    except Exception as exc:  # judge-server down / timeout / bad response
        log.exception("Judging failed for submission %s", submission_id)
        judged = system_error_event(submission_id, str(exc))
    producer.send(config.JUDGED_TOPIC, key=submission_id, value=judged)
    producer.flush()


def run(config, consumer, producer):
    for msg in consumer:
        handle_one(msg.value, config, producer)
        consumer.commit()  # AFTER publish → at-least-once
