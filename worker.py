import json
import logging

from opentelemetry.trace import SpanKind

import tracing
from handler import process_event, system_error_event  # noqa: F401 (re-exported for callers/tests)

log = logging.getLogger("judge-worker")


def safe_json_deserialize(b):
    if b is None:
        return None
    try:
        return json.loads(b.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def handle_one(event, config, producer, headers=None):
    if event is None:
        log.error("Skipping malformed (non-JSON) message")
        return
    submission_id = event.get("submissionId")
    if not submission_id:
        log.error("Received event without submissionId, skipping: %r", event)
        return
    # Continue the trace judge-api started (its Kafka record carries `traceparent`), so one
    # submission is a single trace from HTTP submit to stored verdict.
    with tracing.tracer().start_as_current_span(
            "judge submission",
            context=tracing.context_from_headers(headers),
            kind=SpanKind.CONSUMER,
            attributes={"oj.submission_id": submission_id}) as span:
        try:
            judged = process_event(event, config)
        except Exception as exc:  # judge-server down / timeout / bad response
            log.exception("Judging failed for submission %s", submission_id)
            judged = system_error_event(submission_id, str(exc))
        span.set_attribute("oj.verdict_status", judged.get("status"))
        future = producer.send(config.JUDGED_TOPIC, key=submission_id, value=judged,
                               headers=tracing.headers_from_current_context())
        future.get(timeout=config.JUDGE_TIMEOUT_SECONDS)  # block for delivery ack; raises on failure


def run(config, consumer, producer):
    for msg in consumer:
        handle_one(msg.value, config, producer, headers=msg.headers)
        consumer.commit()  # AFTER publish → at-least-once
