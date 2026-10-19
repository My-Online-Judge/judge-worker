import json
import logging
import time
from datetime import datetime, timezone

from kafka.errors import BrokerResponseError, KafkaError, NoBrokersAvailable
from opentelemetry.trace import SpanKind
from prometheus_client import Counter

import tracing
from handler import process_event, system_error_event  # noqa: F401 (re-exported for callers/tests)

log = logging.getLogger("judge-worker")

# Sub-project 7 §3.2: a verdict Kafka did not take is sent again until it does — never dropped, never judged again,
# and never a reason to exit (an exit cost a Docker restart back-off and a consumer-group rebalance).
PUBLISH_RETRIES = Counter("oj_verdict_publish_retries", "Verdict publishes that failed and were sent again")
# How long verdicts waited for Kafka (follow-up 16): in a full outage the first wait lasts until Kafka is back, so the
# retry count reads 1-2 per outage while this counts its length.
PUBLISH_WAIT_SECONDS = Counter("oj_verdict_publish_wait_seconds", "Seconds spent waiting to send a verdict again")
BACKOFF_START_SECONDS = 1
BACKOFF_MAX_SECONDS = 10


def backoff_delays():
    """1, 2, 4, 8, 10, 10, … seconds."""
    delay = BACKOFF_START_SECONDS
    while True:
        yield delay
        delay = min(delay * 2, BACKOFF_MAX_SECONDS)


def connect(build, config, sleep=time.sleep):
    """build(config), retried while no broker answers — Kafka still starting, or away when Docker restarted us —
    instead of exiting into Docker's restart back-off (sub-project 7 §3.2)."""
    for delay in backoff_delays():
        try:
            return build(config)
        except NoBrokersAvailable:
            log.warning("No Kafka broker answers; trying again in %d s", delay)
            sleep(delay)


def safe_json_deserialize(b):
    if b is None:
        return None
    try:
        return json.loads(b.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def publish_until_sent(producer, config, submission_id, judged, headers, wait):
    """Send the verdict until Kafka acknowledges it. A KafkaError — Kafka away, no metadata, no acknowledgement in
    time — is logged, counted, and the same verdict sent again after wait(seconds). A record the broker refuses for
    good (a non-retriable broker answer, e.g. MessageSizeTooLargeError: the verdict carries the program's whole
    output) is replaced once by a SYSTEM_ERROR without details, as when judging itself fails; if that is refused too,
    it propagates. Anything else is a bug: it propagates."""
    replaced = False
    for delay in backoff_delays():
        try:
            producer.send(config.JUDGED_TOPIC, key=submission_id, value=judged, headers=headers) \
                .get(timeout=config.PUBLISH_TIMEOUT_SECONDS)
            return
        except KafkaError as exc:
            PUBLISH_RETRIES.inc()
            if isinstance(exc, BrokerResponseError) and not exc.retriable:
                if replaced:
                    raise
                log.error("Kafka refused the verdict of submission %s for good (%r); sending a SYSTEM_ERROR instead",
                          submission_id, exc)
                judged, replaced = system_error_event(submission_id, f"verdict not publishable: {exc!r}"), True
                continue
            log.warning("Verdict of submission %s not published (%r); sending it again in %d s",
                        submission_id, exc, delay)
            started = time.monotonic()
            wait(delay)
            PUBLISH_WAIT_SECONDS.inc(time.monotonic() - started)


def started_at_now():
    """The current UTC instant, ISO-8601 with milliseconds and a Z suffix (submission-service localizes it)."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def publish_started(producer, config, submission_id):
    """Tell submission-service judging begins now. Advisory: not awaited, and a KafkaError is only logged."""
    try:
        producer.send(config.STARTED_TOPIC, key=submission_id,
                      value={"submissionId": submission_id, "worker": config.WORKER_ID,
                             "startedAt": started_at_now()})
    except KafkaError as exc:
        log.warning("Start of submission %s not announced (%r)", submission_id, exc)


def handle_one(event, config, producer, headers=None, wait=time.sleep):
    if event is None:
        log.error("Skipping malformed (non-JSON) message")
        return
    submission_id = event.get("submissionId")
    if not submission_id:
        log.error("Received event without submissionId, skipping: %r", event)
        return
    # Continue the trace submission-service started (its Kafka record carries `traceparent`), so one
    # submission is a single trace from HTTP submit to stored verdict.
    with tracing.tracer().start_as_current_span(
            "judge submission",
            context=tracing.context_from_headers(headers),
            kind=SpanKind.CONSUMER,
            attributes={"oj.submission_id": submission_id}) as span:
        try:
            judged = process_event(
                event, config, on_started=lambda: publish_started(producer, config, submission_id))
        except Exception as exc:  # judge-server down / timeout / bad response
            log.exception("Judging failed for submission %s", submission_id)
            judged = system_error_event(submission_id, str(exc))
        span.set_attribute("oj.verdict_status", judged.get("status"))
        publish_until_sent(producer, config, submission_id, judged,
                           tracing.headers_from_current_context(), wait)


def run(config, consumer, producer):
    def wait(seconds):
        # Kafka is away: poll() keeps this consumer in its group (it is how the group sees we are alive) while every
        # partition is paused, so no next submission is fetched. kafka-python 2.0.2 may block in poll() until the
        # coordinator is back (in a full outage the first wait lasts the whole outage), or raise; either way the
        # verdict is sent again afterwards. A rebalance completing inside poll() un-pauses every partition and the poll
        # may then fetch a record and advance its position: rewind it, so the next commit does not skip it.
        consumer.pause(*consumer.assignment())
        try:
            for tp, records in consumer.poll(timeout_ms=int(seconds * 1000)).items():
                consumer.seek(tp, records[0].offset)
        except KafkaError as exc:
            log.warning("poll() while waiting for Kafka failed (%r)", exc)
            time.sleep(seconds)

    for msg in consumer:
        handle_one(msg.value, config, producer, headers=msg.headers, wait=wait)
        consumer.resume(*consumer.paused())
        try:
            consumer.commit()  # AFTER publish → at-least-once
        except KafkaError as exc:
            # The group moved on while Kafka was away: the message comes again, is judged again, and submission-service
            # ignores the second verdict (the first terminal one wins).
            log.warning("Offset commit failed (%r); the message will be delivered and judged again", exc)
