import json
import logging
import time
from datetime import datetime, timezone

from kafka.errors import BrokerResponseError, KafkaError, KafkaTimeoutError
from opentelemetry.trace import SpanKind
from prometheus_client import Counter

import infra_errors
import tracing
from shutdown import Shutdown
from handler import job_identity, process_event, system_error_event  # noqa: F401 (re-exported for callers/tests)

log = logging.getLogger("judge-worker")

# A run (Run code design §8) names the instant after which nobody waits for it; one picked up later is skipped, so a
# backlog left by a run-lane outage is cleared at once instead of being judged into the void.
RUN_EXPIRED = Counter("oj_run_expired", "Runs skipped because their caller had stopped waiting")
# Records the worker skipped and committed past instead of exiting on: JSON that is not an object (non_object), or
# anything handle_one raised on (exception). One of these is a producer bug or a worker bug, never a verdict.
POISON_PILL = Counter("oj_poison_pill", "Records skipped because the worker could not handle them", ["reason"])

# Sub-project 7 §3.2: a verdict Kafka did not take is sent again until it does — never dropped, never judged again,
# and never a reason to exit (an exit cost a Docker restart back-off and a consumer-group rebalance).
PUBLISH_RETRIES = Counter("oj_verdict_publish_retries", "Verdict publishes that failed and were sent again")
# How long verdicts waited for Kafka (follow-up 16): in a full outage the first wait lasts until Kafka is back, so the
# retry count reads 1-2 per outage while this counts its length.
PUBLISH_WAIT_SECONDS = Counter("oj_verdict_publish_wait_seconds", "Seconds spent waiting to send a verdict again")
BACKOFF_START_SECONDS = 1
BACKOFF_MAX_SECONDS = 10
# The submission's generation as submission-service queued it (0 for a submit, n after the n-th rejudge). Echoed
# unchanged on submission.started and submission.judged, so a start or verdict of an earlier judging is not applied to
# a rejudged submission (SP12).
GENERATION_KEY = "rejudgeCount"


# Which submission-service instance asked for a run (its consumer group is per instance, so only that one may complete
# it). Echoed unchanged on run.judged, like the generation; a submission carries none.
REPLY_TO_KEY = "replyTo"


def backoff_delays():
    """1, 2, 4, 8, 10, 10, … seconds."""
    delay = BACKOFF_START_SECONDS
    while True:
        yield delay
        delay = min(delay * 2, BACKOFF_MAX_SECONDS)


def connect(build, config, sleep=time.sleep, stop=lambda: False):
    """build(config), retried while no broker answers — Kafka still starting, or away when Docker restarted us —
    instead of exiting into Docker's restart back-off (sub-project 7 §3.2). stop() true (SIGTERM during the wait)
    exits cleanly: nothing is in hand yet."""
    for delay in backoff_delays():
        try:
            return build(config)
        except KafkaTimeoutError:
            # kafka-python 3 gives up bootstrapping with "Unable to bootstrap" (2.x raised NoBrokersAvailable).
            log.warning("No Kafka broker answers; trying again in %d s", delay)
            sleep(delay)
            if stop():
                log.warning("Shutdown requested while waiting for Kafka: exiting")
                raise SystemExit(0)


def reply_to_of(message):
    """The message's replyTo, as a dict to merge into what is published about it; empty when it has none."""
    return {REPLY_TO_KEY: message[REPLY_TO_KEY]} if REPLY_TO_KEY in message else {}


def generation_of(message):
    """The message's rejudgeCount, as a dict to merge into what is published about it. Empty when the message has
    none (a request queued before generations existed): then nothing is added and submission-service applies the start
    and the verdict as before."""
    return {GENERATION_KEY: message[GENERATION_KEY]} if GENERATION_KEY in message else {}


def safe_json_deserialize(b):
    if b is None:
        return None
    try:
        return json.loads(b.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None


def publish_until_sent(producer, config, submission_id, judged, headers, wait, id_key="submissionId"):
    """Send the verdict until Kafka acknowledges it. A KafkaError — Kafka away, no metadata, no acknowledgement in
    time — is logged, counted, and the same verdict sent again after wait(seconds). A record the broker refuses for
    good (a non-retriable broker answer, e.g. MessageSizeTooLargeError: the verdict carries the program's whole
    output) is replaced once by a SYSTEM_ERROR without details but with the verdict's rejudgeCount, as when judging
    itself fails, named as the request was (id_key); if that is refused too, it propagates. Anything else is a bug:
    it propagates."""
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
                log.error("Kafka refused the verdict of %s for good (%r); sending a SYSTEM_ERROR instead",
                          submission_id, exc)
                replacement = system_error_event(submission_id, f"verdict not publishable: {exc!r}", id_key=id_key)
                judged, replaced = {**replacement, **generation_of(judged), **reply_to_of(judged)}, True
                continue
            log.warning("Verdict of submission %s not published (%r); sending it again in %d s",
                        submission_id, exc, delay)
            started = time.monotonic()
            wait(delay)
            PUBLISH_WAIT_SECONDS.inc(time.monotonic() - started)


def started_at_now():
    """The current UTC instant, ISO-8601 with milliseconds and a Z suffix (submission-service localizes it)."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def publish_started(producer, config, submission_id, generation=None):
    """Tell submission-service judging begins now, with the request's generation (generation_of) when it had one.
    Advisory: not awaited, and a KafkaError is only logged."""
    try:
        producer.send(config.STARTED_TOPIC, key=submission_id,
                      value={"submissionId": submission_id, "worker": config.WORKER_ID,
                             "startedAt": started_at_now(), **(generation or {})})
    except KafkaError as exc:
        log.warning("Start of submission %s not announced (%r)", submission_id, exc)


def judge_with_infra_retry(event, config, on_started, wait):
    """process_event, started again while MinIO or every sandbox is away (infra_errors.transient_reason), after
    2 s, 4 s … 60 s, until JUDGE_INFRA_RETRY_SECONDS would be exceeded: then a SYSTEM_ERROR. wait is run()'s
    pause/poll/seek, so nothing is committed meanwhile and the group keeps the partition. Any other failure is a
    SYSTEM_ERROR at once."""
    id_key, submission_id = job_identity(event)
    began = time.monotonic()
    delays = infra_errors.infra_delays()
    while True:
        try:
            return process_event(event, config, on_started=on_started)
        except Exception as exc:  # judge-server down / timeout / bad response
            reason = infra_errors.transient_reason(exc)
            if reason is None:
                log.exception("Judging failed for %s", submission_id)
                return system_error_event(submission_id, str(exc), id_key=id_key)
            delay = next(delays)
            if time.monotonic() - began + delay > config.JUDGE_INFRA_RETRY_SECONDS:
                log.error("Judge infrastructure (%s) still unavailable for %s after %.0f s (%r); giving up",
                          reason, submission_id, time.monotonic() - began, exc)
                return system_error_event(submission_id, infra_errors.GIVE_UP_MESSAGE, id_key=id_key)
            infra_errors.INFRA_RETRIES.labels(reason).inc()
            log.warning("Judge infrastructure (%s) unavailable for submission %s (%r); judging again in %d s",
                        reason, submission_id, exc, delay)
            wait(delay)


def is_expired(event, now_ms):
    """True for a run whose expiresAt (epoch ms) has passed; a record without one (a submission), or with one that is
    not a number, never expires."""
    expires_at = event.get("expiresAt")
    return isinstance(expires_at, (int, float)) and not isinstance(expires_at, bool) and now_ms > expires_at


def handle_one(event, config, producer, headers=None, wait=time.sleep, now_ms=None):
    if event is None:
        log.error("Skipping malformed (non-JSON) message")
        return
    identity = job_identity(event)
    if identity is None:
        if isinstance(event, dict):
            log.error("Received event without submissionId or runId, skipping: %r", event)
        else:
            log.error("Skipping a record whose JSON is not an object (%s)", type(event).__name__)
            POISON_PILL.labels("non_object").inc()
        return
    id_key, submission_id = identity
    if is_expired(event, time.time() * 1000 if now_ms is None else now_ms):
        log.info("Skipping run %s: its caller stopped waiting at %s", submission_id, event.get("expiresAt"))
        RUN_EXPIRED.inc()
        return
    # Continue the trace submission-service started (its Kafka record carries `traceparent`), so one
    # submission (or run) is a single trace from the HTTP request to the stored verdict (or the reply).
    with tracing.tracer().start_as_current_span(
            "judge submission" if id_key == "submissionId" else "judge run",
            context=tracing.context_from_headers(headers),
            kind=SpanKind.CONSUMER,
            attributes={"oj.submission_id" if id_key == "submissionId" else "oj.run_id": submission_id}) as span:
        generation = generation_of(event)
        judged = judge_with_infra_retry(
            event, config, lambda: publish_started(producer, config, submission_id, generation), wait)
        # every verdict, a SYSTEM_ERROR included, names the request's generation and who asked
        judged = {**judged, **generation, **reply_to_of(event)}
        span.set_attribute("oj.verdict_status", judged.get("status"))
        publish_until_sent(producer, config, submission_id, judged,
                           tracing.headers_from_current_context(), wait, id_key=id_key)


def run(config, consumer, producer, shutdown=None):
    """One pass over the consumer: judge, publish and commit each record in turn. Returns when the consumer's iteration
    ends (consumer_timeout_ms with no record) or, after the record in hand is committed, when shutdown was requested."""
    shutdown = shutdown or Shutdown()

    def wait(seconds):
        # Kafka is away: poll() keeps this consumer in its group (it is how the group sees we are alive) while every
        # partition is paused, so no next submission is fetched. kafka-python 2.0.2 may block in poll() until the
        # coordinator is back (in a full outage the first wait lasts the whole outage), or raise; either way the
        # verdict is sent again afterwards. A rebalance completing inside poll() un-pauses every partition and the poll
        # may then fetch a record and advance its position: rewind it, so the next commit does not skip it.
        # Polled in 1 s slices so a SIGTERM ends the wait within a second; the verdict is still sent before exiting.
        consumer.pause(*consumer.assignment())
        remaining = seconds
        while remaining > 0 and not shutdown.requested:
            piece = min(1, remaining)
            try:
                for tp, records in consumer.poll(timeout_ms=int(piece * 1000)).items():
                    consumer.seek(tp, records[0].offset)
            except KafkaError as exc:
                log.warning("poll() while waiting for Kafka failed (%r)", exc)
                time.sleep(piece)
            remaining -= piece

    for msg in consumer:
        try:
            handle_one(msg.value, config, producer, headers=msg.headers, wait=wait)
        except Exception:
            # A record this code cannot handle (a bug, or a shape nobody foresaw) must not take the worker down: an
            # exit here re-reads the same offset after the restart and crash-loops on it. Log, count, commit past it.
            log.exception("Poison pill at %s/%s offset %s: skipping it", msg.topic, msg.partition, msg.offset)
            POISON_PILL.labels("exception").inc()
        consumer.resume(*consumer.paused())
        try:
            consumer.commit()  # AFTER publish → at-least-once
        except KafkaError as exc:
            # The group moved on while Kafka was away: the message comes again, is judged again, and submission-service
            # ignores the second verdict (the first terminal one wins).
            log.warning("Offset commit failed (%r); the message will be delivered and judged again", exc)
        if shutdown.requested:
            return


def serve(config, consumer, producer, shutdown):
    """run() until shutdown is requested, then close the consumer (leaving the group at once, so its partitions are
    not stalled for the session timeout). The consumer is built with consumer_timeout_ms, so an idle pass ends and
    this loop looks at the flag again."""
    try:
        while not shutdown.requested:
            run(config, consumer, producer, shutdown)
    finally:
        consumer.close()
