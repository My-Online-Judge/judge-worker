from unittest.mock import MagicMock, patch

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import tracing
import verdict
import worker

# A traceparent exactly as the OpenTelemetry Java agent writes it into submission-service's Kafka record.
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
PARENT_SPAN_ID = "00f067aa0ba902b7"
TRACEPARENT = f"00-{TRACE_ID}-{PARENT_SPAN_ID}-01".encode()


@pytest.fixture
def spans(monkeypatch):
    """Record spans in memory without touching the process-global tracer provider."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(tracing, "tracer", lambda: provider.get_tracer("test"))
    return exporter


def _cfg():
    return MagicMock(JUDGED_TOPIC="submission.judged", JUDGE_TIMEOUT_SECONDS=30)


def _sent_traceparent(producer):
    headers = dict(producer.send.call_args.kwargs["headers"])
    return headers["traceparent"].decode()


def test_judging_continues_the_trace_from_the_requested_record(spans):
    producer = MagicMock()
    with patch("worker.process_event", return_value={"submissionId": "s1", "status": 0}):
        worker.handle_one({"submissionId": "s1"}, _cfg(), producer,
                          headers=[("traceparent", TRACEPARENT), ("__TypeId__", b"x")])

    (span,) = spans.get_finished_spans()
    assert span.name == "judge submission"
    assert format(span.context.trace_id, "032x") == TRACE_ID
    assert format(span.parent.span_id, "016x") == PARENT_SPAN_ID
    assert span.attributes["oj.submission_id"] == "s1"
    assert span.attributes["oj.verdict_status"] == 0
    # The verdict record carries OUR span as parent, so submission-service's consumer joins this trace.
    assert _sent_traceparent(producer) == f"00-{TRACE_ID}-{format(span.context.span_id, '016x')}-01"


def test_record_without_trace_headers_starts_a_new_trace(spans):
    producer = MagicMock()
    with patch("worker.process_event", return_value={"submissionId": "s2", "status": 0}):
        worker.handle_one({"submissionId": "s2"}, _cfg(), producer, headers=None)

    (span,) = spans.get_finished_spans()
    assert span.parent is None
    assert format(span.context.trace_id, "032x") in _sent_traceparent(producer)


def test_judge_failure_still_publishes_system_error_inside_the_trace(spans):
    producer = MagicMock()
    with patch("worker.process_event", side_effect=RuntimeError("judge down")):
        worker.handle_one({"submissionId": "s3"}, _cfg(), producer,
                          headers=[("traceparent", TRACEPARENT)])

    assert producer.send.call_args.kwargs["value"]["status"] == verdict.SYSTEM_ERROR
    assert TRACE_ID in _sent_traceparent(producer)
    (span,) = spans.get_finished_spans()
    assert span.attributes["oj.verdict_status"] == verdict.SYSTEM_ERROR


def test_header_with_null_value_is_ignored():
    # Kafka allows a header with no value; it must not break context extraction.
    ctx = tracing.context_from_headers([("traceparent", None)])
    assert not trace.get_current_span(ctx).get_span_context().is_valid


def test_without_an_active_span_no_trace_header_is_produced():
    assert tracing.headers_from_current_context() == []


def test_init_tracing_is_a_no_op_without_an_endpoint(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    before = trace.get_tracer_provider()
    assert tracing.init_tracing() is False
    assert trace.get_tracer_provider() is before
