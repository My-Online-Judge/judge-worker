"""Distributed tracing for judge-worker (OpenTelemetry).

The Java services are traced by the OpenTelemetry Java agent, which writes a W3C `traceparent`
header into every Kafka record they produce. The worker continues that trace: it reads the header
from `submission.requested`, judges inside a child span, and writes its own context into the
`submission.judged` record so submission-service's verdict consumer joins the same trace.
"""
import os

from opentelemetry import trace
from opentelemetry.propagate import extract, inject

_TRACER_NAME = "judge-worker"


def init_tracing(service_name="judge-worker"):
    """Export spans over OTLP/HTTP when OTEL_EXPORTER_OTLP_ENDPOINT is set; otherwise do nothing.

    Returns True when tracing was enabled. Export is batched on a background thread, so a slow or
    missing collector never blocks judging — spans that cannot be sent are dropped.
    """
    if not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return False
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    RequestsInstrumentor().instrument()  # child spans for the HTTP calls to judge-server
    return True


def tracer():
    return trace.get_tracer(_TRACER_NAME)


def context_from_headers(headers):
    """Parent context carried by a consumed Kafka record (kafka-python: list of (str, bytes))."""
    carrier = {}
    for key, value in headers or []:
        if value is not None:
            carrier[key] = value.decode("utf-8", errors="replace")
    return extract(carrier)


def headers_from_current_context():
    """Kafka headers carrying the current span context, for a record about to be produced."""
    carrier = {}
    inject(carrier)
    return [(key, value.encode("utf-8")) for key, value in carrier.items()]
