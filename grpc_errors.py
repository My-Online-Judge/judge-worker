"""What the worker reads off a failed gRPC call, in one place: dispatcher (fail over) and infra_errors (wait out)."""
import grpc

# Status codes of Judge that mean "this sandbox could not take the request": not up, TLS handshake failed, judger
# failure before any case ran (UNAVAILABLE, INTERNAL) or running_jobs == MAX_JOBS with the wait queue full
# (RESOURCE_EXHAUSTED). dispatcher fails over to the next sandbox on these, and infra_errors treats the one it raises
# after the last sandbox failed as the infrastructure being away. Every other code is not: INVALID_ARGUMENT,
# NOT_FOUND, UNAUTHENTICATED and PERMISSION_DENIED are deterministic (the same request fails everywhere), and
# DEADLINE_EXCEEDED means the sandbox has the request and is still judging (it cancels the job when the call ends), so
# a second sandbox, or a second judging, would judge the same submission again, doubling the CPU exactly when the
# sandboxes are busiest.
RETRYABLE_CODES = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INTERNAL,
                             grpc.StatusCode.RESOURCE_EXHAUSTED})


def code_of(exc):
    """The gRPC status code of a failed call, or None for anything that is not one (or whose code() raises or does not
    return a grpc.StatusCode)."""
    code = getattr(exc, "code", None) if isinstance(exc, grpc.RpcError) else None
    try:
        value = code() if callable(code) else None
    except Exception:  # a failed call is being reported: a broken code() is not worth a second failure
        value = None
    return value if isinstance(value, grpc.StatusCode) else None


def details_of(exc):
    """The status details of a failed call as text: "" for anything that has none, or whose details() raises."""
    details = getattr(exc, "details", None) if isinstance(exc, grpc.RpcError) else None
    try:
        text = details() if callable(details) else None
    except Exception:  # a failed call is being reported: its details are never worth a second failure
        text = None
    return text if isinstance(text, str) else ""
