"""Which judging failures are the infrastructure's, and so worth waiting out, and which are a permanent TLS
misconfiguration that fails the submission at once (sub-project 11)."""
import grpc
from minio.error import InvalidResponseError, S3Error, ServerError
import urllib3.exceptions as u3
from prometheus_client import Counter

import grpc_errors

INFRA_RETRIES = Counter("oj_judge_infra_retries", "Judgings sent again because MinIO or every sandbox was away",
                        ["reason"])
GIVE_UP_MESSAGE = "Judge infrastructure unavailable"
DELAY_START_SECONDS = 2
DELAY_MAX_SECONDS = 60
# What a submitter may read of a failure in error_message: one line, at most this long.
MAX_MESSAGE_CHARS = 500
# What marks a gRPC UNAVAILABLE as a TLS verification failure (_is_grpc_certificate_failure), matched in lower case.
# "handshake" is deliberately not one: a sandbox restart that cuts a connection reads like a handshake error.
_GRPC_TLS_WORDS = ("ssl", "certificate")


def infra_delays():
    """2, 4, 8, 16, 32, 60, 60, … seconds."""
    delay = DELAY_START_SECONDS
    while True:
        yield delay
        delay = min(delay * 2, DELAY_MAX_SECONDS)


def _is_certificate_failure(exc):
    """A certificate or verification failure does not heal by waiting (a permanent misconfiguration), so it is never
    retried: it would only burn the whole retry budget before the same SYSTEM_ERROR. This is MinIO's (urllib3's
    SSLError); gRPC's is _is_grpc_certificate_failure."""
    if isinstance(exc, u3.SSLError):
        return True
    return isinstance(exc, u3.MaxRetryError) and isinstance(exc.reason, u3.SSLError)


def _is_grpc_certificate_failure(exc):
    """The sandbox's TLS verification failing: a wrong CA or a wrong hostname (and a client certificate, when gRPC names
    it). Like MinIO's (_is_certificate_failure) it is a permanent misconfiguration, but gRPC reports it as UNAVAILABLE,
    the status of a sandbox that is down, so only the details tell the two apart: they name `ssl` or `certificate`
    (e.g. "Ssl handshake failed: ... CERTIFICATE_VERIFY_FAILED", "Peer name judge-server is not in peer certificate").
    Only UNAVAILABLE is the TLS signal: the details of INTERNAL or RESOURCE_EXHAUSTED say nothing about the handshake.
    Known limit: under TLS 1.3 a client certificate the sandbox rejects is reported after the handshake as only
    "Socket closed", the same as a restarting sandbox, so it cannot be told apart and is waited out."""
    if grpc_errors.code_of(exc) != grpc.StatusCode.UNAVAILABLE:
        return False
    details = grpc_errors.details_of(exc).lower()
    return any(word in details for word in _GRPC_TLS_WORDS)


def transient_reason(exc):
    """"sandbox" / "minio" when exc is the infrastructure being away and judging can safely start over, else None."""
    # A certificate failure is permanent, MinIO's or the sandbox's: it must be ruled out before the generic gRPC branch
    # below, which would wait out its UNAVAILABLE for the whole retry window.
    if _is_certificate_failure(exc) or _is_grpc_certificate_failure(exc):
        return None
    # dispatch_judge raises a gRPC error only after every sandbox failed it with a retryable code (down or full);
    # DEADLINE_EXCEEDED is not one (the sandbox may still be judging, so a retry could judge twice), nor is any other.
    # Every other UNAVAILABLE (refused, socket closed, connection reset, DNS), INTERNAL and RESOURCE_EXHAUSTED is
    # waited out: a restarting sandbox cuts connections, which is not a certificate failure.
    if grpc_errors.code_of(exc) in grpc_errors.RETRYABLE_CODES:
        return "sandbox"
    if isinstance(exc, u3.HTTPError):  # minio's transport
        return "minio"
    if isinstance(exc, ServerError):
        return "minio" if exc.status_code >= 500 else None
    if isinstance(exc, S3Error):  # MinIO answered with an error document: its own 5xx (SlowDown, InternalError) is a busy
        status = getattr(getattr(exc, "response", None), "status", None)  # store; a 4xx (NoSuchKey) will not heal
        return "minio" if status is not None and status >= 500 else None
    if isinstance(exc, InvalidResponseError):
        code = getattr(exc, "_code", None)  # minio 7.2.7 keeps the response code private
        return "minio" if code is not None and code >= 500 else None
    return None


def _one_line(text):
    """text on one line (every run of whitespace, newlines included, is a single space), at most MAX_MESSAGE_CHARS."""
    return " ".join(text.split())[:MAX_MESSAGE_CHARS].rstrip()


def describe(exc):
    """A failure as the one line the submitter reads in error_message. str() of a grpc.RpcError is a multi-line debug
    dump (peer addresses, internal file names): a gRPC failure is `CODE_NAME: details` instead. The log keeps the whole
    exception."""
    code = grpc_errors.code_of(exc)
    if code is None:
        return _one_line(str(exc))
    details = grpc_errors.details_of(exc)
    return _one_line(f"{code.name}: {details}" if details.strip() else code.name)
