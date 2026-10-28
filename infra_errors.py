"""Which judging failures are the infrastructure's, and so worth waiting out (sub-project 11)."""
from minio.error import InvalidResponseError, S3Error, ServerError
import requests
import urllib3.exceptions as u3
from prometheus_client import Counter

INFRA_RETRIES = Counter("oj_judge_infra_retries", "Judgings sent again because MinIO or every sandbox was away",
                        ["reason"])
GIVE_UP_MESSAGE = "Judge infrastructure unavailable"
DELAY_START_SECONDS = 2
DELAY_MAX_SECONDS = 60


def infra_delays():
    """2, 4, 8, 16, 32, 60, 60, … seconds."""
    delay = DELAY_START_SECONDS
    while True:
        yield delay
        delay = min(delay * 2, DELAY_MAX_SECONDS)


def _is_certificate_failure(exc):
    """A certificate or verification failure does not heal by waiting (a permanent misconfiguration), so it is never
    retried: it would only burn the whole retry budget before the same SYSTEM_ERROR."""
    if isinstance(exc, (requests.exceptions.SSLError, u3.SSLError)):
        return True
    return isinstance(exc, u3.MaxRetryError) and isinstance(exc.reason, u3.SSLError)


def transient_reason(exc):
    """"sandbox" / "minio" when exc is the infrastructure being away and judging can safely start over, else None."""
    if _is_certificate_failure(exc):
        return None
    # A read timeout: the sandbox may still be judging, so a retry could judge twice.
    if isinstance(exc, requests.ReadTimeout):
        return None
    # dispatch_judge raises these only after every sandbox failed (a 4xx is raised at once and is not transient).
    if isinstance(exc, requests.ConnectionError):
        return "sandbox"
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, "status_code", None)
        return "sandbox" if status is not None and status >= 500 else None
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
