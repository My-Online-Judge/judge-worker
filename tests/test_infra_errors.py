import itertools

import pytest
import requests
import urllib3
from minio.error import InvalidResponseError, S3Error, ServerError
from urllib3.exceptions import MaxRetryError, SSLError as Urllib3SSLError

import infra_errors


def http_error(status):
    response = requests.Response()
    response.status_code = status
    return requests.HTTPError(response=response)


def s3_error():
    return S3Error("NoSuchKey", "missing", "r", "req", "host", None)


@pytest.mark.parametrize("exc, reason", [
    (requests.ReadTimeout(), None),                      # the sandbox may still be judging: a retry could judge twice
    (requests.ConnectionError(), "sandbox"),
    (requests.ConnectTimeout(), "sandbox"),
    (http_error(503), "sandbox"),
    (http_error(400), None),
    (requests.exceptions.SSLError(), None),              # a certificate failure is permanent: do not burn the budget
    (MaxRetryError(None, "/b/o"), "minio"),
    (MaxRetryError(None, "/b/o", Urllib3SSLError("bad cert")), None),
    (Urllib3SSLError("bad cert"), None),
    (ServerError("x", 503), "minio"),
    (ServerError("x", 404), None),
    (InvalidResponseError(502, "text/html", "body"), "minio"),
    (InvalidResponseError(200, "text/html", "body"), None),
    (s3_error(), None),
    (ValueError("x"), None),
])
def test_transient_reason(exc, reason):
    assert infra_errors.transient_reason(exc) == reason


def test_delays_double_to_sixty():
    assert list(itertools.islice(infra_errors.infra_delays(), 8)) == [2, 4, 8, 16, 32, 60, 60, 60]
