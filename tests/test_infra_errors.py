import itertools

import grpc
import pytest
from minio.error import InvalidResponseError, S3Error, ServerError
from urllib3.exceptions import MaxRetryError, SSLError as Urllib3SSLError

import infra_errors
from tests.rpc_errors import rpc_error

CODE = grpc.StatusCode


def s3_error():
    return S3Error("NoSuchKey", "missing", "r", "req", "host", None)


def s3_error_with_status(code, status):
    # keyword arguments: minio-py puts `response` first, and the names are stable across versions
    return S3Error(response=type("R", (), {"status": status})(), code=code, message="msg", resource="r",
                   request_id="req", host_id="host")


@pytest.mark.parametrize("exc, reason", [
    (MaxRetryError(None, "/b/o"), "minio"),
    (MaxRetryError(None, "/b/o", Urllib3SSLError("bad cert")), None),
    (Urllib3SSLError("bad cert"), None),
    (ServerError("x", 503), "minio"),
    (ServerError("x", 404), None),
    (InvalidResponseError(502, "text/html", "body"), "minio"),
    (InvalidResponseError(200, "text/html", "body"), None),
    (s3_error(), None),
    (s3_error_with_status("SlowDown", 503), "minio"),      # MinIO answering "slow down" is the store being busy
    (s3_error_with_status("InternalError", 500), "minio"),
    (s3_error_with_status("NoSuchKey", 404), None),         # a bundle that is not there does not heal by waiting
    (ValueError("x"), None),
])
def test_transient_reason(exc, reason):
    assert infra_errors.transient_reason(exc) == reason


@pytest.mark.parametrize("code", [CODE.UNAVAILABLE, CODE.INTERNAL, CODE.RESOURCE_EXHAUSTED])
def test_a_sandbox_that_could_not_take_the_request_is_transient(code):
    # dispatch_judge raises these only after every sandbox failed them: down (UNAVAILABLE, INTERNAL) or full.
    assert infra_errors.transient_reason(rpc_error(code, "no sandbox")) == "sandbox"


def test_grpc_unavailable_is_sandbox():
    exc = rpc_error(CODE.UNAVAILABLE, "failed to connect to all addresses; last error: Connection refused")
    assert infra_errors.transient_reason(exc) == "sandbox"


def test_grpc_busy_is_sandbox():
    assert infra_errors.transient_reason(rpc_error(CODE.RESOURCE_EXHAUSTED, "queue full")) == "sandbox"


def test_grpc_deadline_is_not_transient():
    # the sandbox has the request and may still be judging: a retry could judge twice
    assert infra_errors.transient_reason(rpc_error(CODE.DEADLINE_EXCEEDED, "Deadline Exceeded")) is None


def test_grpc_invalid_argument_is_not_transient():
    assert infra_errors.transient_reason(rpc_error(CODE.INVALID_ARGUMENT, "unknown seccomp rule")) is None


@pytest.mark.parametrize("code", [CODE.NOT_FOUND, CODE.UNAUTHENTICATED, CODE.PERMISSION_DENIED, CODE.CANCELLED,
                                  CODE.UNKNOWN, CODE.OK])
def test_every_other_grpc_code_is_not_transient(code):
    assert infra_errors.transient_reason(rpc_error(code, "x")) is None


def test_a_grpc_error_without_a_code_is_not_transient():
    assert infra_errors.transient_reason(grpc.RpcError()) is None


@pytest.mark.parametrize("details", [
    "failed to connect to all addresses; last error: UNKNOWN: ipv4:10.0.0.2:8080: Ssl handshake failed: "
    "SSL_ERROR_SSL: error:0A000086:SSL routines::certificate verify failed",
    "CERTIFICATE_VERIFY_FAILED",
    "Handshake failed with fatal error",
    "ssl error",
])
def test_grpc_tls_failure_is_transient(details):
    # A sandbox restart that cuts a TLS handshake reads like a bad certificate: UNAVAILABLE is always waited out,
    # and the retry budget costs less than a wrong SYSTEM_ERROR.
    exc = rpc_error(CODE.UNAVAILABLE, details)
    assert infra_errors._is_certificate_failure(exc) is False
    assert infra_errors.transient_reason(exc) == "sandbox"


@pytest.mark.parametrize("exc", [
    rpc_error(CODE.UNAVAILABLE, "failed to connect to all addresses; last error: Connection refused"),
    rpc_error(CODE.UNAVAILABLE, ""),
    rpc_error(CODE.UNAVAILABLE, "Ssl handshake failed: certificate verify failed"),   # TLS words change nothing
    rpc_error(CODE.UNAVAILABLE, "SSL_ERROR_SSL: CERTIFICATE_VERIFY_FAILED"),
    rpc_error(CODE.INTERNAL, "certificate"),
    rpc_error(CODE.RESOURCE_EXHAUSTED, "ssl"),
    grpc.RpcError(),
    ValueError("ssl handshake"),
])
def test_a_grpc_failure_is_never_a_certificate_failure(exc):
    # only MinIO's urllib3 SSLError is a permanent certificate failure (test_transient_reason)
    assert infra_errors._is_certificate_failure(exc) is False


def test_a_requests_error_is_no_longer_the_judge_path():
    # the sandbox is called over gRPC now: only the heartbeat uses requests, and it is not retried through here
    import requests
    for exc in (requests.ConnectionError(), requests.ConnectTimeout(), requests.exceptions.SSLError()):
        assert infra_errors.transient_reason(exc) is None


# --- describe: what the submitter sees in error_message ---

def test_describe_a_grpc_error_is_one_line_with_the_code_name():
    exc = rpc_error(CODE.INVALID_ARGUMENT, "  unknown seccomp rule\n\tdebug_error_string:\n{ \"file\": 1 }\n")
    message = infra_errors.describe(exc)
    assert message == 'INVALID_ARGUMENT: unknown seccomp rule debug_error_string: { "file": 1 }'
    assert "\n" not in message


def test_describe_caps_a_long_message():
    message = infra_errors.describe(rpc_error(CODE.INTERNAL, "x " * 1000))
    assert infra_errors.MAX_MESSAGE_CHARS == 500
    # the code name counts, and the cut leaves no trailing blank
    assert message.startswith("INTERNAL: x x") and 499 <= len(message) <= 500


def test_describe_a_grpc_error_without_details_is_just_the_code_name():
    assert infra_errors.describe(rpc_error(CODE.UNAVAILABLE, "")) == "UNAVAILABLE"
    assert infra_errors.describe(rpc_error(CODE.UNAVAILABLE, None)) == "UNAVAILABLE"


def test_describe_survives_details_that_raise():
    class Broken(grpc.RpcError):
        def code(self):
            return CODE.UNAVAILABLE

        def details(self):
            raise RuntimeError("no details")

    assert infra_errors.describe(Broken()) == "UNAVAILABLE"


def test_describe_a_grpc_error_without_a_code_falls_back_to_its_text():
    assert infra_errors.describe(grpc.RpcError("odd\nfailure")) == "odd failure"


def test_describe_anything_else_is_its_text_on_one_line_and_capped():
    assert infra_errors.describe(RuntimeError("judge down")) == "judge down"
    assert infra_errors.describe(RuntimeError("line 1\nline 2\r\n  line 3")) == "line 1 line 2 line 3"
    assert len(infra_errors.describe(RuntimeError("y" * 2000))) == 500


def test_delays_double_to_sixty():
    assert list(itertools.islice(infra_errors.infra_delays(), 8)) == [2, 4, 8, 16, 32, 60, 60, 60]
