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


# What gRPC (grpcio 1.68) really says when the TLS handshake with the sandbox fails verification: the failure is a
# status UNAVAILABLE like a sandbox that is down, and only the details tell the two apart.
WRONG_CA = ("failed to connect to all addresses; last error: UNKNOWN: ipv4:10.0.0.5:8080: Ssl handshake failed: "
            "SSL_ERROR_SSL: error:1000007d:SSL routines:OPENSSL_internal:CERTIFICATE_VERIFY_FAILED")
HOSTNAME_MISMATCH = ("failed to connect to all addresses; last error: UNKNOWN: ipv4:10.0.0.5:8080: "
                     "Peer name judge-server is not in peer certificate")


@pytest.mark.parametrize("details", [
    WRONG_CA,                                                                        # a wrong CA
    HOSTNAME_MISMATCH,                                                               # a wrong hostname
    WRONG_CA.replace("failed:", "failed (TSI_PROTOCOL_FAILURE):"),                   # as grpcio 1.68 writes it
    "failed to connect to all addresses; last error: UNKNOWN: ipv4:10.0.0.5:8080: Ssl handshake failed (TSI_PROTOCOL_"
    "FAILURE): SSL_ERROR_SSL: error:100000f7:SSL routines:OPENSSL_internal:WRONG_VERSION_NUMBER",   # TLS to plaintext
    "Ssl handshake failed: certificate verify failed",
    "error:0A000086:SSL routines::certificate verify failed",
    "CERTIFICATE_VERIFY_FAILED",                                                     # upper case
    "SSL_ERROR_SSL",
    "certificate has expired",                                                       # lower case
    "ssl error",
    "Certificate unknown",                                                           # capitalised
    "SSL",
])
def test_a_grpc_unavailable_naming_ssl_or_a_certificate_is_a_permanent_tls_failure(details):
    # A wrong CA or hostname is a misconfiguration that waiting never heals: no retry, so the
    # submission is a SYSTEM_ERROR at once instead of after the whole infrastructure retry window.
    exc = rpc_error(CODE.UNAVAILABLE, details)
    assert infra_errors._is_grpc_certificate_failure(exc) is True
    assert infra_errors.transient_reason(exc) is None
    assert infra_errors._is_certificate_failure(exc) is False       # that one is MinIO's (urllib3 SSLError)


@pytest.mark.parametrize("details", [
    "Connection refused",
    "Socket closed",
    "failed to connect to all addresses; last error: UNKNOWN: ipv4:10.0.0.5:8080: Failed to connect to remote host: "
    "Connection refused",
    "failed to connect to all addresses; last error: UNAVAILABLE: ipv4:10.0.0.5:8080: Socket closed",
    "Connection reset by peer",
    "failed to connect to all addresses; last error: UNAVAILABLE: ipv4:10.0.0.5:8080: recvmsg:Connection reset by peer",
    "DNS resolution failed for judge-server:8080: C-ares status is not ARES_SUCCESS qtype=A name=judge-server",
    # A sandbox restart that cuts a connection, even in the middle of a handshake, reads like a handshake error but
    # names neither ssl nor a certificate: "handshake" is not the TLS signal, so it is waited out.
    "Handshake read failed",
    "connection closed during handshake",
    "",
    None,                                                                            # a bare UNAVAILABLE
])
def test_an_ordinary_grpc_unavailable_is_still_waited_out(details):
    exc = rpc_error(CODE.UNAVAILABLE, details)
    assert infra_errors._is_grpc_certificate_failure(exc) is False
    assert infra_errors.transient_reason(exc) == "sandbox"


@pytest.mark.parametrize("code", [CODE.INTERNAL, CODE.RESOURCE_EXHAUSTED])
@pytest.mark.parametrize("details", ["ssl", "certificate", WRONG_CA, HOSTNAME_MISMATCH])
def test_only_unavailable_is_the_tls_signal(code, details):
    # A judger failure (INTERNAL) or a full sandbox (RESOURCE_EXHAUSTED) is not a handshake failure, whatever its
    # details say: both stay transient.
    exc = rpc_error(code, details)
    assert infra_errors._is_grpc_certificate_failure(exc) is False
    assert infra_errors.transient_reason(exc) == "sandbox"


@pytest.mark.parametrize("code", [c for c in CODE if c != CODE.UNAVAILABLE])
def test_no_other_grpc_code_is_a_tls_failure(code):
    assert infra_errors._is_grpc_certificate_failure(rpc_error(code, WRONG_CA)) is False


def test_an_unavailable_whose_details_cannot_be_read_is_still_waited_out():
    class Broken(grpc.RpcError):
        def code(self):
            return CODE.UNAVAILABLE

        def details(self):
            raise RuntimeError("no details")

    assert infra_errors._is_grpc_certificate_failure(Broken()) is False
    assert infra_errors.transient_reason(Broken()) == "sandbox"


@pytest.mark.parametrize("exc", [grpc.RpcError(), ValueError("ssl handshake"), RuntimeError("certificate")])
def test_what_is_not_a_grpc_status_is_not_a_grpc_tls_failure(exc):
    assert infra_errors._is_grpc_certificate_failure(exc) is False
    assert infra_errors._is_certificate_failure(exc) is False


def test_minio_certificate_failures_are_still_permanent():
    assert infra_errors.transient_reason(Urllib3SSLError("bad cert")) is None
    assert infra_errors.transient_reason(MaxRetryError(None, "/b/o", Urllib3SSLError("bad cert"))) is None
    assert infra_errors._is_grpc_certificate_failure(Urllib3SSLError("bad cert")) is False


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
