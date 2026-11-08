import grpc
import pytest

import grpc_errors
from tests.rpc_errors import rpc_error

CODE = grpc.StatusCode


def test_code_of_a_failed_call_is_its_status_code():
    assert grpc_errors.code_of(rpc_error(CODE.UNAVAILABLE, "down")) is CODE.UNAVAILABLE


@pytest.mark.parametrize("exc", [grpc.RpcError(), ValueError("x"), RuntimeError("tls")])
def test_anything_but_a_coded_rpc_error_has_no_code(exc):
    assert grpc_errors.code_of(exc) is None


def test_a_non_rpc_error_with_a_code_method_has_no_code():
    class NotGrpc(Exception):
        def code(self):
            return CODE.UNAVAILABLE

    assert grpc_errors.code_of(NotGrpc()) is None


def test_a_code_that_raises_is_no_code():
    class Broken(grpc.RpcError):
        def code(self):
            raise RuntimeError("gone")

    assert grpc_errors.code_of(Broken()) is None


@pytest.mark.parametrize("value", [None, "UNAVAILABLE", 14, object()])
def test_a_code_that_is_not_a_status_code_is_no_code(value):
    class Odd(grpc.RpcError):
        def code(self):
            return value

    assert grpc_errors.code_of(Odd()) is None


def test_details_of_a_failed_call():
    assert grpc_errors.details_of(rpc_error(CODE.UNAVAILABLE, "down")) == "down"


@pytest.mark.parametrize("exc", [grpc.RpcError(), ValueError("x"), rpc_error(CODE.UNAVAILABLE, None)])
def test_missing_details_are_an_empty_string(exc):
    assert grpc_errors.details_of(exc) == ""


def test_details_that_raise_are_an_empty_string():
    class Broken(grpc.RpcError):
        def details(self):
            raise RuntimeError("gone")

    assert grpc_errors.details_of(Broken()) == ""


def test_the_codes_worth_waiting_out_are_down_and_busy():
    assert grpc_errors.RETRYABLE_CODES == {CODE.UNAVAILABLE, CODE.INTERNAL, CODE.RESOURCE_EXHAUSTED}
