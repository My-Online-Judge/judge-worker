"""A fake failed gRPC call, shared by the tests of the dispatcher, the error classification and the worker."""
import grpc


class _RpcError(grpc.RpcError):
    """What a failed unary call raises: grpc.RpcError carrying a status code and its details."""

    def __init__(self, code, details):
        super().__init__(details)
        self._code, self._details = code, details

    def code(self):
        return self._code

    def details(self):
        return self._details


def rpc_error(code, details=""):
    return _RpcError(code, details)
