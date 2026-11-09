#!/bin/sh
# Regenerates judge_server_pb2.py and judge_server_pb2_grpc.py from the sandbox contract in oj-common, and records the
# proto's sha256 (tests/test_proto_stubs.py fails when the proto changes without a regeneration).
# Needs grpcio-tools (requirements.txt) and the oj-common checkout next to this repo. Run from anywhere.
set -eu
cd "$(dirname "$0")/.."
PROTO_DIR=../oj-common/src/main/proto
python -m grpc_tools.protoc -I "$PROTO_DIR" --python_out=. --grpc_python_out=. judge_server.proto
(cd "$PROTO_DIR" && sha256sum judge_server.proto) > judge_server_proto.sha256
