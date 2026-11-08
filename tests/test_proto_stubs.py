"""The committed gRPC stubs (judge_server_pb2*.py) are generated from oj-common's judge_server.proto by
scripts/gen_proto.sh, which records the proto's sha256 in judge_server_proto.sha256. A changed proto without a
regeneration fails here, so the worker never speaks a stale contract."""
import hashlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
PROTO = ROOT.parent / "oj-common" / "src" / "main" / "proto" / "judge_server.proto"


def test_stubs_match_proto():
    if not PROTO.is_file():
        pytest.skip(f"{PROTO} is absent: the oj-common checkout is not next to this repo")
    recorded = (ROOT / "judge_server_proto.sha256").read_text().split()[0]
    assert hashlib.sha256(PROTO.read_bytes()).hexdigest() == recorded, \
        "judge_server.proto changed: run scripts/gen_proto.sh and commit the stubs"


def test_stubs_expose_the_judge_server_contract():
    import judge_server_pb2 as pb
    import judge_server_pb2_grpc as pb_grpc

    assert pb.DESCRIPTOR.package == "oj.judge.v1"
    assert hasattr(pb_grpc, "JudgeServerStub") and hasattr(pb_grpc, "JudgeServerServicer")
    assert {m for m in ("Judge", "Ping") if hasattr(pb_grpc.JudgeServerServicer, m)} == {"Judge", "Ping"}
