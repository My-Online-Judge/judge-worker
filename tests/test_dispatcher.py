import hashlib
from unittest.mock import MagicMock, patch

import dispatcher


def test_token_header_is_sha256_hex():
    assert dispatcher.token_header("abc") == hashlib.sha256(b"abc").hexdigest()


def test_build_judge_body_picks_judge_fields():
    event = {
        "submissionId": "sub-1",
        "src": "code",
        "language_config": {"compile": {}, "run": {}},
        "max_cpu_time": 1000,
        "max_memory": 268435456,
        "test_case_id": "prob",
        "output": True,
    }
    body = dispatcher.build_judge_body(event)
    assert body == {
        "src": "code",
        "language_config": {"compile": {}, "run": {}},
        "max_cpu_time": 1000,
        "max_memory": 268435456,
        "test_case_id": "prob",
        "output": True,
    }
    assert "submissionId" not in body


@patch("dispatcher.requests.post")
def test_call_judge_server_posts_with_token_header(mock_post):
    resp = MagicMock()
    resp.json.return_value = {"err": None, "data": []}
    resp.raise_for_status.return_value = None
    mock_post.return_value = resp

    out = dispatcher.call_judge_server("http://js:8080", "secret", {"src": "x"}, timeout=30)

    assert out == {"err": None, "data": []}
    args, kwargs = mock_post.call_args
    assert args[0] == "http://js:8080/judge"
    assert kwargs["headers"]["X-Judge-Server-Token"] == hashlib.sha256(b"secret").hexdigest()
    assert kwargs["json"] == {"src": "x"}
    assert kwargs["timeout"] == 30
