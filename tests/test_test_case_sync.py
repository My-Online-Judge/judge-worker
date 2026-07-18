import io
import os
import zipfile

import pytest

import test_case_sync


class FakeClient:
    def __init__(self, zip_bytes):
        self._zip = zip_bytes
        self.calls = []

    def fget_object(self, bucket, key, file_path):
        self.calls.append((bucket, key, file_path))
        with open(file_path, "wb") as f:
            f.write(self._zip)


def _bundle_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("info", '{"test_case_number":1}')
        z.writestr("1.in", "1 2\n")
        z.writestr("1.out", "3\n")
    return buf.getvalue()


def test_miss_downloads_and_extracts(tmp_path):
    client = FakeClient(_bundle_zip())
    dest = test_case_sync.ensure_present(
        "simple-a-plus-b__abc123", cache_root=str(tmp_path), client=client, bucket="test-cases")

    assert os.path.isfile(os.path.join(dest, "info"))
    assert os.path.isfile(os.path.join(dest, "1.in"))
    assert client.calls == [("test-cases", "simple-a-plus-b/abc123.zip",
                             client.calls[0][2])]


def test_hit_is_a_noop(tmp_path):
    dest = tmp_path / "simple-a-plus-b__abc123"
    dest.mkdir()
    (dest / "info").write_text("cached")
    client = FakeClient(b"should-not-be-used")

    result = test_case_sync.ensure_present(
        "simple-a-plus-b__abc123", cache_root=str(tmp_path), client=client, bucket="test-cases")

    assert result == str(dest)
    assert client.calls == []  # cache hit: MinIO never touched


def test_malformed_id_raises(tmp_path):
    with pytest.raises(ValueError):
        test_case_sync.ensure_present("no-double-underscore", cache_root=str(tmp_path),
                                      client=FakeClient(b""), bucket="b")
