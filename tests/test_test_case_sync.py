import errno
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


def _write_bundle_zip(dest_path, files=None):
    """Write a valid test-case bundle zip to dest_path."""
    files = files or {"info": '{"test_case_number": 1}', "1.in": "1 2\n", "1.out": "3\n"}
    with zipfile.ZipFile(dest_path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)


class _ZipClient:
    """Fake MinIO client: writes a valid bundle zip, or raises a preset error."""
    def __init__(self, files=None, error=None):
        self._files = files
        self._error = error

    def fget_object(self, bucket, key, dest):
        if self._error:
            raise self._error
        _write_bundle_zip(dest, self._files)


def test_ensure_present_race_loser_is_swallowed(tmp_path, monkeypatch):
    from test_case_sync import ensure_present
    def fake_replace(src, dst):
        raise OSError(errno.ENOTEMPTY, "directory not empty")
    monkeypatch.setattr(os, "replace", fake_replace)
    dest = ensure_present("slug__abc", cache_root=str(tmp_path), client=_ZipClient(), bucket="b")
    assert dest == os.path.join(str(tmp_path), "slug__abc")
    assert os.listdir(tmp_path) == []  # temp dir cleaned; winner's dest not created by us


def test_ensure_present_non_race_oserror_propagates(tmp_path, monkeypatch):
    from test_case_sync import ensure_present
    def fake_replace(src, dst):
        raise OSError(errno.EACCES, "permission denied")
    monkeypatch.setattr(os, "replace", fake_replace)
    with pytest.raises(OSError):
        ensure_present("slug__abc", cache_root=str(tmp_path), client=_ZipClient(), bucket="b")
    assert os.listdir(tmp_path) == []  # temp dir cleaned up


def test_ensure_present_download_failure_cleans_up(tmp_path):
    from test_case_sync import ensure_present
    client = _ZipClient(error=RuntimeError("network down"))
    with pytest.raises(RuntimeError):
        ensure_present("slug__abc", cache_root=str(tmp_path), client=client, bucket="b")
    assert os.listdir(tmp_path) == []  # no partial cache dir left behind


def test_safe_extract_rejects_zip_slip(tmp_path):
    from test_case_sync import _safe_extract_zip
    zip_path = tmp_path / "evil.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("../escape.txt", "pwned")
    dest = tmp_path / "dest"
    dest.mkdir()
    with pytest.raises(ValueError):
        _safe_extract_zip(str(zip_path), str(dest))
    assert not (tmp_path / "escape.txt").exists()


def test_safe_extract_accepts_normal_entries(tmp_path):
    from test_case_sync import _safe_extract_zip
    zip_path = tmp_path / "ok.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("info", "{}")
        zf.writestr("1.in", "x")
    dest = tmp_path / "dest"
    dest.mkdir()
    _safe_extract_zip(str(zip_path), str(dest))
    assert (dest / "info").exists()
    assert (dest / "1.in").exists()


def test_ensure_present_rejects_zip_slip_bundle(tmp_path):
    from test_case_sync import ensure_present
    class _SlipClient:
        def fget_object(self, bucket, key, dest):
            with zipfile.ZipFile(dest, "w") as zf:
                zf.writestr("../escape.txt", "pwned")
    with pytest.raises(ValueError):
        ensure_present("slug__abc", cache_root=str(tmp_path), client=_SlipClient(), bucket="b")
    assert os.listdir(tmp_path) == []              # temp dir cleaned up
    assert not (tmp_path / "escape.txt").exists()
