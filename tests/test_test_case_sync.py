import errno
import io
import os
import time
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


def test_sweep_cache_evicts_stale_keeps_fresh_and_inprogress(tmp_path):
    from test_case_sync import sweep_cache
    now = 1_000_000.0
    old = tmp_path / "slug__old"; old.mkdir(); (old / "info").write_text("{}")
    fresh = tmp_path / "slug__fresh"; fresh.mkdir(); (fresh / "info").write_text("{}")
    inprogress = tmp_path / "tmp_partial"; inprogress.mkdir()  # no info file
    os.utime(old, (now - 100_000, now - 100_000))
    os.utime(fresh, (now - 10, now - 10))
    os.utime(inprogress, (now - 100_000, now - 100_000))
    evicted = sweep_cache(str(tmp_path), ttl_seconds=86_400, now=now)
    assert evicted == ["slug__old"]
    assert not old.exists()
    assert fresh.exists()
    assert inprogress.exists()


def test_ensure_present_cache_hit_bumps_mtime(tmp_path):
    from test_case_sync import ensure_present
    dest = tmp_path / "slug__abc"; dest.mkdir(); (dest / "info").write_text("{}")
    os.utime(dest, (1000.0, 1000.0))
    result = ensure_present("slug__abc", cache_root=str(tmp_path), client=None, bucket="b")
    assert result == str(dest)
    assert os.path.getmtime(dest) > 1000.0  # bumped toward now


def test_maybe_sweep_respects_interval(tmp_path):
    from test_case_sync import maybe_sweep
    now = 1_000_000.0
    stale = tmp_path / "slug__old"; stale.mkdir(); (stale / "info").write_text("{}")
    os.utime(stale, (now - 100_000, now - 100_000))
    ev1 = maybe_sweep(str(tmp_path), ttl_seconds=86_400, interval_seconds=3600, now=now)
    assert ev1 == ["slug__old"]
    stale2 = tmp_path / "slug__old2"; stale2.mkdir(); (stale2 / "info").write_text("{}")
    os.utime(stale2, (now - 100_000, now - 100_000))
    ev2 = maybe_sweep(str(tmp_path), ttl_seconds=86_400, interval_seconds=3600, now=now + 10)
    assert ev2 == []              # within interval → skipped
    assert stale2.exists()


def test_sweep_cache_tolerates_concurrent_removal(tmp_path, monkeypatch):
    from test_case_sync import sweep_cache
    now = 1_000_000.0
    gone = tmp_path / "slug__gone"; gone.mkdir(); (gone / "info").write_text("{}")
    stale = tmp_path / "slug__stale"; stale.mkdir(); (stale / "info").write_text("{}")
    os.utime(gone, (now - 100_000, now - 100_000))
    os.utime(stale, (now - 100_000, now - 100_000))
    real_getmtime = os.path.getmtime
    def flaky_getmtime(p):
        if str(p).endswith("slug__gone"):
            raise FileNotFoundError("vanished mid-sweep")
        return real_getmtime(p)
    monkeypatch.setattr(os.path, "getmtime", flaky_getmtime)
    evicted = sweep_cache(str(tmp_path), ttl_seconds=86_400, now=now)
    assert evicted == ["slug__stale"]   # gone skipped without crashing, stale still evicted
