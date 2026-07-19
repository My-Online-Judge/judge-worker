import errno
import logging
import os
import shutil
import tempfile
import time
import zipfile

from config import Config

log = logging.getLogger("judge-worker")


def _safe_extract_zip(zip_path, dest_dir):
    """Extract a zip into dest_dir, refusing any entry that would escape it (zip-slip)."""
    dest_root = os.path.realpath(dest_dir)
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            target = os.path.realpath(os.path.join(dest_dir, member))
            if target != dest_root and not target.startswith(dest_root + os.sep):
                raise ValueError(f"Unsafe path in test-case bundle (zip-slip): {member}")
        zf.extractall(dest_dir)


def _default_client():
    # Imported lazily so unit tests (which inject a fake client) need no `minio` install.
    from minio import Minio

    endpoint = Config.MINIO_ENDPOINT
    secure = endpoint.startswith("https://")
    host = endpoint.split("://", 1)[-1]
    return Minio(host, access_key=Config.MINIO_ACCESS_KEY,
                 secret_key=Config.MINIO_SECRET_KEY, secure=secure)


def ensure_present(test_case_id, cache_root=None, client=None, bucket=None):
    """Ensure <cache_root>/<test_case_id>/ holds the test-case bundle, downloading it from MinIO on
    a miss. Bundles are immutable and content-addressed, so a present dir is never stale. Returns
    the directory path. Raises ValueError for a malformed test_case_id."""
    cache_root = cache_root or Config.TEST_CASE_CACHE_DIR
    bucket = bucket or Config.MINIO_BUCKET
    dest = os.path.join(cache_root, test_case_id)
    if os.path.exists(os.path.join(dest, "info")):
        try:
            os.utime(dest, None)  # mark recently used so the age sweep keeps it
        except OSError:
            pass
        return dest

    slug, sep, digest = test_case_id.rpartition("__")
    if not sep:
        raise ValueError(f"Malformed test_case_id (expected '<slug>__<hash>'): {test_case_id}")
    key = f"{slug}/{digest}.zip"

    client = client or _default_client()
    os.makedirs(cache_root, exist_ok=True)
    tmp_dir = tempfile.mkdtemp(dir=cache_root)
    tmp_zip = os.path.join(tmp_dir, "bundle.zip")
    try:
        client.fget_object(bucket, key, tmp_zip)
        _safe_extract_zip(tmp_zip, tmp_dir)
        os.remove(tmp_zip)
        try:
            os.replace(tmp_dir, dest)  # atomic install
        except OSError as e:
            # ENOTEMPTY/EEXIST == another worker already installed this bundle (benign race).
            # Any other OSError (ENOSPC, EACCES, EXDEV, ...) is a real failure — let it propagate.
            if e.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                raise
            shutil.rmtree(tmp_dir, ignore_errors=True)
            log.debug("Test-case bundle %s already installed by another worker", test_case_id)
            return dest
        log.info("Synced test-case bundle %s from MinIO (%s)", test_case_id, key)
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
    return dest


def sweep_cache(cache_root, ttl_seconds, now):
    """Delete installed bundle dirs under cache_root not used within ttl_seconds.

    An installed bundle is a child dir containing an `info` file; in-progress temp dirs
    (no `info`) are never touched. Resilient to concurrent removal by another worker
    (a vanished entry is skipped, not fatal). Returns the sorted list of evicted dir names.
    """
    evicted = []
    try:
        names = sorted(os.listdir(cache_root))
    except OSError:
        return evicted
    for name in names:
        path = os.path.join(cache_root, name)
        try:
            if not os.path.isdir(path):
                continue
            if not os.path.exists(os.path.join(path, "info")):
                continue  # in-progress install — leave it
            if now - os.path.getmtime(path) > ttl_seconds:
                shutil.rmtree(path, ignore_errors=True)
                evicted.append(name)
        except OSError:
            # Entry vanished mid-sweep (another worker won) or became inaccessible — skip it.
            continue
    return evicted


def maybe_sweep(cache_root=None, ttl_seconds=None, interval_seconds=None, now=None):
    """Rate-limited driver for sweep_cache. Runs at most once per interval_seconds,
    coordinated across workers by the mtime of a `.last_sweep` marker in cache_root."""
    cache_root = cache_root or Config.TEST_CASE_CACHE_DIR
    if ttl_seconds is None:
        ttl_seconds = Config.TEST_CASE_CACHE_TTL_SECONDS
    if interval_seconds is None:
        interval_seconds = Config.TEST_CASE_CACHE_SWEEP_INTERVAL_SECONDS
    if now is None:
        now = time.time()
    marker = os.path.join(cache_root, ".last_sweep")
    try:
        os.makedirs(cache_root, exist_ok=True)
        try:
            last = os.path.getmtime(marker)
        except OSError:
            last = 0
        if now - last < interval_seconds:
            return []
        with open(marker, "a"):
            os.utime(marker, (now, now))  # claim this interval before sweeping
    except OSError:
        # Cache volume not (yet) accessible — GC is best-effort and must never break judging.
        log.warning("Cache sweep skipped: cache_root %s is not accessible", cache_root)
        return []
    evicted = sweep_cache(cache_root, ttl_seconds, now)
    if evicted:
        log.info("Cache sweep evicted %d stale bundle(s): %s", len(evicted), evicted)
    return evicted
