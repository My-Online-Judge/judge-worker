import errno
import logging
import os
import shutil
import tempfile
import zipfile

from config import Config

log = logging.getLogger("judge-worker")


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
        with zipfile.ZipFile(tmp_zip) as zf:
            zf.extractall(tmp_dir)
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
