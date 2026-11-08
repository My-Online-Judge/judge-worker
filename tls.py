"""The worker's client certificate (sub-project 10): on when OJ_TLS_DIR is set, plaintext otherwise.

Config is read at call time (config.Config), so a reloaded config is seen.
"""
import os

import grpc
import urllib3

import config


def enabled() -> bool:
    return bool(config.Config.OJ_TLS_DIR)


def paths() -> tuple[str, str, str]:
    """(ca, cert, key). With TLS on, a missing file raises: the worker must not start without its certificate, and
    never falls back to plaintext."""
    c = config.Config
    files = (c.TLS_CA_FILE, c.TLS_CERT_FILE, c.TLS_KEY_FILE)
    if enabled():
        for f in files:
            if not os.path.isfile(f):
                raise RuntimeError(f"OJ_TLS_DIR is set but {f} is missing")
    return files


def kafka_kwargs() -> dict:
    """kafka-python settings for listener INTERNAL: TLS, the broker verified against the CA and its host name, and the
    worker's own certificate presented (the broker requires one)."""
    if not enabled():
        return {}
    ca, cert, key = paths()
    return {"security_protocol": "SSL", "ssl_cafile": ca, "ssl_certfile": cert, "ssl_keyfile": key,
            "ssl_check_hostname": True}


def requests_kwargs() -> dict:
    """requests settings for calls to the services (the heartbeat to submission-service) and to the sandboxes (/ping):
    the worker's certificate presented (they require one), the server verified against the CA (requests also checks the
    host name)."""
    if not enabled():
        return {}
    ca, cert, key = paths()
    return {"cert": (cert, key), "verify": ca}


def grpc_channel_credentials() -> grpc.ChannelCredentials | None:
    """Credentials for a gRPC channel to a sandbox (JudgeServer): the sandbox verified against the CA (gRPC checks the
    host name of the address dialled), the worker's certificate presented (the sandbox requires one and admits only the
    CNs it allow-lists). None when TLS is off: the caller opens a plaintext channel. A channel keeps the files' contents
    from the moment it is created."""
    if not enabled():
        return None
    ca, cert, key = paths()
    return grpc.ssl_channel_credentials(root_certificates=_read(ca), private_key=_read(key),
                                        certificate_chain=_read(cert))


def _read(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def metrics_kwargs() -> dict:
    """prometheus-client start_http_server settings: /metrics over TLS with the worker's certificate, and only clients
    presenting a certificate signed by the CA (Prometheus) get an answer."""
    if not enabled():
        return {}
    ca, cert, key = paths()
    return {"certfile": cert, "keyfile": key, "client_cafile": ca, "client_auth_required": True}


def minio_http_client() -> urllib3.PoolManager | None:
    """urllib3 pool for minio-py: MinIO verified against the CA (and its host name), the worker's certificate presented
    should MinIO ask for one. Otherwise minio-py's own pool size and retry pacing; the timeouts and retry count are the
    worker's (config MINIO_TIMEOUT_SECONDS, MINIO_RETRIES), short enough that a MinIO brownout does not hold the
    consumer past its poll interval. None when TLS is off, and minio-py builds its default pool."""
    if not enabled():
        return None
    ca, cert, key = paths()
    timeout = config.Config.MINIO_TIMEOUT_SECONDS
    return urllib3.PoolManager(
        cert_reqs="CERT_REQUIRED", ca_certs=ca, cert_file=cert, key_file=key,
        timeout=urllib3.Timeout(connect=timeout, read=timeout), maxsize=10,
        retries=urllib3.Retry(total=config.Config.MINIO_RETRIES, backoff_factor=0.2,
                              status_forcelist=[500, 502, 503, 504]))
