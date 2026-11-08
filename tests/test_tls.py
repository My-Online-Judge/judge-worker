import importlib
import re
from unittest.mock import patch

import grpc
import pytest

import config
import tls


def test_disabled_returns_empty(monkeypatch):
    monkeypatch.delenv("OJ_TLS_DIR", raising=False)
    importlib.reload(config)

    assert tls.enabled() is False
    assert tls.kafka_kwargs() == {}


def test_enabled_returns_ssl_kwargs(tls_dir):
    ca, cert, key = (str(tls_dir / name) for name in ("ca.crt", "tls.crt", "tls.key"))

    assert tls.enabled() is True
    assert tls.paths() == (ca, cert, key)
    assert tls.kafka_kwargs() == {"security_protocol": "SSL", "ssl_cafile": ca, "ssl_certfile": cert,
                                  "ssl_keyfile": key, "ssl_check_hostname": True}


@pytest.mark.parametrize("kwargs", ["kafka_kwargs", "requests_kwargs", "metrics_kwargs", "minio_http_client",
                                    "grpc_channel_credentials"])
def test_missing_file_raises_with_name(tls_dir, kwargs):
    (tls_dir / "tls.key").unlink()

    # With TLS on, a missing certificate stops the worker; it never falls back to plaintext.
    with pytest.raises(RuntimeError, match=re.escape(str(tls_dir / "tls.key"))):
        getattr(tls, kwargs)()


def test_requests_kwargs_disabled(monkeypatch):
    monkeypatch.delenv("OJ_TLS_DIR", raising=False)
    importlib.reload(config)

    assert tls.requests_kwargs() == {}


def test_requests_kwargs_enabled(tls_dir):
    ca, cert, key = (str(tls_dir / name) for name in ("ca.crt", "tls.crt", "tls.key"))

    # The worker's certificate presented; the service verified against the CA (requests also checks the host name).
    assert tls.requests_kwargs() == {"cert": (cert, key), "verify": ca}


def test_metrics_kwargs_disabled(monkeypatch):
    monkeypatch.delenv("OJ_TLS_DIR", raising=False)
    importlib.reload(config)

    assert tls.metrics_kwargs() == {}


def test_metrics_kwargs_enabled(tls_dir):
    ca, cert, key = (str(tls_dir / name) for name in ("ca.crt", "tls.crt", "tls.key"))

    assert tls.metrics_kwargs() == {"certfile": cert, "keyfile": key, "client_cafile": ca,
                                    "client_auth_required": True}


def test_minio_http_client_none_when_disabled(monkeypatch):
    monkeypatch.delenv("OJ_TLS_DIR", raising=False)
    importlib.reload(config)

    # None: minio-py builds its own pool, as before TLS.
    assert tls.minio_http_client() is None


def test_minio_http_client_configured(tls_dir):
    from minio import Minio

    ca, cert, key = (str(tls_dir / name) for name in ("ca.crt", "tls.crt", "tls.key"))
    kw = tls.minio_http_client().connection_pool_kw
    default = Minio("minio:9000")._http.connection_pool_kw

    # MinIO verified against the CA, the worker's certificate presented.
    assert (kw["cert_reqs"], kw["ca_certs"], kw["cert_file"], kw["key_file"]) == ("CERT_REQUIRED", ca, cert, key)
    # minio-py's own pool size and retry pacing, but shorter timeouts and fewer retries (config.MINIO_TIMEOUT_SECONDS,
    # MINIO_RETRIES): the MinIO sync is a stretch with no poll(), so it must fit the consumer's poll interval.
    assert (kw["timeout"].connect_timeout, kw["timeout"].read_timeout) == (
        config.Config.MINIO_TIMEOUT_SECONDS, config.Config.MINIO_TIMEOUT_SECONDS)
    assert config.Config.MINIO_TIMEOUT_SECONDS < default["timeout"].read_timeout
    assert kw["maxsize"] == default["maxsize"]
    assert kw["retries"].total == config.Config.MINIO_RETRIES
    assert (kw["retries"].backoff_factor, kw["retries"].status_forcelist) == (
        default["retries"].backoff_factor, default["retries"].status_forcelist)


def test_grpc_credentials_none_without_tls(monkeypatch):
    monkeypatch.delenv("OJ_TLS_DIR", raising=False)
    importlib.reload(config)

    # None: the caller opens a plaintext channel, as before TLS.
    assert tls.grpc_channel_credentials() is None


def test_grpc_credentials_reads_files(tls_dir):
    assert isinstance(tls.grpc_channel_credentials(), grpc.ChannelCredentials)

    # With TLS on, a missing certificate stops the worker; it never falls back to plaintext.
    (tls_dir / "ca.crt").unlink()
    with pytest.raises(RuntimeError, match=re.escape(str(tls_dir / "ca.crt"))):
        tls.grpc_channel_credentials()


def test_grpc_credentials_trust_the_ca_and_present_the_workers_certificate(tls_dir):
    with patch("tls.grpc.ssl_channel_credentials") as ssl_channel_credentials:
        creds = tls.grpc_channel_credentials()

    # The sandbox is verified against the CA (gRPC checks the host name of the address), and it requires the worker's
    # certificate: the files' contents, not their paths.
    ssl_channel_credentials.assert_called_once_with(
        root_certificates=b"ca.crt", private_key=b"tls.key", certificate_chain=b"tls.crt")
    assert creds is ssl_channel_credentials.return_value
