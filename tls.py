"""The worker's client certificate (sub-project 10): on when OJ_TLS_DIR is set, plaintext otherwise.

Config is read at call time (config.Config), so a reloaded config is seen.
"""
import os

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
    """requests settings for calls to the services (the heartbeat to submission-service): the worker's certificate
    presented (the services require one), the service verified against the CA (requests also checks the host name)."""
    if not enabled():
        return {}
    ca, cert, key = paths()
    return {"cert": (cert, key), "verify": ca}


def metrics_kwargs() -> dict:
    """prometheus-client start_http_server settings: /metrics over TLS with the worker's certificate, and only clients
    presenting a certificate signed by the CA (Prometheus) get an answer."""
    if not enabled():
        return {}
    ca, cert, key = paths()
    return {"certfile": cert, "keyfile": key, "client_cafile": ca, "client_auth_required": True}
