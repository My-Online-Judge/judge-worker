"""The configured sandbox address (JUDGE_SERVER_URL) as a gRPC target. Config.validate and the dispatcher both parse
with it; it imports nothing of the worker, so config can use it without a cycle (dispatcher imports config)."""
from urllib.parse import urlsplit


def parse_address(url):
    """The gRPC target (host:port) of a configured sandbox URL: an http:// or https:// scheme is dropped (the transport
    is chosen by OJ_TLS_DIR, not by the scheme), a bare host:port is kept. ValueError for a path, an empty host or a bad
    port: a misconfiguration that would otherwise fail on every submission."""
    rest = url.strip()
    for scheme in ("https://", "http://"):
        if rest.lower().startswith(scheme):
            rest = rest[len(scheme):]
            break
    parts = urlsplit("//" + rest)
    # parts.port raises ValueError itself for a port that is not a number in range; 0 is in range but cannot be dialled.
    if not parts.hostname or parts.path or parts.query or parts.fragment or parts.port == 0:
        raise ValueError(f"not a judge-server address (host:port): {url!r}")
    return parts.netloc
