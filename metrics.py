"""The worker's Prometheus endpoint (sub-project 6 §3.1): sandbox health and dispatch counts, scraped as job
judge-worker. Its being scrapable at all is the worker's liveness signal (alert JudgeWorkerDown)."""
import ssl
import threading
from wsgiref.simple_server import WSGIRequestHandler

from prometheus_client import make_wsgi_app, start_http_server
from prometheus_client.exposition import ThreadingWSGIServer

import tls

# A TLS client gets this long to finish its handshake, and then for each read or write, before it is dropped.
TLS_HANDSHAKE_TIMEOUT_SECONDS = 5


class _TlsServer(ThreadingWSGIServer):
    """/metrics over TLS, each handshake in its connection's own thread.

    prometheus-client's TLS mode wraps the listening socket, so every handshake ran inside accept() on the one serving
    thread, with no timeout: a client that connected and sent nothing (anything on oj-net or sandbox-net, where
    submitted code runs) stalled every scrape. Here accept() only accepts."""

    def __init__(self, address, handler, ssl_context):
        self.ssl_context = ssl_context
        super().__init__(address, handler)

    def get_request(self):
        conn, address = self.socket.accept()
        conn.settimeout(TLS_HANDSHAKE_TIMEOUT_SECONDS)
        return self.ssl_context.wrap_socket(conn, server_side=True, do_handshake_on_connect=False), address

    def finish_request(self, request, client_address):
        try:
            request.do_handshake()
        except OSError:  # no or a refused client certificate, not TLS at all, or silent past the timeout
            return  # the connection is closed (shutdown_request), as the old in-accept handshake failure was: no log
        super().finish_request(request, client_address)


class _QuietHandler(WSGIRequestHandler):
    """No log line per scrape, like prometheus-client's own server."""

    def log_message(self, format, *args):
        pass


def _ssl_context(certfile, keyfile, client_cafile, client_auth_required):
    """The server context from tls.metrics_kwargs(), as prometheus-client builds it for start_http_server."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_verify_locations(cafile=client_cafile)
    if client_auth_required:
        context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(certfile, keyfile)
    return context


def start(config):
    """Serve /metrics on METRICS_PORT from a daemon thread; with TLS on, over HTTPS to clients presenting a certificate
    signed by the CA (sub-project 10)."""
    settings = tls.metrics_kwargs()
    if not settings:
        start_http_server(config.METRICS_PORT)
        return
    server = _TlsServer(("0.0.0.0", config.METRICS_PORT), _QuietHandler, _ssl_context(**settings))
    server.set_app(make_wsgi_app())
    threading.Thread(target=server.serve_forever, name="metrics", daemon=True).start()
