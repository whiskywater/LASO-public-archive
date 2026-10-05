#!/usr/bin/env python3
"""Loopback-only acceptance proxy for deterministic artifact-transfer chaos tests."""

from __future__ import annotations

import argparse
import http.client
import math
import socket
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "LASOAcceptanceProxy/1"

    def log_message(self, _format: str, *_args: object) -> None:
        # Requests carry bearer credentials; never log headers or request data.
        return

    def do_HEAD(self) -> None:
        self._relay()

    def do_GET(self) -> None:
        self._relay()

    def do_PUT(self) -> None:
        self._relay(upload=True)

    def _relay(self, upload: bool = False) -> None:
        upload_deadline = (
            time.monotonic() + self.server.upload_timeout if upload else None
        )
        self.connection.settimeout(self.server.upload_timeout)
        content_length = 0
        if upload:
            length_headers = self.headers.get_all("Content-Length", [])
            if self.headers.get("Transfer-Encoding") or len(length_headers) != 1:
                self.send_error(400, "upload requires a single Content-Length")
                self.close_connection = True
                return
            try:
                content_length = int(length_headers[0])
            except ValueError:
                self.send_error(400, "invalid Content-Length")
                self.close_connection = True
                return
            if content_length < 0:
                self.send_error(400, "invalid Content-Length")
                self.close_connection = True
                return
            if content_length > self.server.max_upload_bytes:
                self.send_error(413, "upload exceeds acceptance proxy limit")
                self.close_connection = True
                return

        upstream = http.client.HTTPConnection(
            self.server.upstream_host,
            self.server.upstream_port,
            timeout=(
                self._remaining(upload_deadline)
                if upload_deadline is not None
                else self.server.upload_timeout
            ),
        )
        try:
            length_header = self.headers.get("Content-Length")
            upstream.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
            for name, value in self.headers.items():
                if name.lower() not in {"host", "connection", "content-length", "expect"}:
                    upstream.putheader(name, value)
            if length_header is not None:
                upstream.putheader("Content-Length", str(content_length))
            upstream.endheaders()

            if upload:
                self._mark(self.server.started_marker)
                remaining = content_length
                while remaining:
                    assert upload_deadline is not None
                    self.connection.settimeout(self._remaining(upload_deadline))
                    # read1 makes at most one underlying read, so a slow drip
                    # cannot reset the timeout repeatedly within a chunk.
                    chunk = self.rfile.read1(min(64 * 1024, remaining))
                    self._remaining(upload_deadline)
                    if not chunk:
                        upstream.close()
                        self.close_connection = True
                        return
                    if self.server.rate_bps:
                        delay = len(chunk) / self.server.rate_bps
                        if delay >= self._remaining(upload_deadline):
                            raise socket.timeout(
                                "rate-limited upload cannot meet its total deadline"
                            )
                        time.sleep(delay)
                        self._remaining(upload_deadline)

                    time_left = self._remaining(upload_deadline)
                    if upstream.sock is not None:
                        upstream.sock.settimeout(time_left)
                    upstream.send(chunk)
                    self._remaining(upload_deadline)
                    remaining -= len(chunk)

            if upload_deadline is not None:
                time_left = self._remaining(upload_deadline)
                if upstream.sock is not None:
                    upstream.sock.settimeout(time_left)
            response = upstream.getresponse()
            if upload_deadline is not None:
                self._remaining(upload_deadline)
            body = response.read() if upload else None
            if upload_deadline is not None:
                self._remaining(upload_deadline)

            if upload and response.status == 200 and self.server.published_marker:
                self._mark(self.server.published_marker)
                if self.server.release_file:
                    barrier_deadline = time.monotonic() + self.server.hold_timeout
                    while not Path(self.server.release_file).exists():
                        barrier_left = barrier_deadline - time.monotonic()
                        if barrier_left <= 0:
                            self.send_error(504, "acceptance release barrier timed out")
                            return
                        wait_for = min(0.025, barrier_left)
                        if upload_deadline is not None:
                            wait_for = min(wait_for, self._remaining(upload_deadline))
                        time.sleep(wait_for)
                    if upload_deadline is not None:
                        self._remaining(upload_deadline)

            if upload_deadline is not None:
                self.connection.settimeout(self._remaining(upload_deadline))
            self.send_response(response.status, response.reason)
            for name, value in response.getheaders():
                if name.lower() not in {"connection", "transfer-encoding", "keep-alive"}:
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
            if upload_deadline is not None:
                self._remaining(upload_deadline)

            if self.command != "HEAD":
                if upload:
                    if upload_deadline is not None:
                        self.connection.settimeout(self._remaining(upload_deadline))
                    self.wfile.write(body or b"")
                    if upload_deadline is not None:
                        self._remaining(upload_deadline)
                else:
                    while True:
                        chunk = response.read(64 * 1024)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
            self.close_connection = True
        except (BrokenPipeError, ConnectionResetError, OSError, http.client.HTTPException):
            # Client death during upload is an expected injected condition.
            self.close_connection = True
        finally:
            upstream.close()

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("acceptance upload deadline exceeded")
        return remaining

    @staticmethod
    def _mark(marker: str | None) -> None:
        if marker:
            path = Path(marker)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("ready\n", encoding="ascii")


class ArtifactChaosProxy(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__(("127.0.0.1", args.listen_port), ProxyHandler)
        self.upstream_host = args.upstream_host
        self.upstream_port = args.upstream_port
        self.started_marker = args.started_marker
        self.published_marker = args.published_marker
        self.release_file = args.release_file
        self.rate_bps = args.rate_bps
        self.hold_timeout = args.hold_timeout
        self.max_upload_bytes = args.max_upload_bytes
        self.upload_timeout = args.upload_timeout


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--upstream-host", default="127.0.0.1")
    parser.add_argument("--upstream-port", type=int, required=True)
    parser.add_argument("--started-marker")
    parser.add_argument("--published-marker")
    parser.add_argument("--release-file")
    parser.add_argument("--rate-bps", type=int, default=0)
    parser.add_argument("--hold-timeout", type=float, default=600)
    parser.add_argument("--max-upload-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--upload-timeout", type=float, default=300)
    args = parser.parse_args()
    if (
        args.rate_bps < 0
        or not math.isfinite(args.hold_timeout)
        or args.hold_timeout <= 0
        or args.max_upload_bytes <= 0
        or not math.isfinite(args.upload_timeout)
        or args.upload_timeout <= 0
    ):
        parser.error("rate must be non-negative; size and time limits must be finite and positive")
    if bool(args.published_marker) != bool(args.release_file):
        parser.error("published marker and release file must be provided together")
    server = ArtifactChaosProxy(args)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
