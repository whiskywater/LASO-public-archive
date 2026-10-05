#!/usr/bin/env python3
"""Deterministic local tests for the artifact chaos proxy's upload barriers."""

from __future__ import annotations

import http.client
import importlib.util
import io
import socket
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


PROXY_PATH = Path(__file__).with_name("artifact-chaos-proxy.py")
SPEC = importlib.util.spec_from_file_location("artifact_chaos_proxy", PROXY_PATH)
assert SPEC is not None and SPEC.loader is not None
PROXY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROXY)


class UploadOriginHandler(BaseHTTPRequestHandler):
    def log_message(self, _format: str, *_args: object) -> None:
        return

    def do_PUT(self) -> None:
        expected = int(self.headers.get("Content-Length", "0"))
        chunks = bytearray()
        while len(chunks) < expected:
            chunk = self.rfile.read(min(4096, expected - len(chunks)))
            if not chunk:
                break
            chunks.extend(chunk)
        self.server.upload_body = bytes(chunks)
        self.server.upload_complete = len(chunks) == expected
        self.server.upload_received.set()
        self.send_response(200 if self.server.upload_complete else 400)
        self.send_header("Content-Length", "2")
        self.end_headers()
        try:
            self.wfile.write(b"ok")
        except OSError:
            # The interrupted-upload case deliberately closes the proxy side.
            pass


class ArtifactChaosProxyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="laso-proxy-test-")
        root = Path(self.temp.name)
        self.started = root / "upload-started"
        self.published = root / "object-published"
        self.release = root / "release-publication"
        self.origin = ThreadingHTTPServer(("127.0.0.1", 0), UploadOriginHandler)
        self.origin.daemon_threads = True
        self.origin.upload_received = threading.Event()
        self.origin.upload_body = b""
        self.origin.upload_complete = False
        self.origin_thread = threading.Thread(target=self.origin.serve_forever, daemon=True)
        self.origin_thread.start()

        args = SimpleNamespace(
            listen_port=0,
            upstream_host="127.0.0.1",
            upstream_port=self.origin.server_port,
            started_marker=str(self.started),
            published_marker=str(self.published),
            release_file=str(self.release),
            rate_bps=0,
            hold_timeout=3,
            max_upload_bytes=1024,
            upload_timeout=1,
        )
        self.proxy = PROXY.ArtifactChaosProxy(args)
        self.proxy_thread = threading.Thread(target=self.proxy.serve_forever, daemon=True)
        self.proxy_thread.start()

    def tearDown(self) -> None:
        self.proxy.shutdown()
        self.proxy.server_close()
        self.proxy_thread.join(timeout=2)
        self.origin.shutdown()
        self.origin.server_close()
        self.origin_thread.join(timeout=2)
        self.temp.cleanup()

    def _wait_for(self, path: Path) -> None:
        deadline = time.monotonic() + 2
        while not path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(path.exists(), f"timed out waiting for {path.name}")

    def _connection(self) -> http.client.HTTPConnection:
        return http.client.HTTPConnection("127.0.0.1", self.proxy.server_port, timeout=2)

    def _upload(self, payload: bytes) -> tuple[int | None, float]:
        self.proxy.max_upload_bytes = max(self.proxy.max_upload_bytes, len(payload))
        connection = self._connection()
        started = time.monotonic()
        status: int | None = None
        try:
            connection.request("PUT", "/api/v1/artifacts", body=payload)
            response = connection.getresponse()
            status = response.status
            response.read()
        except (http.client.HTTPException, OSError):
            # The proxy intentionally closes requests that exceed their deadline.
            pass
        finally:
            connection.close()
        return status, time.monotonic() - started

    def test_truncated_client_upload_is_not_published(self) -> None:
        connection = self._connection()
        connection.putrequest("PUT", "/api/v1/artifacts")
        connection.putheader("Content-Length", "64")
        connection.endheaders()
        connection.send(b"partial-body")
        connection.close()

        self._wait_for(self.started)
        self.assertTrue(self.origin.upload_received.wait(2))
        self.assertEqual(self.origin.upload_body, b"partial-body")
        self.assertFalse(self.origin.upload_complete)
        self.assertFalse(self.published.exists())

    def test_oversized_upload_is_rejected_before_forwarding(self) -> None:
        connection = self._connection()
        connection.putrequest("PUT", "/api/v1/artifacts")
        connection.putheader("Content-Length", "1025")
        connection.endheaders()
        response = connection.getresponse()
        self.assertEqual(response.status, 413)
        response.read()
        connection.close()
        self.assertFalse(self.origin.upload_received.is_set())
        self.assertFalse(self.started.exists())
        self.assertFalse(self.published.exists())

    def test_unsupported_upload_framing_is_rejected(self) -> None:
        connection = self._connection()
        connection.putrequest("PUT", "/api/v1/artifacts")
        connection.putheader("Transfer-Encoding", "chunked")
        connection.endheaders()
        response = connection.getresponse()
        self.assertEqual(response.status, 400)
        response.read()
        connection.close()
        self.assertFalse(self.origin.upload_received.is_set())
        self.assertFalse(self.started.exists())

    def test_stalled_upload_is_bounded_and_not_published(self) -> None:
        connection = self._connection()
        connection.putrequest("PUT", "/api/v1/artifacts")
        connection.putheader("Content-Length", "64")
        connection.endheaders()
        with self.assertRaises(http.client.RemoteDisconnected):
            connection.getresponse()
        connection.close()

        self._wait_for(self.started)
        self.assertTrue(self.origin.upload_received.wait(2))
        self.assertEqual(self.origin.upload_body, b"")
        self.assertFalse(self.origin.upload_complete)
        self.assertFalse(self.published.exists())

    def test_publication_barrier_holds_success_response_until_release(self) -> None:
        result: dict[str, int] = {}
        completed = threading.Event()

        def upload() -> None:
            connection = self._connection()
            connection.request("PUT", "/api/v1/artifacts", body=b"immutable-object")
            response = connection.getresponse()
            result["status"] = response.status
            response.read()
            connection.close()
            completed.set()

        client_thread = threading.Thread(target=upload, daemon=True)
        client_thread.start()
        self._wait_for(self.published)
        self.assertTrue(self.origin.upload_received.wait(2))
        self.assertTrue(self.origin.upload_complete)
        self.assertFalse(self.release.exists())
        self.assertFalse(completed.wait(0.05), "proxy released the response before the barrier")

        self.release.write_text("release\n", encoding="ascii")
        self.assertTrue(completed.wait(2))
        client_thread.join(timeout=2)
        self.assertEqual(result.get("status"), 200)

    def test_rate_limit_that_exceeds_total_deadline_fails_promptly(self) -> None:
        self.proxy.rate_bps = 64 * 1024
        self.proxy.upload_timeout = 0.05
        payload = b"x" * (64 * 1024)

        status, elapsed = self._upload(payload)

        self.assertNotEqual(status, 200)
        self.assertLess(elapsed, 0.75, "rate limiting outlived the upload deadline")
        self.assertTrue(self.origin.upload_received.wait(2))
        self.assertFalse(self.origin.upload_complete)
        self.assertLess(len(self.origin.upload_body), len(payload))
        self.assertFalse(self.published.exists())

    def test_rate_limited_upload_within_deadline_succeeds(self) -> None:
        self.proxy.rate_bps = 64 * 1024
        self.proxy.upload_timeout = 1.5
        self.release.write_text("release\n", encoding="ascii")
        payload = b"y" * (16 * 1024)

        status, elapsed = self._upload(payload)

        self.assertEqual(status, 200)
        self.assertLess(elapsed, self.proxy.upload_timeout)
        self.assertTrue(self.origin.upload_received.wait(2))
        self.assertTrue(self.origin.upload_complete)
        self.assertEqual(self.origin.upload_body, payload)
        self.assertTrue(self.published.exists())

    def test_multiple_rate_limited_chunks_share_one_deadline(self) -> None:
        self.proxy.rate_bps = 640 * 1024
        self.proxy.upload_timeout = 0.25
        payload = b"z" * (192 * 1024)

        status, elapsed = self._upload(payload)

        self.assertNotEqual(status, 200)
        self.assertLess(elapsed, 1.0, "chunk processing reset or exceeded the deadline")
        self.assertTrue(self.origin.upload_received.wait(2))
        self.assertFalse(self.origin.upload_complete)
        self.assertLess(len(self.origin.upload_body), len(payload))
        self.assertFalse(self.published.exists())

    def test_slow_drip_body_reads_share_one_deadline(self) -> None:
        self.proxy.upload_timeout = 0.06
        connection = socket.create_connection(
            ("127.0.0.1", self.proxy.server_port), timeout=2
        )
        started = time.monotonic()
        connection.sendall(
            b"PUT /api/v1/artifacts HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Content-Length: 8\r\n\r\n"
        )
        response = b""
        try:
            for _ in range(8):
                connection.sendall(b"s")
                time.sleep(0.02)
            while True:
                chunk = connection.recv(1024)
                if not chunk:
                    break
                response += chunk
        except OSError:
            pass
        finally:
            connection.close()
        elapsed = time.monotonic() - started

        self.assertFalse(response.startswith(b"HTTP/1.1 200"))
        self.assertLess(elapsed, 0.75)
        self.assertTrue(self.origin.upload_received.wait(2))
        self.assertFalse(self.origin.upload_complete)
        self.assertLess(len(self.origin.upload_body), 8)
        self.assertFalse(self.published.exists())

    def test_rate_limited_upload_near_deadline_can_complete(self) -> None:
        self.proxy.rate_bps = 64 * 1024
        self.proxy.upload_timeout = 0.8
        self.release.write_text("release\n", encoding="ascii")
        payload = b"b" * (32 * 1024)

        status, elapsed = self._upload(payload)

        self.assertEqual(status, 200)
        self.assertLess(elapsed, self.proxy.upload_timeout)
        self.assertTrue(self.origin.upload_received.wait(2))
        self.assertTrue(self.origin.upload_complete)
        self.assertEqual(self.origin.upload_body, payload)

    def test_publication_barrier_wait_obeys_upload_deadline(self) -> None:
        self.proxy.upload_timeout = 0.1
        self.proxy.hold_timeout = 3
        payload = b"published-before-timeout"

        status, elapsed = self._upload(payload)

        self.assertNotEqual(status, 200)
        self.assertLess(elapsed, 0.75)
        self.assertTrue(self.origin.upload_received.wait(2))
        self.assertTrue(self.origin.upload_complete)
        self.assertTrue(self.published.exists())

    def test_non_finite_timeout_limits_are_rejected(self) -> None:
        invalid_limits = (
            ("--upload-timeout", "nan"),
            ("--upload-timeout", "inf"),
            ("--hold-timeout", "nan"),
            ("--hold-timeout", "inf"),
        )
        for option, value in invalid_limits:
            with self.subTest(option=option, value=value):
                argv = [
                    "artifact-chaos-proxy.py",
                    "--listen-port",
                    "0",
                    "--upstream-port",
                    "1",
                    option,
                    value,
                ]
                with patch.object(sys, "argv", argv), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as raised:
                        PROXY.main()
                self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
