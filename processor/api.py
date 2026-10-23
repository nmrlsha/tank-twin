"""Read-only HTTP API over the stored data (stdlib only, no web framework).

GET /health                     service status
GET /history?minutes=10         stored readings, oldest first (max 1440 min)
GET /aggregates                 aggregates at the latest reading
GET /alarms?hours=24            alarm episodes
GET /rejected?limit=50          most recent rejected messages
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import storage

log = logging.getLogger("api")


class _BadRequest(Exception):
    pass


def _num(params: dict, key: str, default: float, lo: float, hi: float) -> float:
    raw = params.get(key, [None])[0]
    if raw is None:
        return default
    try:
        v = float(raw)
    except ValueError:
        raise _BadRequest(f"{key} must be a number") from None
    if not lo <= v <= hi:
        raise _BadRequest(f"{key} must be between {lo:g} and {hi:g}")
    return v


def make_handler(db_path: str, asset_id: str, status_fn):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # route access log to logging
            log.debug("%s - %s", self.address_string(), fmt % args)

        def _send(self, code: int, body) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):  # noqa: N802 - name required by BaseHTTPRequestHandler
            url = urlparse(self.path)
            params = parse_qs(url.query)
            try:
                if url.path == "/health":
                    return self._send(200, status_fn())
                conn = storage.connect(db_path, readonly=True)
                try:
                    if url.path == "/history":
                        minutes = _num(params, "minutes", 10, 0.1, 1440)
                        limit = int(_num(params, "limit", 100000, 1, 100000))
                        rows = storage.history(conn, asset_id, minutes, limit)
                        return self._send(200, {"asset_id": asset_id, "minutes": minutes,
                                                "count": len(rows), "readings": rows})
                    if url.path == "/aggregates":
                        last = conn.execute("SELECT MAX(ts_epoch) FROM readings WHERE asset_id=?",
                                            (asset_id,)).fetchone()[0]
                        at = last if last is not None else time.time()
                        return self._send(200, {"asset_id": asset_id, "at": storage.iso(at),
                                                **storage.aggregates(conn, asset_id, at)})
                    if url.path == "/alarms":
                        hours = _num(params, "hours", 24, 0.01, 24 * 30)
                        return self._send(200, {"asset_id": asset_id,
                                                "alarms": storage.alarm_list(conn, asset_id, hours)})
                    if url.path == "/rejected":
                        limit = int(_num(params, "limit", 50, 1, 1000))
                        return self._send(200, {"rejected": storage.rejected_list(conn, limit)})
                    return self._send(404, {"error": f"unknown path {url.path}"})
                finally:
                    conn.close()
            except _BadRequest as exc:
                return self._send(400, {"error": str(exc)})
            except sqlite3.Error as exc:
                log.error("database error: %s", exc)
                return self._send(503, {"error": "database unavailable"})

    return Handler


def start_api(host: str, port: int, db_path: str, asset_id: str, status_fn) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), make_handler(db_path, asset_id, status_fn))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="http-api", daemon=True).start()
    log.info("HTTP API on http://%s:%d", host, port)
    return server
