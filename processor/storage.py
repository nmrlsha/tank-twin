"""SQLite storage for raw readings, AI outputs, alarms and rejected messages.

Only the processor's worker thread writes. The HTTP API and the backup job
open their own connections; WAL mode lets them read while the worker writes.
"""

from __future__ import annotations

import glob
import os
import sqlite3
import statistics
import time
from datetime import datetime, timezone

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    id                  INTEGER PRIMARY KEY,
    asset_id            TEXT    NOT NULL,
    ts                  TEXT    NOT NULL,   -- measurement time, ISO 8601 UTC
    ts_epoch            REAL    NOT NULL,   -- same, seconds since epoch
    seq                 INTEGER NOT NULL,
    level_pct           REAL    NOT NULL,
    inflow_lpm          REAL    NOT NULL,
    outflow_lpm         REAL    NOT NULL,
    pump_on             INTEGER NOT NULL,   -- 0 / 1
    net_flow_lpm        REAL    NOT NULL,
    d_level_5s          REAL,               -- NULL while warming up
    pred_level_30s_pct  REAL,               -- NULL if no prediction
    model_status        TEXT    NOT NULL,
    model_version       TEXT,
    rule_alarm          TEXT    NOT NULL,
    ai_alarm            TEXT    NOT NULL,
    received_at         REAL    NOT NULL,   -- epoch, when the processor got it
    processed_at        REAL    NOT NULL,   -- epoch, when processing finished
    pipeline_delay_ms   REAL    NOT NULL    -- processed_at - ts_epoch
);
CREATE INDEX IF NOT EXISTS idx_readings_asset_time ON readings (asset_id, ts_epoch);

CREATE TABLE IF NOT EXISTS alarms (
    id           INTEGER PRIMARY KEY,
    asset_id     TEXT NOT NULL,
    source       TEXT NOT NULL,     -- 'rule' or 'ai'
    alarm        TEXT NOT NULL,     -- e.g. HIGH_LEVEL, OVERFLOW_RISK
    started_ts   REAL NOT NULL,     -- epoch of the reading that raised it
    ended_ts     REAL,              -- NULL while still active
    start_level  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alarms_asset_time ON alarms (asset_id, started_ts);

CREATE TABLE IF NOT EXISTS rejected (
    id           INTEGER PRIMARY KEY,
    received_at  REAL NOT NULL,
    topic        TEXT NOT NULL,
    payload      TEXT NOT NULL,
    reason       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rejected_time ON rejected (received_at);
"""

HISTORY_COLUMNS = ("ts", "ts_epoch", "seq", "level_pct", "inflow_lpm", "outflow_lpm",
                   "pump_on", "net_flow_lpm", "pred_level_30s_pct", "model_status",
                   "rule_alarm", "ai_alarm", "pipeline_delay_ms")


def iso(epoch: float) -> str:
    return (datetime.fromtimestamp(epoch, timezone.utc)
            .isoformat(timespec="milliseconds").replace("+00:00", "Z"))


def connect(path: str, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    else:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        # The writer connection is created at start-up (main thread) and then
        # used only by the worker thread, never by two threads at once.
        conn = sqlite3.connect(path, timeout=5, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


class Storage:
    def __init__(self, path: str):
        self.path = path
        self.conn = connect(path)
        self.conn.executescript(SCHEMA)
        self.conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        self.conn.commit()

    def close(self):
        self.conn.close()

    # ------------------------------------------------------------- writes

    def insert_reading(self, row: dict) -> None:
        cols = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        self.conn.execute(f"INSERT INTO readings ({cols}) VALUES ({marks})",
                          tuple(row.values()))
        self.conn.commit()

    def insert_rejected(self, topic: str, payload: str, reason: str,
                        received_at: float | None = None) -> None:
        self.conn.execute(
            "INSERT INTO rejected (received_at, topic, payload, reason) VALUES (?, ?, ?, ?)",
            (received_at or time.time(), topic, payload[:2000], reason))
        self.conn.commit()

    def open_alarm(self, asset_id: str, source: str, alarm: str,
                   ts_epoch: float, level: float) -> None:
        self.conn.execute(
            "INSERT INTO alarms (asset_id, source, alarm, started_ts, start_level) "
            "VALUES (?, ?, ?, ?, ?)", (asset_id, source, alarm, ts_epoch, level))
        self.conn.commit()

    def close_alarms(self, asset_id: str, source: str, ts_epoch: float) -> None:
        self.conn.execute(
            "UPDATE alarms SET ended_ts = ? WHERE asset_id = ? AND source = ? AND ended_ts IS NULL",
            (ts_epoch, asset_id, source))
        self.conn.commit()

    # -------------------------------------------------------------- reads

    def active_alarms(self, asset_id: str) -> dict[str, str]:
        """{'rule': alarm, 'ai': alarm} for alarms still open (restart recovery)."""
        rows = self.conn.execute(
            "SELECT source, alarm FROM alarms WHERE asset_id = ? AND ended_ts IS NULL "
            "ORDER BY started_ts", (asset_id,)).fetchall()
        return {r["source"]: r["alarm"] for r in rows}

    def recent_levels(self, asset_id: str, seconds: float, now: float) -> list[tuple[float, float]]:
        """(ts_epoch, level) of recent readings, oldest first (restart recovery)."""
        rows = self.conn.execute(
            "SELECT ts_epoch, level_pct FROM readings WHERE asset_id = ? AND ts_epoch > ? "
            "ORDER BY ts_epoch", (asset_id, now - seconds)).fetchall()
        return [(r[0], r[1]) for r in rows]

    def last_reading(self, asset_id: str):
        return self.conn.execute(
            "SELECT * FROM readings WHERE asset_id = ? ORDER BY ts_epoch DESC LIMIT 1",
            (asset_id,)).fetchone()


# ------------------------------------------------------------------ aggregates
# Plain functions taking a connection, so the HTTP API can call them on its
# own read-only connection.

def aggregates(conn: sqlite3.Connection, asset_id: str, at: float) -> dict:
    """Aggregates over windows ending at time `at` (epoch seconds)."""
    q = conn.execute
    a = asset_id
    mean_60s = q("SELECT AVG(level_pct) FROM readings WHERE asset_id=? AND ts_epoch>? AND ts_epoch<=?",
                 (a, at - 60, at)).fetchone()[0]
    mx, mn = q("SELECT MAX(level_pct), MIN(level_pct) FROM readings "
               "WHERE asset_id=? AND ts_epoch>? AND ts_epoch<=?", (a, at - 600, at)).fetchone()
    count_60s = q("SELECT COUNT(*) FROM readings WHERE asset_id=? AND ts_epoch>? AND ts_epoch<=?",
                  (a, at - 60, at)).fetchone()[0]
    # Volume pumped in: each 1 Hz sample covers 1 s, so litres = sum(L/min) / 60.
    pumped = q("SELECT SUM(inflow_lpm) / 60.0 FROM readings WHERE asset_id=? AND ts_epoch>? AND ts_epoch<=?",
               (a, at - 3600, at)).fetchone()[0]
    alarms_1h = q("SELECT COUNT(*) FROM alarms WHERE asset_id=? AND started_ts>? AND started_ts<=?",
                  (a, at - 3600, at)).fetchone()[0]
    rejected_1h = q("SELECT COUNT(*) FROM rejected WHERE received_at>?",
                    (at - 3600,)).fetchone()[0]
    delays = [r[0] for r in q("SELECT pipeline_delay_ms FROM readings WHERE asset_id=? "
                              "AND ts_epoch>? AND ts_epoch<=?", (a, at - 60, at))]
    rnd = lambda v, n=2: None if v is None else round(v, n)  # noqa: E731
    return {
        "level_mean_60s_pct": rnd(mean_60s),
        "level_max_10min_pct": rnd(mx),
        "level_min_10min_pct": rnd(mn),
        "readings_last_60s": count_60s,
        "pumped_volume_l_last_hour": rnd(pumped or 0.0, 1),
        "alarms_last_hour": alarms_1h,
        "rejected_last_hour": rejected_1h,
        "pipeline_delay_ms_median_60s": rnd(statistics.median(delays), 1) if delays else None,
        "pipeline_delay_ms_max_60s": rnd(max(delays), 1) if delays else None,
    }


def history(conn: sqlite3.Connection, asset_id: str, minutes: float, limit: int) -> list[dict]:
    since = time.time() - minutes * 60
    rows = conn.execute(
        f"SELECT {', '.join(HISTORY_COLUMNS)} FROM readings WHERE asset_id=? AND ts_epoch>? "
        f"ORDER BY ts_epoch DESC LIMIT ?", (asset_id, since, limit)).fetchall()
    out = [dict(r) for r in reversed(rows)]  # oldest first
    for r in out:
        r["pump_on"] = bool(r["pump_on"])
    return out


def alarm_list(conn: sqlite3.Connection, asset_id: str, hours: float) -> list[dict]:
    rows = conn.execute(
        "SELECT source, alarm, started_ts, ended_ts, start_level FROM alarms "
        "WHERE asset_id=? AND started_ts>? ORDER BY started_ts",
        (asset_id, time.time() - hours * 3600)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["started"] = iso(d["started_ts"])
        d["ended"] = iso(d["ended_ts"]) if d["ended_ts"] else None
        d["duration_s"] = round(d["ended_ts"] - d["started_ts"], 1) if d["ended_ts"] else None
        out.append(d)
    return out


def rejected_list(conn: sqlite3.Connection, limit: int) -> list[dict]:
    rows = conn.execute("SELECT received_at, topic, payload, reason FROM rejected "
                        "ORDER BY received_at DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r, received=iso(r["received_at"])) for r in rows]


# ---------------------------------------------------------------------- backup

def backup(db_path: str, backup_dir: str, keep: int = 6) -> str:
    """Consistent online copy using SQLite's backup API; keeps the newest `keep`."""
    os.makedirs(backup_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest_path = os.path.join(backup_dir, f"tank-{stamp}.db")
    src = sqlite3.connect(db_path, timeout=10)
    dst = sqlite3.connect(dest_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    for old in sorted(glob.glob(os.path.join(backup_dir, "tank-*.db")))[:-keep]:
        os.remove(old)
    return dest_path
