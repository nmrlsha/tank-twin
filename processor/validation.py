"""Validation of raw state messages (schema: docs/planning.md section 5.1).

validate_raw() never raises. It returns (record, None) for a good message or
(None, reason) for a bad one, so a malformed message can never crash the
processor.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone

LEVEL_RANGE = (0.0, 100.0)    # %
FLOW_RANGE = (0.0, 25.0)      # L/min
MAX_FUTURE_S = 5.0            # allowed clock skew
MAX_AGE_S = 60.0              # older messages are rejected as stale

REQUIRED = ("asset_id", "timestamp", "seq", "level_pct", "inflow_lpm",
            "outflow_lpm", "pump_on")


@dataclass(frozen=True)
class RawRecord:
    asset_id: str
    timestamp: str      # ISO 8601 UTC, as received
    ts_epoch: float     # same instant, seconds since 1970 (UTC)
    seq: int
    level_pct: float
    inflow_lpm: float
    outflow_lpm: float
    pump_on: bool


def _is_number(v) -> bool:
    # bool is a subclass of int in Python; reject it explicitly.
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def parse_utc(ts: str) -> datetime:
    """Parse an ISO 8601 timestamp that must be in UTC ('Z' or '+00:00')."""
    dt = datetime.fromisoformat(ts)  # Python 3.11+ accepts the 'Z' suffix
    if dt.tzinfo is None or dt.utcoffset().total_seconds() != 0:
        raise ValueError("timestamp must be UTC")
    return dt


def validate_raw(payload: bytes | str, expected_asset: str,
                 now: float | None = None) -> tuple[RawRecord | None, str | None]:
    if now is None:
        now = datetime.now(timezone.utc).timestamp()

    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, "not valid JSON"
    if not isinstance(data, dict):
        return None, "message is not a JSON object"

    missing = [k for k in REQUIRED if k not in data]
    if missing:
        return None, f"missing field(s): {', '.join(missing)}"

    if data["asset_id"] != expected_asset:
        return None, f"unexpected asset_id {data['asset_id']!r}"

    if not isinstance(data["timestamp"], str):
        return None, "timestamp must be a string"
    try:
        ts_epoch = parse_utc(data["timestamp"]).timestamp()
    except ValueError:
        return None, f"bad timestamp {data['timestamp']!r} (need ISO 8601 UTC)"
    if ts_epoch > now + MAX_FUTURE_S:
        return None, "timestamp is in the future"
    if ts_epoch < now - MAX_AGE_S:
        return None, f"timestamp older than {MAX_AGE_S:.0f} s"

    seq = data["seq"]
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
        return None, "seq must be a non-negative integer"

    for key, (lo, hi) in (("level_pct", LEVEL_RANGE),
                          ("inflow_lpm", FLOW_RANGE),
                          ("outflow_lpm", FLOW_RANGE)):
        v = data[key]
        if not _is_number(v):
            return None, f"{key} must be a finite number"
        if not lo <= v <= hi:
            return None, f"{key}={v} outside valid range {lo}-{hi}"

    if not isinstance(data["pump_on"], bool):
        return None, "pump_on must be true or false"

    return RawRecord(
        asset_id=data["asset_id"],
        timestamp=data["timestamp"],
        ts_epoch=ts_epoch,
        seq=seq,
        level_pct=float(data["level_pct"]),
        inflow_lpm=float(data["inflow_lpm"]),
        outflow_lpm=float(data["outflow_lpm"]),
        pump_on=data["pump_on"],
    ), None
