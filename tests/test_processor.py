"""Unit tests for the processor: validation, features, alarms, storage,
aggregates, prediction handling and restart recovery.

Run from the repo root:  python -m pytest tests -v
"""

import json
import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "processor"))

import alarms  # noqa: E402
import storage  # noqa: E402
from features import FEATURES, LevelHistory, make_features  # noqa: E402
from predictor import MODEL_MISSING, OK, WARMING_UP, Predictor  # noqa: E402
from processor import Processor  # noqa: E402
from validation import validate_raw  # noqa: E402

NOW = 1_800_000_000.0  # fixed "current time" for deterministic tests


def iso(t):
    return storage.iso(t)


def raw(t=NOW, seq=1, level=50.0, inflow=8.0, outflow=4.0, pump=True, **override):
    d = {"asset_id": "tank_01", "timestamp": iso(t), "seq": seq, "level_pct": level,
         "inflow_lpm": inflow, "outflow_lpm": outflow, "pump_on": pump}
    d.update(override)
    return json.dumps(d).encode()


# ============================================================ validation

def test_valid_message_accepted():
    rec, reason = validate_raw(raw(), "tank_01", now=NOW)
    assert reason is None
    assert rec.level_pct == 50.0 and rec.pump_on is True
    assert rec.ts_epoch == pytest.approx(NOW)


def test_plus_zero_offset_accepted():
    from validation import parse_utc
    ts = "2027-01-15T08:00:00+00:00"
    rec, reason = validate_raw(raw(timestamp=ts), "tank_01", now=parse_utc(ts).timestamp())
    assert reason is None


@pytest.mark.parametrize("payload, fragment", [
    (b"not json", "not valid JSON"),
    (b'{"asset_id": "tank_01", "level_pct": 5', "not valid JSON"),
    (b"[1,2,3]", "not a JSON object"),
    (b"\xff\xfe\x00", "not valid JSON"),
    (raw(level=187.5), "outside valid range"),
    (raw(level=-1), "outside valid range"),
    (raw(inflow=30), "outside valid range"),
    (raw(inflow="fast"), "finite number"),
    (raw(level=True), "finite number"),
    (raw(pump=1), "pump_on"),
    (raw(seq=-3), "seq"),
    (raw(seq=1.5), "seq"),
    (raw(asset_id="tank_99"), "asset_id"),
    (raw(timestamp="yesterday"), "bad timestamp"),
    (raw(timestamp="2027-01-15T08:00:00"), "bad timestamp"),         # no timezone
    (raw(timestamp="2027-01-15T16:00:00+08:00"), "bad timestamp"),   # not UTC
    (raw(t=NOW + 60), "future"),
    (raw(t=NOW - 120), "older than"),
])
def test_invalid_messages_rejected(payload, fragment):
    rec, reason = validate_raw(payload, "tank_01", now=NOW)
    assert rec is None
    assert fragment in reason


def test_missing_field_rejected():
    d = json.loads(raw())
    del d["outflow_lpm"]
    rec, reason = validate_raw(json.dumps(d), "tank_01", now=NOW)
    assert rec is None and "outflow_lpm" in reason


def test_nan_rejected():
    payload = raw().decode().replace("50.0", "NaN").encode()
    rec, reason = validate_raw(payload, "tank_01", now=NOW)
    assert rec is None and "finite" in reason


# ============================================================== features

def test_feature_order_matches_definition():
    row = make_features(50.0, 8.0, 3.0, True, 1.2)
    assert len(row) == len(FEATURES)
    assert dict(zip(FEATURES, row)) == {"level_pct": 50.0, "inflow_lpm": 8.0,
                                        "outflow_lpm": 3.0, "pump_on": 1.0,
                                        "net_flow_lpm": 5.0, "d_level_5s": 1.2}


def test_d_level_needs_five_seconds():
    h = LevelHistory()
    for i in range(5):           # t = 0..4: not enough history yet
        h.add(float(i), 10.0 + i)
        assert h.d_level() is None
    h.add(5.0, 15.0)
    assert h.d_level() == pytest.approx(5.0)   # 15 - 10
    h.add(6.0, 16.0)
    assert h.d_level() == pytest.approx(5.0)   # 16 - 11


def test_gap_resets_history():
    h = LevelHistory()
    for i in range(10):
        h.add(float(i), 50.0)
    assert h.d_level() is not None
    h.add(20.0, 50.0)            # 11 s gap = stream interruption
    assert h.d_level() is None


def test_duplicate_time_ignored():
    h = LevelHistory()
    h.add(1.0, 10.0)
    h.add(1.0, 99.0)
    assert len(h) == 1


# ================================================================ alarms

def test_rule_alarm_with_hysteresis():
    s = alarms.NONE
    s = alarms.RULE.classify(94.9, s); assert s == alarms.NONE
    s = alarms.RULE.classify(95.0, s); assert s == "HIGH_LEVEL"
    s = alarms.RULE.classify(93.5, s); assert s == "HIGH_LEVEL"   # still inside deadband
    s = alarms.RULE.classify(92.9, s); assert s == alarms.NONE
    s = alarms.RULE.classify(9.9, s);  assert s == "LOW_LEVEL"
    s = alarms.RULE.classify(11.9, s); assert s == "LOW_LEVEL"
    s = alarms.RULE.classify(12.1, s); assert s == alarms.NONE


def test_ai_alarm_none_without_prediction():
    assert alarms.AI.classify(None, "OVERFLOW_RISK") == alarms.NONE
    assert alarms.AI.classify(91.0, alarms.NONE) == "OVERFLOW_RISK"
    assert alarms.AI.classify(14.0, alarms.NONE) == "LOW_LEVEL_RISK"


# ======================================================= processor core

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "tank.db")


def feed(proc, levels, start=NOW, inflow=8.0, outflow=4.0, seq0=0):
    out = []
    for i, lv in enumerate(levels):
        t = start + i
        out.append(proc.handle("tank/tank_01/raw",
                               raw(t=t, seq=seq0 + i, level=lv, inflow=inflow, outflow=outflow),
                               received_at=t + 0.05))
    return out


def test_state_message_fields(db_path):
    p = Processor("tank_01", db_path, "/nonexistent/model.joblib")
    kind, state = feed(p, [50.0])[0]
    assert kind == "state"
    for key in ("asset_id", "timestamp", "seq", "level_pct", "inflow_lpm", "outflow_lpm",
                "pump_on", "pred_level_30s_pct", "model_status", "rule_alarm", "ai_alarm",
                "level_mean_60s_pct", "level_max_10min_pct", "net_flow_lpm",
                "alarms_last_hour", "processed_at"):
        assert key in state, key
    assert state["net_flow_lpm"] == pytest.approx(4.0)
    json.dumps(state)


def test_no_model_means_no_prediction(db_path):
    p = Processor("tank_01", db_path, "/nonexistent/model.joblib")
    results = feed(p, [50.0 + i * 0.1 for i in range(8)])
    statuses = [s["model_status"] for _, s in results]
    assert statuses[:5] == [WARMING_UP] * 5      # < 5 s of history
    assert statuses[-1] == MODEL_MISSING
    assert all(s["pred_level_30s_pct"] is None for _, s in results)
    assert all(s["ai_alarm"] == alarms.NONE for _, s in results)


def test_rejected_message_stored_not_crashing(db_path):
    p = Processor("tank_01", db_path, "/nonexistent")
    kind, body = p.handle("tank/tank_01/raw", b"garbage", received_at=NOW)
    assert kind == "rejected" and "JSON" in body["reason"]
    rows = p.db.conn.execute("SELECT reason, payload FROM rejected").fetchall()
    assert len(rows) == 1 and rows[0]["payload"] == "garbage"
    assert p.db.conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 0


def test_duplicate_and_out_of_order_rejected(db_path):
    p = Processor("tank_01", db_path, "/nonexistent")
    assert p.handle("t", raw(t=NOW, seq=5), received_at=NOW)[0] == "state"
    kind, body = p.handle("t", raw(t=NOW, seq=5), received_at=NOW + 0.1)
    assert kind == "rejected" and "duplicate" in body["reason"]
    kind, body = p.handle("t", raw(t=NOW - 3, seq=2), received_at=NOW + 0.2)
    assert kind == "rejected" and "out-of-order" in body["reason"]
    # A restarted simulator starts at seq 0 again but with a newer timestamp: accepted.
    assert p.handle("t", raw(t=NOW + 1, seq=0), received_at=NOW + 1)[0] == "state"


def test_aggregates(db_path):
    p = Processor("tank_01", db_path, "/nonexistent")
    levels = [40.0 + i for i in range(10)]       # 40..49 over 10 s
    _, last = feed(p, levels)[-1]
    assert last["level_mean_60s_pct"] == pytest.approx(44.5)
    assert last["level_max_10min_pct"] == 49.0
    assert last["level_min_10min_pct"] == 40.0
    assert last["pumped_volume_l_last_hour"] == pytest.approx(10 * 8.0 / 60, abs=0.1)


def test_rule_alarm_episode_recorded(db_path):
    p = Processor("tank_01", db_path, "/nonexistent")
    levels = [90, 94, 96, 97, 96, 94, 92, 90]    # one HIGH_LEVEL episode
    results = feed(p, levels)
    assert [s["rule_alarm"] for _, s in results] == [
        "NONE", "NONE", "HIGH_LEVEL", "HIGH_LEVEL", "HIGH_LEVEL", "HIGH_LEVEL", "NONE", "NONE"]
    assert results[-1][1]["alarms_last_hour"] == 1
    rows = p.db.conn.execute("SELECT * FROM alarms").fetchall()
    assert len(rows) == 1
    assert rows[0]["alarm"] == "HIGH_LEVEL" and rows[0]["ended_ts"] == pytest.approx(NOW + 6)


def test_restart_recovers_history_and_alarms(db_path):
    p1 = Processor("tank_01", db_path, "/nonexistent")
    feed(p1, [95.0 + i * 0.5 for i in range(8)])  # alarm active at shutdown
    p1.db.close()

    p2 = Processor("tank_01", db_path, "/nonexistent")
    assert p2.alarm_state["rule"] == "HIGH_LEVEL"
    assert p2.last_ts_epoch == pytest.approx(NOW + 7)
    # History was restored, so d_level is available immediately (no warm-up).
    kind, state = p2.handle("t", raw(t=NOW + 8, seq=8, level=99.0), received_at=NOW + 8)
    assert state["model_status"] == MODEL_MISSING
    # Old messages from before the restart are still rejected.
    assert p2.handle("t", raw(t=NOW + 2, seq=2), received_at=NOW + 9)[0] == "rejected"
    # History query sees data written before and after the restart.
    conn = storage.connect(db_path, readonly=True)
    rows = conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    assert rows == 9


def test_backup_creates_consistent_copy(db_path, tmp_path):
    p = Processor("tank_01", db_path, "/nonexistent")
    feed(p, [50.0] * 5)
    bdir = tmp_path / "backups"
    paths = [storage.backup(db_path, str(bdir), keep=2) for _ in range(3)]
    time.sleep(0.01)
    kept = sorted(os.listdir(bdir))
    assert len(kept) <= 2
    import sqlite3
    with sqlite3.connect(paths[-1]) as c:
        assert c.execute("SELECT COUNT(*) FROM readings").fetchone()[0] == 5


# ===================================================== with a real model

@pytest.fixture
def model_path(tmp_path):
    """A tiny stand-in model so the prediction path can be tested before
    the real model is trained (the real one comes from ai/train.py)."""
    import joblib
    import numpy as np
    from sklearn.linear_model import LinearRegression
    rng = np.random.default_rng(0)
    X = rng.uniform([0, 0, 0, 0, -25, -5], [100, 25, 25, 1, 25, 5], size=(500, 6))
    y = X[:, 0] + X[:, 4] * 30 / 60 / 50 * 100           # physics-like target
    m = LinearRegression().fit(X, y)
    path = tmp_path / "level_model.joblib"
    joblib.dump({"model": m, "features": list(FEATURES), "horizon_s": 30,
                 "version": "test-1"}, path)
    return str(path)


def test_prediction_and_ai_alarm(db_path, model_path):
    p = Processor("tank_01", db_path, model_path)
    # Rising fast with high net inflow -> predicted overflow before it happens.
    results = feed(p, [80 + i * 0.4 for i in range(10)], inflow=15.0, outflow=3.0)
    last = results[-1][1]
    assert last["model_status"] == OK
    assert last["model_version"] == "test-1"
    assert last["pred_level_30s_pct"] > last["level_pct"]
    assert last["ai_alarm"] == "OVERFLOW_RISK"
    assert last["rule_alarm"] == "NONE"          # current level still below 95 %


def test_predictor_rejects_wrong_feature_list(tmp_path):
    import joblib
    path = tmp_path / "bad.joblib"
    joblib.dump({"model": object(), "features": ["level_pct"], "version": "x"}, path)
    pr = Predictor(str(path))
    assert pr.model is None
    pred, status = pr.predict([1, 2, 3, 4, 5, 6])
    assert pred is None and status == "MODEL_ERROR"
