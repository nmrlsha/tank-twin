"""Unit tests for the simulator physics, message schema and control parsing.

Run from the repo root:  python -m pytest tests -v
"""

import json
import random
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))

from simulator import (  # noqa: E402
    SCENARIOS, TankModel, build_message, make_invalid_payload, parse_control,
)

ISO_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
FIELDS = {"asset_id", "timestamp", "seq", "level_pct", "inflow_lpm",
          "outflow_lpm", "pump_on"}


def run(model: TankModel, seconds: int) -> list[float]:
    levels = []
    for _ in range(seconds):
        model.step(1.0)
        levels.append(model.level_pct)
    return levels


def make_model(scenario="normal", level=50.0, seed=1) -> TankModel:
    m = TankModel(level_pct=level, rng=random.Random(seed))
    m.set_scenario(scenario)
    return m


# ---------------------------------------------------------------- physics

def test_fill_raises_level():
    m = make_model("fill", level=20.0)
    levels = run(m, 120)
    assert levels[-1] > 20.0 + 15.0  # ~+6 L/min net for 2 min ≈ +24 %
    assert m.pump_on is True


def test_drain_lowers_level():
    m = make_model("drain", level=80.0)
    levels = run(m, 120)
    assert levels[-1] < 80.0 - 15.0
    assert m.pump_on is False


def test_level_stays_within_physical_limits():
    for scenario in ("abnormal_inflow", "drain"):
        m = make_model(scenario, level=50.0)
        levels = run(m, 1800)
        assert all(0.0 <= lv <= 100.0 for lv in levels)


def test_abnormal_inflow_reaches_overflow():
    m = make_model("abnormal_inflow", level=50.0)
    levels = run(m, 600)
    assert max(levels) == pytest.approx(100.0)


def test_normal_hysteresis_keeps_level_in_band():
    m = make_model("normal", level=50.0)
    levels = run(m, 3600)
    # After settling, level cycles roughly between 30 % and 80 %.
    settled = levels[300:]
    assert min(settled) > 20.0
    assert max(settled) < 90.0


def test_pump_failure_reports_on_but_level_falls():
    m = make_model("pump_failure", level=70.0)
    levels = run(m, 180)
    assert m.pump_on is True
    assert levels[-1] < 70.0 - 10.0


def test_unknown_scenario_rejected():
    m = make_model()
    with pytest.raises(ValueError):
        m.set_scenario("explode")


def test_same_seed_is_reproducible():
    a, b = make_model(seed=42), make_model(seed=42)
    assert run(a, 200) == run(b, 200)


# --------------------------------------------------------------- messages

def test_message_matches_schema():
    m = make_model()
    m.step()
    msg = build_message("tank_01", 7, m.measure())
    assert set(msg) == FIELDS
    assert msg["asset_id"] == "tank_01"
    assert msg["seq"] == 7
    assert ISO_UTC.match(msg["timestamp"])
    assert isinstance(msg["pump_on"], bool)
    assert 0.0 <= msg["level_pct"] <= 100.0
    assert 0.0 <= msg["inflow_lpm"] <= 25.0
    assert 0.0 <= msg["outflow_lpm"] <= 25.0
    json.dumps(msg)  # must be serialisable


def test_measurements_always_in_valid_range():
    for scenario in SCENARIOS:
        m = make_model(scenario, level=50.0)
        for _ in range(900):
            m.step()
            r = m.measure()
            assert 0.0 <= r["level_pct"] <= 100.0
            assert 0.0 <= r["inflow_lpm"] <= 25.0
            assert 0.0 <= r["outflow_lpm"] <= 25.0


def is_valid(payload: str) -> bool:
    """Minimal validator mirroring the schema; the processor will own the real one."""
    try:
        d = json.loads(payload)
    except json.JSONDecodeError:
        return False
    if not isinstance(d, dict) or set(d) != FIELDS:
        return False
    for k in ("level_pct", "inflow_lpm", "outflow_lpm"):
        if not isinstance(d[k], (int, float)) or isinstance(d[k], bool):
            return False
    return (0 <= d["level_pct"] <= 100 and 0 <= d["inflow_lpm"] <= 25
            and 0 <= d["outflow_lpm"] <= 25)


def test_injected_messages_are_invalid():
    rng = random.Random(0)
    good = build_message("tank_01", 1, make_model().measure())
    assert is_valid(json.dumps(good))
    for _ in range(50):
        assert not is_valid(make_invalid_payload(good, rng))


# ---------------------------------------------------------------- control

def test_control_accepts_valid_scenario():
    assert parse_control(b'{"scenario": "drain"}') == {"scenario": "drain"}
    assert parse_control('{"inject_invalid": true}') == {"inject_invalid": True}


@pytest.mark.parametrize("payload", [
    b"not json",
    b"[1, 2]",
    b'{"scenario": "explode"}',
    b'{"inject_invalid": "yes"}',
    b"{}",
    b"\xff\xfe",
])
def test_control_rejects_bad_input(payload):
    with pytest.raises(ValueError):
        parse_control(payload)
