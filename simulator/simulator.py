"""
Water tank and pump simulator for the tank_01 digital twin.

Publishes one raw state message per second to MQTT topic tank/<asset_id>/raw,
following the schema in docs/planning.md (section 5.1).

The physics lives in TankModel, which has no MQTT code so it can be unit
tested and reused offline to generate AI training data.

Run locally (broker on localhost:1883):
    python simulator.py
Print messages without a broker:
    python simulator.py --dry-run --duration 10
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

SCENARIOS = ("normal", "fill", "drain", "abnormal_inflow", "pump_failure")

log = logging.getLogger("simulator")


# --------------------------------------------------------------------------
# Physics model
# --------------------------------------------------------------------------

@dataclass
class TankConfig:
    capacity_l: float = 50.0          # tank volume at 100 % level
    pump_flow_lpm: float = 8.0        # nominal pump inflow
    abnormal_flow_lpm: float = 15.0   # inflow during abnormal_inflow scenario
    pump_on_below_pct: float = 30.0   # normal mode: pump switches on below this
    pump_off_above_pct: float = 80.0  # normal mode: pump switches off above this
    level_noise_pct: float = 0.3      # sensor noise (std dev), level
    flow_noise_lpm: float = 0.1       # sensor noise (std dev), flows


# Demand (outflow) range in L/min for each scenario.
DEMAND_RANGE = {
    "normal": (2.0, 6.0),
    "fill": (1.0, 2.0),
    "drain": (4.0, 8.0),
    "abnormal_inflow": (2.0, 4.0),
    "pump_failure": (3.0, 6.0),
}


@dataclass
class TankModel:
    """True (noise-free) tank state plus the rules that move it forward."""

    config: TankConfig = field(default_factory=TankConfig)
    level_pct: float = 50.0
    pump_on: bool = False
    inflow_lpm: float = 0.0
    outflow_lpm: float = 4.0
    scenario: str = "normal"
    rng: random.Random = field(default_factory=random.Random)

    def set_scenario(self, scenario: str) -> None:
        if scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario: {scenario!r}")
        self.scenario = scenario

    def _update_demand(self) -> None:
        """Outflow drifts slowly inside the scenario's demand range."""
        lo, hi = DEMAND_RANGE[self.scenario]
        value = self.outflow_lpm + self.rng.gauss(0.0, 0.15)
        # After a scenario change the demand may be outside the new range;
        # move it there smoothly (20 % of the gap per second).
        if value < lo:
            value += 0.2 * (lo - value)
        elif value > hi:
            value -= 0.2 * (value - hi)
        self.outflow_lpm = max(0.0, value)

    def _update_pump_and_inflow(self) -> None:
        cfg = self.config
        s = self.scenario
        if s == "normal":
            # Hysteresis controller.
            if self.level_pct < cfg.pump_on_below_pct:
                self.pump_on = True
            elif self.level_pct > cfg.pump_off_above_pct:
                self.pump_on = False
            target_in = cfg.pump_flow_lpm if self.pump_on else 0.0
        elif s == "fill":
            self.pump_on = True
            target_in = cfg.pump_flow_lpm
        elif s == "drain":
            self.pump_on = False
            target_in = 0.0
        elif s == "abnormal_inflow":
            # Control failure: pump stuck on and inflow far above nominal.
            self.pump_on = True
            target_in = cfg.abnormal_flow_lpm
        else:  # pump_failure
            # Pump reports running but delivers (almost) nothing.
            self.pump_on = True
            target_in = 0.2
        # First-order response so flows change smoothly, not instantly.
        self.inflow_lpm += 0.3 * (target_in - self.inflow_lpm)

    def step(self, dt_s: float = 1.0) -> None:
        """Advance the true state by dt_s seconds."""
        self._update_pump_and_inflow()
        self._update_demand()
        # Tank cannot deliver water it does not have.
        if self.level_pct <= 0.0:
            self.outflow_lpm = min(self.outflow_lpm, self.inflow_lpm)
        net_lpm = self.inflow_lpm - self.outflow_lpm
        delta_pct = net_lpm / 60.0 * dt_s / self.config.capacity_l * 100.0
        # Above 100 % the tank overflows; level is capped at the rim.
        self.level_pct = min(100.0, max(0.0, self.level_pct + delta_pct))

    def measure(self) -> dict:
        """Sensor readings: true state plus noise, clipped to physical limits."""
        cfg = self.config
        level = self.level_pct + self.rng.gauss(0.0, cfg.level_noise_pct)
        inflow = self.inflow_lpm + self.rng.gauss(0.0, cfg.flow_noise_lpm)
        outflow = self.outflow_lpm + self.rng.gauss(0.0, cfg.flow_noise_lpm)
        return {
            "level_pct": round(min(100.0, max(0.0, level)), 2),
            "inflow_lpm": round(max(0.0, inflow), 2),
            "outflow_lpm": round(max(0.0, outflow), 2),
            "pump_on": self.pump_on,
        }


# --------------------------------------------------------------------------
# Messages
# --------------------------------------------------------------------------

def utc_now_iso() -> str:
    """Current UTC time as ISO 8601 with millisecond precision and a Z suffix."""
    return (datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"))


def build_message(asset_id: str, seq: int, reading: dict, timestamp: str | None = None) -> dict:
    """Raw state message, schema in docs/planning.md section 5.1."""
    return {
        "asset_id": asset_id,
        "timestamp": timestamp or utc_now_iso(),
        "seq": seq,
        "level_pct": reading["level_pct"],
        "inflow_lpm": reading["inflow_lpm"],
        "outflow_lpm": reading["outflow_lpm"],
        "pump_on": reading["pump_on"],
    }


def make_invalid_payload(valid: dict, rng: random.Random) -> str:
    """One deliberately bad message, for the invalid-input test scenario."""
    kind = rng.choice(["not_json", "missing_field", "out_of_range", "wrong_type"])
    bad = dict(valid)
    if kind == "not_json":
        return '{"asset_id": "' + valid["asset_id"] + '", "level_pct": 5'  # truncated
    if kind == "missing_field":
        bad.pop("level_pct")
    elif kind == "out_of_range":
        bad["level_pct"] = 187.5
    else:  # wrong_type
        bad["inflow_lpm"] = "fast"
    return json.dumps(bad)


def parse_control(payload: bytes | str) -> dict:
    """Validate a control message. Returns a dict with only the accepted keys.

    Raises ValueError for anything malformed, so a bad control message can
    never crash or confuse the simulator.
    """
    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"control message is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("control message must be a JSON object")
    out = {}
    if "scenario" in data:
        if data["scenario"] not in SCENARIOS:
            raise ValueError(f"unknown scenario: {data['scenario']!r}")
        out["scenario"] = data["scenario"]
    if "inject_invalid" in data:
        if not isinstance(data["inject_invalid"], bool):
            raise ValueError("inject_invalid must be true or false")
        out["inject_invalid"] = data["inject_invalid"]
    if not out:
        raise ValueError("control message has no recognised fields")
    return out


# --------------------------------------------------------------------------
# MQTT service
# --------------------------------------------------------------------------

class SimulatorService:
    def __init__(self, host: str, port: int, asset_id: str, rate_hz: float,
                 scenario: str, seed: int | None, dry_run: bool = False):
        self.asset_id = asset_id
        self.period_s = 1.0 / rate_hz
        self.dry_run = dry_run
        self.rng = random.Random(seed)
        self.model = TankModel(rng=self.rng)
        self.model.set_scenario(scenario)
        self.seq = 0
        self._inject_invalid = False
        self._lock = threading.Lock()
        self._stop = threading.Event()

        base = f"tank/{asset_id}"
        self.topic_raw = f"{base}/raw"
        self.topic_status = f"{base}/status"
        self.topic_control = f"{base}/sim/control"

        self.client = None
        if not dry_run:
            import paho.mqtt.client as mqtt
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                      client_id=f"simulator-{asset_id}")
            # If we die without saying goodbye, the broker announces it for us.
            self.client.will_set(self.topic_status, "offline", qos=1, retain=True)
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message
            self.client.reconnect_delay_set(min_delay=1, max_delay=10)
            self.client.connect_async(host, port, keepalive=10)

    # MQTT callbacks run on paho's network thread.
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("connect failed: %s", reason_code)
            return
        log.info("connected to broker")
        client.publish(self.topic_status, "online", qos=1, retain=True)
        client.subscribe(self.topic_control, qos=1)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        log.warning("disconnected from broker (%s); will retry", reason_code)

    def _on_message(self, client, userdata, msg):
        try:
            control = parse_control(msg.payload)
        except ValueError as exc:
            log.warning("ignored control message: %s", exc)
            return
        with self._lock:
            if "scenario" in control:
                self.model.set_scenario(control["scenario"])
                log.info("scenario -> %s", control["scenario"])
            if control.get("inject_invalid"):
                self._inject_invalid = True
                log.info("will inject one invalid message")

    def stop(self, *_):
        self._stop.set()

    def run(self, duration_s: float | None = None):
        if self.client:
            self.client.loop_start()
        log.info("publishing to %s every %.2f s (scenario=%s)",
                 self.topic_raw, self.period_s, self.model.scenario)
        start = time.monotonic()
        next_tick = start
        try:
            while not self._stop.is_set():
                if duration_s is not None and time.monotonic() - start >= duration_s:
                    break
                self._tick()
                # Fixed-rate schedule: no drift from processing time.
                next_tick += self.period_s
                delay = next_tick - time.monotonic()
                if delay < 0:  # fell behind (e.g. machine paused); resync
                    next_tick = time.monotonic()
                    delay = 0
                self._stop.wait(delay)
        finally:
            if self.client:
                # Clean shutdown: announce offline ourselves, then disconnect.
                self.client.publish(self.topic_status, "offline", qos=1, retain=True).wait_for_publish(2)
                self.client.disconnect()
                self.client.loop_stop()
            log.info("stopped after %d messages", self.seq)

    def _tick(self):
        with self._lock:
            self.model.step(self.period_s)
            reading = self.model.measure()
            inject = self._inject_invalid
            self._inject_invalid = False
        msg = build_message(self.asset_id, self.seq, reading)
        self.seq += 1
        payload = make_invalid_payload(msg, self.rng) if inject else json.dumps(msg)
        if self.dry_run:
            print(payload, flush=True)
        else:
            self.client.publish(self.topic_raw, payload, qos=1)
        if inject:
            log.info("sent invalid message: %s", payload)


def main():
    p = argparse.ArgumentParser(description="Water tank simulator")
    p.add_argument("--host", default=os.getenv("MQTT_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    p.add_argument("--asset-id", default=os.getenv("ASSET_ID", "tank_01"))
    p.add_argument("--rate-hz", type=float, default=float(os.getenv("PUBLISH_HZ", "1.0")))
    p.add_argument("--scenario", default=os.getenv("SCENARIO", "normal"), choices=SCENARIOS)
    seed_env = os.getenv("SEED")
    p.add_argument("--seed", type=int, default=int(seed_env) if seed_env else None)
    p.add_argument("--duration", type=float, default=None, help="stop after N seconds")
    p.add_argument("--dry-run", action="store_true", help="print messages, no MQTT")
    args = p.parse_args()

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    svc = SimulatorService(args.host, args.port, args.asset_id, args.rate_hz,
                           args.scenario, args.seed, dry_run=args.dry_run)
    signal.signal(signal.SIGTERM, svc.stop)  # docker stop
    signal.signal(signal.SIGINT, svc.stop)   # Ctrl+C
    svc.run(args.duration)


if __name__ == "__main__":
    main()
