"""Integration test: simulator -> MQTT broker -> subscriber.

Needs the broker running:   docker compose up -d broker
Run from the repo root:      python -m pytest tests/test_stream_integration.py -v

Skipped automatically if no broker answers on localhost:1883.
"""

import json
import os
import socket
import sys
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))

from simulator import SimulatorService  # noqa: E402

HOST = os.getenv("MQTT_HOST", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
ASSET = "tank_test"  # separate asset ID so a running simulator doesn't interfere


def broker_up() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not broker_up(), reason="no MQTT broker on localhost:1883")


class Collector:
    def __init__(self, topics):
        self.messages = []  # (topic, payload, receive_time)
        self.connected = threading.Event()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = lambda c, *_: (
            [c.subscribe(t, qos=1) for t in topics], self.connected.set())
        self.client.on_message = lambda c, u, m: self.messages.append(
            (m.topic, m.payload.decode(errors="replace"), time.time()))
        self.client.connect(HOST, PORT)
        self.client.loop_start()
        assert self.connected.wait(5), "collector could not connect"
        time.sleep(0.3)  # let subscriptions settle

    def close(self):
        self.client.loop_stop()
        self.client.disconnect()

    def on(self, topic):
        return [m for m in self.messages if m[0] == topic]


def start_sim(duration, scenario="normal"):
    svc = SimulatorService(HOST, PORT, ASSET, 1.0, scenario, seed=1)
    t = threading.Thread(target=svc.run, args=(duration,), daemon=True)
    t.start()
    return svc, t


def test_stream_rate_schema_and_status():
    col = Collector([f"tank/{ASSET}/raw", f"tank/{ASSET}/status"])
    try:
        _, t = start_sim(duration=10.5)
        t.join(20)
        time.sleep(0.5)

        raw = col.on(f"tank/{ASSET}/raw")
        # ~1 Hz for 10 s (allow for connection start-up).
        assert 9 <= len(raw) <= 12, f"got {len(raw)} messages"

        msgs = [json.loads(p) for _, p, _ in raw]
        seqs = [m["seq"] for m in msgs]
        assert seqs == sorted(seqs), "messages out of order"
        assert all(m["asset_id"] == ASSET for m in msgs)

        # Interval between messages close to 1 s.
        times = [rt for _, _, rt in raw]
        gaps = [b - a for a, b in zip(times, times[1:])]
        assert all(0.7 <= g <= 1.3 for g in gaps[1:]), gaps

        status = [p for _, p, _ in col.on(f"tank/{ASSET}/status")]
        assert status[0] == "online" or "online" in status
        assert status[-1] == "offline"
    finally:
        col.close()


def test_scenario_change_via_control_topic():
    col = Collector([f"tank/{ASSET}/raw"])
    try:
        svc, t = start_sim(duration=12, scenario="normal")
        time.sleep(3)
        col.client.publish(f"tank/{ASSET}/sim/control",
                           json.dumps({"scenario": "drain"}), qos=1)
        time.sleep(1.5)
        assert svc.model.scenario == "drain"

        # Bad control messages are ignored without crashing the simulator.
        col.client.publish(f"tank/{ASSET}/sim/control", "garbage", qos=1)
        col.client.publish(f"tank/{ASSET}/sim/control",
                           json.dumps({"scenario": "explode"}), qos=1)
        time.sleep(1.5)
        assert svc.model.scenario == "drain"
        assert t.is_alive()

        t.join(20)
        raw = [json.loads(p) for _, p, _ in col.on(f"tank/{ASSET}/raw")]
        assert raw[-1]["pump_on"] is False  # drain turns the pump off
    finally:
        col.close()


def test_inject_invalid_sends_one_bad_message():
    col = Collector([f"tank/{ASSET}/raw"])
    try:
        _, t = start_sim(duration=6)
        time.sleep(2)
        col.client.publish(f"tank/{ASSET}/sim/control",
                           json.dumps({"inject_invalid": True}), qos=1)
        t.join(20)

        bad = 0
        for _, p, _ in col.on(f"tank/{ASSET}/raw"):
            try:
                d = json.loads(p)
                ok = ("level_pct" in d and isinstance(d.get("inflow_lpm"), (int, float))
                      and 0 <= d["level_pct"] <= 100)
            except json.JSONDecodeError:
                ok = False
            bad += not ok
        assert bad == 1
    finally:
        col.close()
