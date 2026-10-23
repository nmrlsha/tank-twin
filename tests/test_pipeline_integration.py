"""End-to-end integration test of the data path:

    simulator -> MQTT broker -> processor -> SQLite -> MQTT state + HTTP API

Covers the brief's mandatory scenarios that do not need the Omniverse app:
normal operation, invalid input, stream interruption and restart + history.

Needs the broker running:   docker compose up -d broker
Uses asset ID 'tank_it' and HTTP port 18080, so it can run alongside the
real tank_01 services.
Run from the repo root:      python -m pytest tests/test_pipeline_integration.py -v
"""

import json
import os
import socket
import sys
import threading
import time
import urllib.request
from pathlib import Path

import paho.mqtt.client as mqtt
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "simulator"))
sys.path.insert(0, str(ROOT / "processor"))

from simulator import SimulatorService  # noqa: E402
from processor import ProcessorService  # noqa: E402

HOST = os.getenv("MQTT_HOST", "localhost")
PORT = int(os.getenv("MQTT_PORT", "1883"))
ASSET = "tank_it"     # separate asset ID: does not touch the real tank_01 data
HTTP_PORT = 18080


def broker_up():
    try:
        with socket.create_connection((HOST, PORT), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not broker_up(), reason="no MQTT broker on localhost:1883")


def get(path):
    with urllib.request.urlopen(f"http://127.0.0.1:{HTTP_PORT}{path}", timeout=3) as r:
        return json.loads(r.read())


class Listener:
    def __init__(self):
        self.msgs = []
        self.ready = threading.Event()
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.c.on_connect = lambda c, *_: (c.subscribe(f"tank/{ASSET}/#", qos=1), self.ready.set())
        self.c.on_message = lambda c, u, m: self.msgs.append((m.topic, m.payload.decode(), time.time()))
        self.c.connect(HOST, PORT)
        self.c.loop_start()
        assert self.ready.wait(5)
        time.sleep(0.3)

    def on(self, suffix):
        return [(p, t) for top, p, t in self.msgs if top == f"tank/{ASSET}/{suffix}"]

    def close(self):
        self.c.loop_stop()
        self.c.disconnect()


def start_processor(db):
    svc = ProcessorService(HOST, PORT, ASSET, db, "/nonexistent/model.joblib", HTTP_PORT,
                           str(Path(db).parent / "backups"), backup_interval_s=3600, backup_keep=2)
    t = threading.Thread(target=svc.run, daemon=True)
    t.start()
    for _ in range(50):
        if svc.mqtt_connected:
            break
        time.sleep(0.1)
    time.sleep(0.5)
    return svc, t


def start_sim(duration, scenario="normal", seed=1):
    svc = SimulatorService(HOST, PORT, ASSET, 1.0, scenario, seed=seed)
    t = threading.Thread(target=svc.run, args=(duration,), daemon=True)
    t.start()
    return svc, t


def test_full_pipeline(tmp_path):
    db = str(tmp_path / "tank.db")
    lis = Listener()
    proc, proc_t = start_processor(db)
    try:
        # ---- 1. Normal operation: ~1 Hz processed states, stored in SQLite.
        sim, sim_t = start_sim(duration=10.5)
        time.sleep(4)
        # ---- 2. Invalid input: one bad message is rejected, nothing crashes.
        lis.c.publish(f"tank/{ASSET}/sim/control", json.dumps({"inject_invalid": True}), qos=1)
        sim_t.join(20)
        time.sleep(1.0)

        states = [json.loads(p) for p, _ in lis.on("state")]
        rejected = [json.loads(p) for p, _ in lis.on("rejected")]
        assert 8 <= len(states) <= 11, len(states)
        assert len(rejected) == 1, rejected
        assert proc_t.is_alive()

        s = states[-1]
        assert s["asset_id"] == ASSET
        assert s["model_status"] == "MODEL_MISSING"     # no trained model yet
        assert s["level_mean_60s_pct"] is not None
        # Data-to-processed delay well under the 2 s target.
        assert max(x["pipeline_delay_ms"] for x in states) < 2000

        h = get("/health")
        assert h["mqtt_connected"] and h["messages_processed"] == len(states)
        assert h["messages_rejected"] == 1
        assert get("/rejected")["rejected"][0]["reason"]
        agg = get("/aggregates")
        assert agg["readings_last_60s"] == len(states)

        # ---- 3. Stream interruption: simulator gone -> no new states, status offline.
        n_before = len(lis.on("state"))
        time.sleep(3)
        assert len(lis.on("state")) == n_before
        assert lis.on("status")[-1][0] == "offline"
        assert get("/health")["last_reading_age_s"] > 2.5

        # ---- 4. Restart processor: history survives and new data continues.
        proc.stop()
        proc_t.join(10)
        proc, proc_t = start_processor(db)
        sim, sim_t = start_sim(duration=4.5, seed=2)
        sim_t.join(15)
        time.sleep(1.0)

        hist = get("/history?minutes=5")
        assert hist["count"] >= len(states) + 3           # old + new readings
        times = [r["ts_epoch"] for r in hist["readings"]]
        assert times == sorted(times)                       # oldest first
        assert lis.on("status")[-1][0] == "offline"         # resumed then stopped cleanly

        # Bad HTTP queries are answered with 400, not a crash.
        with pytest.raises(urllib.error.HTTPError) as e:
            get("/history?minutes=abc")
        assert e.value.code == 400
    finally:
        proc.stop()
        proc_t.join(10)
        lis.close()
