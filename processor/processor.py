"""Processor service for the tank_01 digital twin.

For every raw message on tank/<asset>/raw it:
  1. validates it (bad messages -> 'rejected' table + tank/<asset>/rejected)
  2. computes features and runs the AI model (if a trained model exists)
  3. evaluates the rule and AI alarms
  4. stores everything in SQLite
  5. computes aggregates and publishes the processed state on tank/<asset>/state

It also serves stored data over HTTP (see api.py) and makes periodic
database backups.

MQTT callbacks only put messages on a queue; one worker thread does all
processing and all database writes, so there is never more than one writer.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import signal
import threading
import time

import alarms
import storage
from api import start_api
from features import LevelHistory, make_features
from predictor import Predictor
from validation import validate_raw

log = logging.getLogger("processor")


class Processor:
    """MQTT-free core, so it can be unit tested directly."""

    def __init__(self, asset_id: str, db_path: str, model_path: str):
        self.asset_id = asset_id
        self.db = storage.Storage(db_path)
        self.predictor = Predictor(model_path)
        self.history = LevelHistory()
        self.processed = 0
        self.rejected = 0
        self.last_ts_epoch: float | None = None

        # Restart recovery: continue from what is already stored.
        last = self.db.last_reading(asset_id)
        if last is not None:
            self.last_ts_epoch = last["ts_epoch"]
            for t, lv in self.db.recent_levels(asset_id, 10, last["ts_epoch"] + 1):
                self.history.add(t, lv)
        active = self.db.active_alarms(asset_id)
        self.alarm_state = {"rule": active.get("rule", alarms.NONE),
                            "ai": active.get("ai", alarms.NONE)}
        log.info("recovered state: last reading %s, active alarms %s",
                 storage.iso(self.last_ts_epoch) if self.last_ts_epoch else "none",
                 self.alarm_state)

    def handle(self, topic: str, payload: bytes, received_at: float | None = None):
        """Process one raw message.

        Returns ("state", dict) for a good message or ("rejected", dict).
        """
        received_at = received_at or time.time()
        rec, reason = validate_raw(payload, self.asset_id, now=received_at)

        if rec is not None and self.last_ts_epoch is not None and rec.ts_epoch <= self.last_ts_epoch:
            reason = ("duplicate message" if rec.ts_epoch == self.last_ts_epoch
                      else "out-of-order message (older than last stored)")
            rec = None

        if rec is None:
            text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else str(payload)
            self.db.insert_rejected(topic, text, reason, received_at)
            self.rejected += 1
            log.warning("rejected message: %s | %s", reason, text[:200])
            return "rejected", {"asset_id": self.asset_id, "received_at": storage.iso(received_at),
                                "reason": reason, "payload": text[:500]}

        self.last_ts_epoch = rec.ts_epoch

        # Features and AI prediction.
        self.history.add(rec.ts_epoch, rec.level_pct)
        d_level = self.history.d_level()
        net_flow = rec.inflow_lpm - rec.outflow_lpm
        feats = (make_features(rec.level_pct, rec.inflow_lpm, rec.outflow_lpm,
                               rec.pump_on, d_level) if d_level is not None else None)
        pred, model_status = self.predictor.predict(feats)

        # Alarms (with hysteresis), recording each alarm episode.
        new_state = {"rule": alarms.RULE.classify(rec.level_pct, self.alarm_state["rule"]),
                     "ai": alarms.AI.classify(pred, self.alarm_state["ai"])}
        for source in ("rule", "ai"):
            if new_state[source] != self.alarm_state[source]:
                if self.alarm_state[source] != alarms.NONE:
                    self.db.close_alarms(self.asset_id, source, rec.ts_epoch)
                if new_state[source] != alarms.NONE:
                    self.db.open_alarm(self.asset_id, source, new_state[source],
                                       rec.ts_epoch, rec.level_pct)
                    log.warning("%s alarm raised: %s (level %.1f %%, predicted %s)",
                                source, new_state[source], rec.level_pct, pred)
                else:
                    log.info("%s alarm cleared", source)
        self.alarm_state = new_state

        processed_at = time.time()
        row = {
            "asset_id": rec.asset_id, "ts": rec.timestamp, "ts_epoch": rec.ts_epoch,
            "seq": rec.seq, "level_pct": rec.level_pct, "inflow_lpm": rec.inflow_lpm,
            "outflow_lpm": rec.outflow_lpm, "pump_on": int(rec.pump_on),
            "net_flow_lpm": round(net_flow, 3),
            "d_level_5s": None if d_level is None else round(d_level, 3),
            "pred_level_30s_pct": pred, "model_status": model_status,
            "model_version": self.predictor.version if pred is not None else None,
            "rule_alarm": new_state["rule"], "ai_alarm": new_state["ai"],
            "received_at": received_at, "processed_at": processed_at,
            "pipeline_delay_ms": round((processed_at - rec.ts_epoch) * 1000, 1),
        }
        self.db.insert_reading(row)
        agg = storage.aggregates(self.db.conn, self.asset_id, rec.ts_epoch)
        self.processed += 1

        state = {
            "asset_id": rec.asset_id,
            "timestamp": rec.timestamp,
            "seq": rec.seq,
            "level_pct": rec.level_pct,
            "inflow_lpm": rec.inflow_lpm,
            "outflow_lpm": rec.outflow_lpm,
            "pump_on": rec.pump_on,
            "net_flow_lpm": row["net_flow_lpm"],
            "pred_level_30s_pct": pred,
            "model_status": model_status,
            "model_version": row["model_version"],
            "rule_alarm": new_state["rule"],
            "ai_alarm": new_state["ai"],
            "level_mean_60s_pct": agg["level_mean_60s_pct"],
            "level_max_10min_pct": agg["level_max_10min_pct"],
            "level_min_10min_pct": agg["level_min_10min_pct"],
            "alarms_last_hour": agg["alarms_last_hour"],
            "pumped_volume_l_last_hour": agg["pumped_volume_l_last_hour"],
            "pipeline_delay_ms": row["pipeline_delay_ms"],
            "processed_at": storage.iso(processed_at),
        }
        return "state", state


class ProcessorService:
    def __init__(self, host, port, asset_id, db_path, model_path, http_port,
                 backup_dir, backup_interval_s, backup_keep):
        import paho.mqtt.client as mqtt

        self.core = Processor(asset_id, db_path, model_path)
        self.db_path = db_path
        self.backup_dir = backup_dir
        self.backup_interval_s = backup_interval_s
        self.backup_keep = backup_keep
        self.http_port = http_port
        self.started_at = time.time()
        self.queue: queue.Queue = queue.Queue(maxsize=10000)
        self._stop = threading.Event()
        self.mqtt_connected = False
        self.last_backup: str | None = None

        base = f"tank/{asset_id}"
        self.topic_raw = f"{base}/raw"
        self.topic_state = f"{base}/state"
        self.topic_rejected = f"{base}/rejected"
        self.topic_status = f"{base}/processor/status"

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                                  client_id=f"processor-{asset_id}")
        self.client.will_set(self.topic_status, "offline", qos=1, retain=True)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)
        self.client.connect_async(host, port, keepalive=10)

    # --- MQTT callbacks (network thread): keep them tiny.
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("connect failed: %s", reason_code)
            return
        self.mqtt_connected = True
        log.info("connected to broker, subscribing to %s", self.topic_raw)
        client.subscribe(self.topic_raw, qos=1)
        client.publish(self.topic_status, "online", qos=1, retain=True)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.mqtt_connected = False
        log.warning("disconnected from broker (%s); will retry", reason_code)

    def _on_message(self, client, userdata, msg):
        try:
            self.queue.put_nowait((msg.topic, msg.payload, time.time()))
        except queue.Full:
            log.error("queue full, dropping message")

    # --- worker thread: all processing and DB writes happen here.
    def _worker(self):
        while not self._stop.is_set():
            try:
                topic, payload, received_at = self.queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                kind, body = self.core.handle(topic, payload, received_at)
                out_topic = self.topic_state if kind == "state" else self.topic_rejected
                self.client.publish(out_topic, json.dumps(body), qos=1)
            except Exception:  # noqa: BLE001 - never let one message kill the service
                log.exception("unexpected error while processing a message")

    def _backup_loop(self):
        while not self._stop.wait(self.backup_interval_s):
            try:
                self.last_backup = storage.backup(self.db_path, self.backup_dir, self.backup_keep)
                log.info("database backup written: %s", self.last_backup)
            except Exception:  # noqa: BLE001
                log.exception("backup failed")

    def status(self) -> dict:
        c = self.core
        return {
            "service": "processor",
            "asset_id": c.asset_id,
            "uptime_s": round(time.time() - self.started_at, 1),
            "mqtt_connected": self.mqtt_connected,
            "model_loaded": c.predictor.model is not None,
            "model_version": c.predictor.version,
            "messages_processed": c.processed,
            "messages_rejected": c.rejected,
            "last_reading": storage.iso(c.last_ts_epoch) if c.last_ts_epoch else None,
            "last_reading_age_s": round(time.time() - c.last_ts_epoch, 1) if c.last_ts_epoch else None,
            "queue_size": self.queue.qsize(),
            "last_backup": self.last_backup,
        }

    def stop(self, *_):
        self._stop.set()

    def run(self):
        self.http = start_api("0.0.0.0", self.http_port, self.db_path,
                              self.core.asset_id, self.status)
        self.client.loop_start()
        workers = [threading.Thread(target=self._worker, name="worker", daemon=True),
                   threading.Thread(target=self._backup_loop, name="backup", daemon=True)]
        for t in workers:
            t.start()
        try:
            while not self._stop.wait(1):
                pass
        finally:
            log.info("shutting down")
            self.client.publish(self.topic_status, "offline", qos=1, retain=True).wait_for_publish(2)
            self.client.disconnect()
            self.client.loop_stop()
            workers[0].join(3)
            self.http.shutdown()
            self.http.server_close()  # release port 8000
            self.core.db.close()


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    svc = ProcessorService(
        host=os.getenv("MQTT_HOST", "localhost"),
        port=int(os.getenv("MQTT_PORT", "1883")),
        asset_id=os.getenv("ASSET_ID", "tank_01"),
        db_path=os.getenv("DB_PATH", "data/tank.db"),
        model_path=os.getenv("MODEL_PATH", "ai/models/level_model.joblib"),
        http_port=int(os.getenv("HTTP_PORT", "8000")),
        backup_dir=os.getenv("BACKUP_DIR", "data/backups"),
        backup_interval_s=float(os.getenv("BACKUP_INTERVAL_S", "600")),
        backup_keep=int(os.getenv("BACKUP_KEEP", "6")),
    )
    signal.signal(signal.SIGTERM, svc.stop)
    signal.signal(signal.SIGINT, svc.stop)
    svc.run()


if __name__ == "__main__":
    main()
