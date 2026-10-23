# Water Tank Digital Twin (Project 2, Topic 3)

Simulation-driven digital twin prototype of one water tank and pump (`tank_01`).
See `docs/planning.md` for the full system plan, state dictionary and interfaces.

## Services

| Service | Runs in | Ports / topics |
|---|---|---|
| Mosquitto broker | Docker (`eclipse-mosquitto:2.0.20`) | TCP 1883 |
| Simulator | Docker (Python 3.12) | publishes `tank/tank_01/raw` at 1 Hz |
| Processor | Docker (Python 3.12) | subscribes `tank/tank_01/raw`; publishes `tank/tank_01/state` and `tank/tank_01/rejected`; HTTP API on 8000 |
| Omniverse Kit app | Ubuntu desktop *(later step)* | subscribes `tank/tank_01/state`, reads HTTP history |

Launch order is handled by Docker Compose: the broker starts first, and the
simulator and processor start once the broker reports healthy.

## Start the backend

```bash
docker compose up -d --build     # build and start broker, simulator, processor
docker compose ps                # all three "running"; broker and processor "healthy"
docker compose logs -f processor # watch the processor log (Ctrl+C to exit)
```

Watch the live data:

```bash
mosquitto_sub -h localhost -t 'tank/tank_01/state' -v     # processed states
mosquitto_sub -h localhost -t 'tank/#' -v                 # everything
```

Stop everything (data in `./data` is kept):

```bash
docker compose down
```

## HTTP API (processor, port 8000)

| Request | Returns |
|---|---|
| `curl localhost:8000/health` | Service status, MQTT connection, message counts, last reading age |
| `curl 'localhost:8000/history?minutes=10'` | Stored readings, oldest first (max 1440 minutes) |
| `curl localhost:8000/aggregates` | Rolling mean, 10-min max/min, alarm count, pumped volume, delay |
| `curl 'localhost:8000/alarms?hours=24'` | Alarm episodes with start, end, duration |
| `curl 'localhost:8000/rejected?limit=20'` | Latest rejected messages and why |

Add `| python3 -m json.tool` to any of these for readable output.

## Data storage

- Database: `data/tank.db` (SQLite). Inspect with `sqlite3 data/tank.db` (install: `sudo apt install sqlite3`).
- Backups: `data/backups/`, written every 10 minutes, newest 6 kept.
- Tables: `readings`, `alarms`, `rejected` (schema in `processor/storage.py`).

## Control the simulator

Change scenario (`normal`, `fill`, `drain`, `abnormal_inflow`, `pump_failure`):

```bash
mosquitto_pub -h localhost -t tank/tank_01/sim/control -m '{"scenario": "abnormal_inflow"}'
```

Send one malformed message (invalid-input test):

```bash
mosquitto_pub -h localhost -t tank/tank_01/sim/control -m '{"inject_invalid": true}'
```

## Mandatory test scenarios (backend part)

| Scenario | How |
|---|---|
| Normal operation | `curl localhost:8000/aggregates` shows `readings_last_60s` ≈ 60 |
| Abnormal condition | Switch to `abnormal_inflow`; `rule_alarm` becomes `HIGH_LEVEL` near 95 % (AI alarm once the model is trained) |
| Stream interruption | `docker compose stop simulator`; `last_reading_age_s` in `/health` keeps growing; `tank/tank_01/status` = `offline`. `docker compose start simulator` to recover |
| Invalid input | Send `inject_invalid`; check `curl localhost:8000/rejected` |
| Restart and history | `docker compose restart processor`, then `curl 'localhost:8000/history?minutes=30'` still returns the earlier data |

## Tests

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests -v
```

Unit tests always run. Integration tests need the broker (`docker compose up -d`)
and are skipped if none is reachable on port 1883. They use their own asset IDs
(`tank_test`, `tank_it`) and port 18080, so they don't disturb the running `tank_01` services.

## Run without Docker (for development)

```bash
python simulator/simulator.py --dry-run --duration 10   # print messages, no broker needed
python simulator/simulator.py --scenario fill            # needs a broker on localhost:1883
python processor/processor.py                            # writes data/tank.db, API on :8000
```
