# Water Tank Digital Twin (Project 2, Topic 3)

Simulation-driven digital twin prototype of one water tank and pump (`tank_01`).
See `docs/planning.md` for the full system plan, state dictionary and interfaces.

## Services

| Service | Runs in | Port / topic |
|---|---|---|
| Mosquitto broker | Docker (`eclipse-mosquitto:2.0.20`) | TCP 1883 |
| Simulator | Docker (Python 3.12, paho-mqtt 2.1.0) | publishes `tank/tank_01/raw` at 1 Hz |
| Processor | Docker *(next step)* | `tank/tank_01/state`, HTTP 8000 |
| Omniverse Kit app | Ubuntu desktop *(later step)* | subscribes `tank/tank_01/state` |

## Start the backend

```bash
docker compose up -d --build     # start broker + simulator
docker compose ps                # both should show "running" / "healthy"
docker compose logs -f simulator # watch the simulator log (Ctrl+C to exit)
```

Watch the live data stream:

```bash
mosquitto_sub -h localhost -t 'tank/#' -v
```

Stop everything:

```bash
docker compose down
```

## Control the simulator

Change scenario (`normal`, `fill`, `drain`, `abnormal_inflow`, `pump_failure`):

```bash
mosquitto_pub -h localhost -t tank/tank_01/sim/control -m '{"scenario": "abnormal_inflow"}'
```

Send one malformed message (invalid-input test):

```bash
mosquitto_pub -h localhost -t tank/tank_01/sim/control -m '{"inject_invalid": true}'
```

Stream interruption test: `docker compose stop simulator`, then `docker compose start simulator`.
The broker publishes `offline` on `tank/tank_01/status` when the simulator stops.

## Tests

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests -v
```

Unit tests always run. Integration tests (`test_stream_integration.py`) need the broker:
`docker compose up -d broker`. They are skipped if no broker is reachable on port 1883.

## Run the simulator without Docker

```bash
python simulator/simulator.py --dry-run --duration 10   # print messages, no broker needed
python simulator/simulator.py --scenario fill            # publish to localhost:1883
```
