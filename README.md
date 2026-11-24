# weishaupt2mqtt

All-in-one functional MQTT bridge for Weishaupt WTC condensing boilers and Kromschröder WCM
controllers (`ka`, `em1`–`em3`, `fs1`–`fs3`, `bc1`–`bc2`).

Bundles `ebusd` (bound to `127.0.0.1:8888`, native ebusd MQTT disabled), the compiled Weishaupt
definitions, `weishaupt-metadata.json`, and all write guards behind a single functional MQTT
interface (see [docs/mqtt-contract.md](docs/mqtt-contract.md)).

## Access levels in MQTT

| Level | Weishaupt term | MQTT topics | Guard |
|---|---|---|---|
| **1 — User** | *End user level* | `weishaupt/<circuit>/set`, `weishaupt/<circuit>/program/<id>/set` | `writes_enabled` |
| **2 — Installer** | *Heating engineer level* (`ka`, `em`, `fs`) | `weishaupt/<circuit>/installer/set` | `writes_enabled` + `installer_writes_enabled` + `"confirm": true` |
| **3 — Boilers** | *Heating engineer level* (`bc1`, `bc2`) | *(read-only)* | — |

## Quick start (external `ebusd`, read-only trial)

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e .
EBUSD_HOST=127.0.0.1 MQTT_HOST=127.0.0.1 python3 -m weishaupt2mqtt
```

## Quick start (all-in-one Docker)

```bash
docker build -t weishaupt2mqtt .
docker run --rm \
  -e EBUS_DEVICE=ens:<adapter-ip>:9999 \
  -e MQTT_HOST=<broker-ip> \
  -e MQTT_USERNAME=<user> \
  -e MQTT_PASSWORD=<pass> \
  weishaupt2mqtt
```

## Tests

```bash
python3 -m unittest discover -s tests -v
```
