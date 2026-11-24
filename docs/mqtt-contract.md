# MQTT contract — `weishaupt2mqtt`

Specification of the functional MQTT interface exposed by `weishaupt2mqtt` — the single bridge
that bundles `ebusd` (internal, `127.0.0.1:8888`, native ebusd MQTT disabled), the compiled
Weishaupt definitions, `weishaupt-metadata.json`, and all write guards.

Every consumer — the Home Assistant companion (`ha-weishaupt`), automations, scripts, or external
supervision tools — operates the heating installation exclusively through this contract.

## Principles

1. **One interface, one safety boundary.** All reads, writes, guards (global write switch, hourly
   write budget, linked bounds, read-back verification, shared stores, clock broadcast) live in
   `weishaupt2mqtt`. No client talks directly to `ebusd`.
2. **Functional payloads, not raw bus frames.** Values are decoded JSON with absent sensors mapped
   to `null` and invalid memory reads (`firstbyte != 0`) discarded. Weekly programs are exposed
   and written by day, not as raw slot telegrams.
3. **Circuit-keyed topics.** Topics are keyed by ebusd circuit (`ka`, `em1`, `em2`, `em3`, `fs1`,
   `fs2`, `fs3`, `bc1`, `bc2`, `broadcast`) so message names never collide (e.g. `ka/SupplyMaxTemp`
   vs `em1/SupplyMaxTemp`). Device grouping (`manager` → `ka` + `em1`) is published in
   `bridge/metadata`.
4. **Configurable base topic.** Default is `weishaupt` (configurable via `base_topic`). Every
   topic below is prefixed with `<base_topic>/` (shown as `weishaupt/`).
5. **UTF-8 JSON everywhere**, except `bridge/state` which is plain `online` / `offline` (LWT).

## Topic map

| Topic | Direction | Retain | Purpose |
|---|---|---|---|
| `weishaupt/bridge/state` | pub | yes | LWT availability: `online` or `offline` |
| `weishaupt/bridge/info` | pub | yes | Bridge health, bus signal, write budget, versions |
| `weishaupt/bridge/metadata` | pub | yes | Schema-1 metadata for the scanned devices (ranges, units, menus, risk classes) |
| `weishaupt/bridge/config/set` | sub | no | Runtime bridge controls (`writes_enabled`, `sync_clock`, `refresh`) |
| `weishaupt/bridge/result` | pub | no | Result of a `bridge/config/set` command |
| `weishaupt/bridge/journal` | pub | no | Audit event for every write attempt (success, refused, failed) |
| `weishaupt/<circuit>/state` | pub | yes | Latest decoded values of the circuit |
| `weishaupt/<circuit>/program/<program_id>` | pub | yes | Decoded weekly schedule (`1`–`7`) |
| `weishaupt/<circuit>/faults` | pub | yes | Ordered fault history and counters (`bc1`, `bc2`) |
| `weishaupt/<circuit>/set` | sub | no | Write *End user level* settings on `<circuit>` |
| `weishaupt/<circuit>/program/<program_id>/set` | sub | no | Write one or more days of a weekly program (*End user level*) |
| `weishaupt/<circuit>/installer/set` | sub | no | Write *Heating engineer level* settings on `<circuit>` (requires `"confirm": true`) |
| `weishaupt/<circuit>/result` | pub | no | Outcome of any `/set`, `/program/.../set`, or `/installer/set` command |

QoS 1 is recommended for all command (`/set`) and result (`/result`, `/journal`) topics.

---

## Bridge topics

### `weishaupt/bridge/state` (retained)

Plain text: `online` when connected to the broker and `ebusd` is reachable; `offline` published
via MQTT Last Will and Testament (LWT) on unexpected disconnect, or explicitly on graceful shutdown.

### `weishaupt/bridge/info` (retained)

Published at startup and whenever any field changes.

```json
{
  "bridge_version": "0.1.0",
  "definitions_version": "1.3.1",
  "ebusd_version": "26.1.p20261029",
  "run_time_checksum": "present",
  "bus_signal": true,
  "circuits": ["bc1", "bc2", "ka", "em1", "em2", "em3", "fs1", "fs2", "fs3"],
  "writes": {
    "enabled": true,
    "installer_enabled": false,
    "allow_stock_ebusd": false,
    "used_last_hour": 2,
    "max_per_hour": 30
  },
  "clock": {
    "last_sync": "2026-11-24T16:00:00+01:00",
    "outside_temp": 7.5
  }
}
```

### `weishaupt/bridge/metadata` (retained)

Publishes the `weishaupt-metadata-<version>.json` document (see `docs/companion-contract.md`,
schema 1), filtered to the devices present on the bus (or the full document plus `present_circuits`
in `bridge/info`). Clients such as `ha-weishaupt` use it to build entities, labels (EN/FR), menus,
linked bounds, `shared_with`, `stored_only` and risk classes without bundling a separate copy.

### `weishaupt/bridge/config/set` (command)

Accepted keys (one or more per payload, optional `correlation_id`):

```json
{
  "writes_enabled": false,
  "correlation_id": "req-1"
}
```

- `writes_enabled` (`bool`): global write kill-switch for *End user level* writes (persisted by
  the bridge). Turning it off also blocks *Heating engineer level* writes.
- `installer_writes_enabled` (`bool`): separate switch for *Heating engineer level* writes on
  `/installer/set` (`false` by default, persisted by the bridge).
- `sync_clock` (`true`): triggers an immediate clock broadcast at second `00` of the next minute,
  preserving the last outside temperature from the clock master.
- `refresh` (`"<circuit>/<message>"` or `["<circuit>/<message>", ...]`): forces an on-demand
  `read -f` of the listed message(s) and republishes `<circuit>/state`.

Outcome is published on `weishaupt/bridge/result`.

### `weishaupt/bridge/journal` (non-retained)

Emitted for every write attempt (setting, program, or clock), whether accepted, refused, or failed:

```json
{
  "timestamp": "2026-11-24T16:35:12+01:00",
  "correlation_id": "req-42",
  "kind": "setting",
  "circuit": "fs1",
  "target": "RoomNormalTemp",
  "source": "ff",
  "status": "ok",
  "before": 20.0,
  "requested": 20.5,
  "after": 20.5
}
```

---

## Circuit state topics (read)

### `weishaupt/<circuit>/state` (retained)

Flat JSON object keyed by ebusd message name (excluding individual program slot messages, which
are aggregated under `/program/<program_id>`, and raw fault slots, which are aggregated under
`/faults`):

- Single-field message → scalar value (`number`, `string` for value lists, `boolean`, or `null`
  when the sensor is absent / replacement value).
- Multi-field message → nested JSON object keyed by field name (excluding `skip: true` and `IGN`
  fields).
- Memory reads (`50 00`) with `firstbyte != 0` are discarded and never overwrite valid values.
- Includes `"_updated"` (ISO-8601 timestamp of the last update).

Example (`weishaupt/ka/state`):

```json
{
  "OutsideTemp": 7.5,
  "DHWTemp": 54.2,
  "DHWSetpoint": 55.0,
  "BlockingTime": 5,
  "Boiler1Status": {
    "phase": "BurnerInOperation",
    "flame": true,
    "load": 42,
    "supplytemp": 61.5,
    "error": 0
  },
  "_updated": "2026-11-24T16:30:05+01:00"
}
```

### `weishaupt/<circuit>/program/<program_id>` (retained)

Weekly schedule aggregated from the 21 slot registers (3 slots × 7 days, `"1"` = Monday … `"7"` =
Sunday). Each day is an ordered list of `"HH:MM-HH:MM"` intervals (quarter-hour steps; empty list
means off all day).

Example (`weishaupt/fs1/program/heating1`):

```json
{
  "circuit": "fs1",
  "program": "heating1",
  "writable": true,
  "shared_with": null,
  "stored_only": false,
  "days": {
    "1": ["06:30-18:00"],
    "2": ["06:30-18:00"],
    "3": ["06:30-12:00", "14:00-18:00"],
    "4": ["06:30-18:00"],
    "5": ["06:30-18:00"],
    "6": [],
    "7": []
  },
  "_updated": "2026-11-24T16:30:05+01:00"
}
```

For a shared store (e.g. `weishaupt/fs2/program/heating1`), `"writable": false` and
`"shared_with": "fs1/heating1"`.

### `weishaupt/<circuit>/faults` (retained, `bc1` / `bc2`)

Resolved fault history (ordered most-recent first using the boiler's fault pointer) and fault
counters, omitting empty slots.

---

## Write topics (command & result)

Writes are separated by access level so that both the bridge switches and MQTT broker ACLs
(e.g. Mosquitto) can enforce least privilege:

| Level | Weishaupt manual term | Topics | Bridge switch | Payload flag |
|---|---|---|---|---|
| **1 — User** | *End user level* / *Niveau Utilisateur* | `<circuit>/set`, `<circuit>/program/<id>/set` | `writes_enabled` | none |
| **2 — Installer (controllers)** | *Heating engineer level* / *Niveau Installateur* (`ka`, `em`, `fs`) | `<circuit>/installer/set` | `writes_enabled` + `installer_writes_enabled` | `"confirm": true` |
| **3 — Boiler automats** | *Heating engineer level — Parameter level* (`bc1`, `bc2`) | *(none — read-only)* | — | — |

### End user level settings: `weishaupt/<circuit>/set`

Payload: one or more *End user level* `<MessageName>: <value>` pairs, plus optional
`correlation_id`:

```json
{
  "RoomNormalTemp": 20.5,
  "correlation_id": "req-42"
}
```

Sending a *Heating engineer level* setting on `/set` is refused before any bus traffic
(`status: "refused", reason: "installer_topic_required"`).

### Heating engineer level settings: `weishaupt/<circuit>/installer/set`

Payload: one or more *Heating engineer level* `<MessageName>: <value>` pairs (e.g.
`GradientCurve`, `SupplyMaxTemp`, `DeCoupleMinTemp`, cascade and legionella parameters) **with**
`"confirm": true`:

```json
{
  "SupplyMaxTemp": 75.0,
  "confirm": true,
  "correlation_id": "req-43"
}
```

- If `installer_writes_enabled` is `false`, nothing is sent (`status: "refused",
  reason: "installer_writes_disabled"`).
- If `"confirm": true` is omitted, nothing is sent and `result` returns
  `"status": "confirmation_required"` along with `current`, `requested`, `min`, `max`, and `unit`.
- Sending an *End user level* setting on `/installer/set` is refused
  (`status: "refused", reason: "wrong_level_topic"`).
- Boiler automat parameters (`bc1`, `bc2`) are always read-only.

#### Execution sequence per setting

1. **Local checks** (no bus traffic): `writes.enabled` is true; setting exists and is writable
   (not read-only, not `shared_with`, not unmapped); build capability allows it; value is on step
   and within static range / value list; `"confirm": true` present if regulation class.
2. **Pre-read (`read -f`)**: read the target setting and any linked-bound siblings (`write.below` /
   `write.above`). Reject if out of narrowed bounds. If `current == requested`, publish
   `status: "unchanged"` without writing.
3. **Rate limit**: check the hourly write budget (EEPROM wear protection).
4. **Write**: send `write [-s ff] -c <circuit> -h <bytes>` to internal `ebusd` (do not connect
   the vendor service tool while writes are enabled).
5. **Read-back (`read -f`)**: verify the device now holds the requested value. Update
   `<circuit>/state`, publish `<circuit>/result` and `bridge/journal`.

### Weekly programs: `weishaupt/<circuit>/program/<program_id>/set`

Payload: partial or full `days` map (`"1"`–`"7"`), optional `correlation_id`. Days not listed are
left untouched.

```json
{
  "days": {
    "1": ["06:30-18:00"],
    "3": ["06:30-12:00", "14:00-18:00"]
  },
  "correlation_id": "prog-7"
}
```

#### Execution sequence

1. Validate all provided days first (quarter-hour steps, `start < end`, non-overlapping, at most 3
   intervals per day, program writable and not `shared_with`).
2. Read the current slots of the listed days from the device (`read -f`).
3. Compute the minimal diff (only slots whose raw bytes actually change).
4. Check upfront that the number of changed slots fits in the remaining hourly write budget; if
   not, write **nothing** and return `status: "refused", reason: "rate_limit"`.
5. Write each changed slot, read back, republish `<circuit>/program/<program_id>`, and publish
   `<circuit>/result` (with `slots_written`, `slots_unchanged`) and `bridge/journal`.

### Result: `weishaupt/<circuit>/result` (non-retained)

Published for every command received on `/set` or `/program/<program_id>/set`:

```json
{
  "timestamp": "2026-11-24T16:35:12+01:00",
  "correlation_id": "req-43",
  "circuit": "em2",
  "target": "SupplyMaxTemp",
  "status": "confirmation_required",
  "reason": "regulation_setting_requires_confirm",
  "current": 70.0,
  "requested": 75.0,
  "min": 8.0,
  "max": 82.0,
  "unit": "°C"
}
```

Standard `status` values:

| `status` | Meaning |
|---|---|
| `ok` | Written on the bus and verified by read-back |
| `unchanged` | Device already held the requested value; nothing sent |
| `confirmation_required` | Regulation setting sent without `"confirm": true`; nothing sent |
| `refused` | Blocked by a guard before any bus write (`writes_disabled`, `installer_writes_disabled`, `installer_topic_required`, `wrong_level_topic`, `read_only`, `shared_with`, `out_of_range`, `linked_bound`, `invalid_step`, `rate_limit`, `stock_ebusd_not_allowed`, `invalid_payload`) |
| `not_taken` | Sent and acknowledged on the bus, but read-back returned a different value |
| `error` | Bus/ebusd error (no signal, timeout, connection lost) |

---

## How clients use this contract

- **Home Assistant companion (`ha-weishaupt`)**: subscribes to `weishaupt/bridge/#`,
  `weishaupt/+/state`, `weishaupt/+/program/+`, `weishaupt/+/faults`, and `weishaupt/+/result`.
  For *End user level* settings it publishes to `/set`; for *Heating engineer level* settings it
  keeps its two-screen confirmation UI and publishes `{"<Setting>": <value>, "confirm": true}` on
  `/installer/set` once the user confirms.
- **External scripts / automations**:
  - User setpoint (ACL limited to `weishaupt/+/set` and `weishaupt/+/program/+/set`):
    `weishaupt/fs1/set` → `{"RoomNormalTemp": 20.5}`
  - Program update:
    `weishaupt/fs1/program/heating1/set` → `{"days": {"1": ["06:30-18:00"]}}`
  - Installer setting (requires ACL on `weishaupt/+/installer/set` + `installer_writes_enabled`):
    `weishaupt/em2/installer/set` → `{"SupplyMaxTemp": 75.0, "confirm": true}`
