# weishaupt2mqtt

An MQTT bridge for Weishaupt WTC condensing boilers and their Kromschröder WCM controllers
(system manager, heating modules, room units). It reads the heating system over eBUS and publishes
decoded values, weekly programs and fault memory as JSON on MQTT; it accepts guarded write commands
on MQTT and nothing else writes to the bus.

One container holds everything:

- **ebusd** (the build [aghulas/ebusd v26.1-weishaupt.2](https://github.com/aghulas/ebusd/releases/tag/v26.1-weishaupt.2)),
  bound to the loopback interface of the container: no ebusd port is published, ebusd's own MQTT
  is off;
- the **Weishaupt definitions** [weishaupt-1.3.1](https://github.com/aghulas/ebusd-configuration/releases/tag/weishaupt-1.3.1)
  and their metadata (labels, ranges, write conditions);
- the **bridge** itself (Python), with the write guards.

Unofficial: not made or endorsed by Weishaupt, Kromschröder or the ebusd project. MQTT topics and
payloads are specified in [docs/mqtt-contract.md](docs/mqtt-contract.md).

**What it is not.** It does not create Home Assistant entities by itself (no MQTT discovery): the
entities come from the integration [ha-weishaupt](https://github.com/aghulas/ha-weishaupt), or from
MQTT sensors you declare yourself. It does not write to the boiler automats, ever. It is not a
general ebusd-to-MQTT gateway: it knows only the devices of these definitions.

**Checked on one installation:** two Weishaupt WTC-GB 250 condensing boilers in cascade (automat
software 11.04), a WCM-KA 3.0 system manager, two WCM-EM heating modules and two WCM-FS room units,
three heating circuits; a network eBUS adapter (ebusd device string `ens:<address>:9999`); Home
Assistant OS on a 64-bit ARM board with the Mosquitto broker add-on. The image also builds and
starts on 64-bit ARM Docker without a bus (the start-up and the refusals below were reproduced that
way). The `amd64` image was not run. Other equipment may answer differently.

**Your responsibility.** Reading changes nothing. Writing changes the settings of a gas heating
appliance: do it only if you understand the setting, note its value first, and expect no help from
the manufacturer for an installation changed this way. Writes are off until you switch them on.

## What you need

- An eBUS adapter on the bus of the WCM controllers, reachable **over the network** (the Home
  Assistant add-on passes no serial device to the container; see [Known limits](#known-limits)).
- An MQTT broker (in Home Assistant: the **Mosquitto broker** add-on and the MQTT integration).
- No other ebusd on the same adapter: stop an ebusd add-on or service that uses it. Running two
  was not tried.
- **For entities in Home Assistant, read [Use it from Home Assistant](#use-it-from-home-assistant)
  before you install**: the integration's entry has to be created against an ebusd add-on first,
  and switched to the bridge afterwards. For MQTT alone (your own sensors, another tool) the
  bridge is all you need.

## Install as a Home Assistant add-on

The add-on is published from the branch `weishaupt` of
[aghulas/ha-addons](https://github.com/aghulas/ha-addons/tree/weishaupt), folder `weishaupt2mqtt`
(a copy of this repository at the same version). The folder `addon/` of this repository is not the
published add-on.

1. *Settings → Add-ons → Add-on store → ⋮ → Repositories*, add
   `https://github.com/aghulas/ha-addons#weishaupt`. The `#weishaupt` selects the branch and is for
   the store only: in a browser that address opens the default branch, which does not hold this
   add-on.
2. Reload the store and install **Weishaupt2MQTT**. There is no prebuilt image: it is built on your
   machine, which downloads the ebusd executable and the definitions from their GitHub releases
   (each checked against a SHA-256) and takes several minutes.
3. On the **Configuration** tab set `ebus_device` to your adapter, as ebusd writes a device
   (`ens:<address>:9999` for an enhanced-protocol network adapter). Leave the rest as it is for a
   first start: with the Mosquitto add-on, the broker address and credentials are found by
   themselves.
4. Start the add-on and open its **Log** tab. See [First start](#first-start).

### Options

| Option | Default | Meaning |
|---|---|---|
| `ebus_device` | *(empty)* | The adapter, in ebusd's `--device` form. Required unless `ebusd_external` is on: with it empty the add-on stops with `ERROR: EBUS_DEVICE is required unless EBUSD_EXTERNAL=true`. |
| `ebusd_address` | `31` | ebusd's own bus address. Keep `31`: the settings proven from ebusd's own address are refused from another (`source_not_own`). |
| `ebusd_external` | `false` | `true`: do not start the bundled ebusd; use the one at `ebusd_host`:`ebusd_port`, which must have these definitions loaded (`--scanconfig --configpath=…`). |
| `ebusd_host`, `ebusd_port` | `127.0.0.1`, `8888` | Where ebusd answers. Leave as is with the bundled ebusd. |
| `mqtt_host`, `mqtt_port` | `core-mosquitto`, `1883` | The broker. No TLS. |
| `mqtt_username`, `mqtt_password` | *(empty)* | Empty under Home Assistant: the credentials (and the address, if left at its default) of the broker add-on are asked of the Supervisor. |
| `base_topic` | `weishaupt` | First level of every topic. |
| `writes_enabled` | `false` | Start value of the user-level write switch. See [Writing](#writing). |
| `installer_writes_enabled` | `false` | Start value of the installer-level write switch. It only acts when `writes_enabled` is on too. |
| `allow_stock_ebusd` | `false` | Only for `ebusd_external` with an official ebusd, which does not compute the write checksum: allow writes to the system manager, heating modules and room units with the placeholder checksum of the definitions. No effect with the bundled ebusd. |
| `max_writes_per_hour` | `30` | Hourly write budget, 1 to 200. |
| `clock_sync_enabled` | `false` | `true`: the bridge sets the date and time of the installation from its own clock every 30 minutes (first time 30 minutes after the start). It is a write: it also needs `writes_enabled`. See [Set the date and time](#set-the-date-and-time) before switching it on. |
| `log_level` | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR`, for the bridge (ebusd logs at `notice`). |

The two write switches can also be changed while the bridge runs (see [Writing](#writing)); the
add-on then stores them in its data folder, and **the stored values win over these two options at
the next start**. `bridge/info` shows what is in force.

## Install with Docker

Outside Home Assistant the same image is built from this repository. Built and started on 64-bit
ARM; there is no published image.

```bash
git clone https://github.com/aghulas/weishaupt2mqtt && cd weishaupt2mqtt
docker build -t weishaupt2mqtt .
docker run -d --name weishaupt2mqtt --restart unless-stopped -e EBUS_DEVICE=ens:<adapter-address>:9999 -e MQTT_HOST=<broker-address> weishaupt2mqtt
```

Each add-on option is an environment variable here: `EBUS_DEVICE`, `EBUSD_ADDRESS`,
`EBUSD_EXTERNAL`, `EBUSD_HOST`, `EBUSD_PORT`, `MQTT_HOST` (default `127.0.0.1`), `MQTT_PORT`,
`MQTT_USERNAME`, `MQTT_PASSWORD`, `WEISHAUPT_BASE_TOPIC`, `WRITES_ENABLED`,
`INSTALLER_WRITES_ENABLED`, `ALLOW_STOCK_EBUSD`, `MAX_WRITES_PER_HOUR`, `CLOCK_SYNC_ENABLED`,
`LOG_LEVEL`. Only as environment variables, in seconds: `POLL_PASSIVE_INTERVAL` (15),
`POLL_CYCLIC_INTERVAL` (60), `POLL_PROGRAM_INTERVAL` (3600: settings, counters and programs),
`POLL_FAULTS_INTERVAL` (3600: fault memory), `CLOCK_SYNC_INTERVAL` (1800). Give `-e MQTT_USERNAME=… -e MQTT_PASSWORD=…` where the broker asks for a login. Two
differences from the add-on:

- The write switches changed at run time are **not** kept over a restart unless you name a file
  for them on a volume: `-v weishaupt2mqtt:/data -e WEISHAUPT_STATE_FILE=/data/state.json`.
- A serial adapter can be passed with Docker's `--device`; that was not tried.
- A container runs in UTC unless told otherwise: give `-e TZ=<your time zone>` (for example
  `Europe/Paris`) **before any use of the clock**, or the installation would be set to UTC.

Without a container, against an ebusd you run yourself with the definitions loaded (Python 3.11 or
later; the install must be the editable one, `-e`, for the bridge to find its metadata file):

```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -e .
EBUSD_HOST=<ebusd-address> MQTT_HOST=<broker-address> python3 -m weishaupt2mqtt
```

## First start

The log of a start, here without a bus (ebusd's lines carry milliseconds and a `[bus …]` tag, the
bridge's lines `weishaupt2mqtt.bridge`):

```text
Waiting for internal ebusd on 127.0.0.1:8888...
2026-11-27 11:40:46,377 INFO weishaupt2mqtt.bridge: Connected to ebusd (26.1.p20261120), definitions 1.3.1, circuits=['broadcast'], checksum=unknown
```

- The `Connected to ebusd` line comes about **half a minute** after the start (the bridge waits for
  ebusd's scan of the bus) and names the device codes found (ebusd and the topics call them
  *circuits*; see [Words used here](#words-used-here)). With the devices answering, `circuits=`
  lists them and the line ends with `checksum=present`. A device found later by the scan is added
  by itself (`Discovered new circuits`).
- `circuits=['broadcast']` alone and `checksum=unknown`, as above, mean ebusd found no device.
  Lines `[bus error] unable to open <address>:9999` repeated every few seconds mean ebusd does not
  reach the adapter: check `ebus_device` and that nothing else holds the adapter. The bridge then
  publishes `offline` on `weishaupt/bridge/state`, and `online` by itself once ebusd has a signal.
- `Bridge state offline: <reason>` and `Bridge state online: reading again` mark each change of
  `weishaupt/bridge/state` after the start (reasons under [Check that it works](#check-that-it-works)).
- `Reading pass <n>, step <name> failed`, followed by a Python traceback, is an error inside the
  bridge: reading goes on with the next step, and the error is worth a report (see
  [Report a problem](#report-a-problem)).
- `MQTT connection error (…); reconnecting in 5 s` means the broker refuses or is not reached.
- With `ebusd_external` and no ebusd answering, the bridge stops at once with
  `EbusdConnectionError: cannot connect to ebusd`.

Under Home Assistant the log starts with `Supervisor token detected, checking for MQTT service...`.

## Check that it works

Subscribe to everything under the base topic. In Home Assistant: *Settings → Devices & services →
MQTT → Configure → Listen to a topic*, `weishaupt/#`. With the Mosquitto tools:

```bash
mosquitto_sub -h <broker-address> -u <user> -P <password> -t 'weishaupt/#' -v
```

The commands of this README leave `-u <user> -P <password>` out: add them where the broker asks for
a login (the Mosquitto broker add-on does). To publish from Home Assistant instead of a shell:
*Developer tools → Actions → `mqtt.publish`*, with the topic and the payload of the command.

What must appear, all retained (they are there at once for a new subscriber):

| Topic | Content |
|---|---|
| `weishaupt/bridge/state` | `online` while values are being read, `offline` otherwise |
| `weishaupt/bridge/info` | versions, `"bus_signal"`, `"health"`, `"circuits"`, the write switches and the budget |
| `weishaupt/bridge/metadata` | the metadata of the definitions (large) |
| `weishaupt/<circuit>/state` | one JSON object per device code, keyed by message name, with `"_updated"` |
| `weishaupt/<circuit>/program/<id>` | one per weekly program, by day |
| `weishaupt/bc1/faults`, `weishaupt/bc2/faults` | fault memory and fault counters of the boilers |

`bridge/info` with the bus connected (versions and checksum as read on the test installation;
`"circuits"` holds the device codes found on yours):

```json
{"bridge_version": "0.1.4", "definitions_version": "1.3.1", "ebusd_version": "26.1.p20261120",
 "run_time_checksum": "present", "bus_signal": true,
 "health": {"ok": true, "reason": null, "detail": null, "since": "2026-11-27T11:41:02+01:00"},
 "circuits": ["bc1", "bc2", "ka", "em1", "em2", "em3", "fs1", "fs2", "fs3", "broadcast"],
 "writes": {"enabled": false, "installer_enabled": false, "allow_stock_ebusd": false,
            "used_last_hour": 0, "max_per_hour": 30},
 "clock": {"writable": true, "last_sync": null, "outside_temp": null}}
```

The first pass reads every setting, the programs and the fault memory from the devices and is
slow: on the test installation it took about a quarter of an hour before all topics were there.
After it: live values are refreshed every 15 s from what
ebusd hears and every 60 s for the values it must ask for, settings and counters every hour,
programs and fault memory from the devices once a day. A setting changed on a controller itself
can therefore take an hour to show; `{"refresh": "<circuit>/<message>"}` on
`weishaupt/bridge/config/set` reads one at once.

**What `online` means.** `bridge/state` is `online` while the bridge is connected to the broker,
ebusd answers, ebusd has a signal on the bus and the reading loop works; otherwise `offline`.
`"health"` in `bridge/info` gives the reason and since when:

| `"reason"` | Meaning | Look at |
|---|---|---|
| `ebusd_unreachable` | ebusd does not answer | the log: did ebusd stop? |
| `no_bus_signal` | ebusd runs but hears nothing | the adapter (power, network, `ebus_device`), another program holding it, the bus |
| `reading_failed` | the last reading pass ended on an error, named in `"detail"` | the log; report it |
| `reading_stalled` | the reading loop did nothing for five minutes | the log; restart the bridge; report it |

A lost bus is noticed within about a minute, plus the time ebusd takes to give its signal up.
`bridge/info` is published again whenever one of its fields changes. For one device, `"_updated"`
in its `state` topic still carries the time of the last change of any of its values.

## Use it from Home Assistant

**With the integration [ha-weishaupt](https://github.com/aghulas/ha-weishaupt)** (1.4.0 or later):
it builds the devices, entities, the panel and the confirmations from these topics, through Home
Assistant's MQTT integration. In the entry of the integration: *⋮ → Reconfigure → Weishaupt2MQTT
(MQTT bridge)*, give the base topic (`weishaupt`), submit. Devices, entities and history are kept.

Version 1.4.1 of the integration can only **create** its entry against an ebusd it reaches over
TCP, which this bridge does not offer. A first installation therefore goes:

1. an ebusd add-on with the definitions, and the entry, as the integration's README describes
   under *First installation: reading* (take the latest definitions, 1.3.1, where that text still
   names 0.6.x);
2. this bridge, with the ebusd add-on stopped;
3. *Reconfigure* as above.

That is the path taken on the test installation, which had a working entry before the bridge
existed. Whether a lighter first step is enough (an ebusd without the definitions) was not tried.
The integration's README, *With Weishaupt2MQTT (MQTT)*, says what differs in this mode.

**Without it**, declare MQTT sensors yourself, for example:

```yaml
mqtt:
  sensor:
    - name: "Outside temperature"
      state_topic: "weishaupt/ka/state"
      value_template: "{{ value_json.OutsideTemp }}"
      unit_of_measurement: "°C"
      availability_topic: "weishaupt/bridge/state"
```

Message names are those of the definitions (the release notice of the definitions lists the
writable ones; `bridge/metadata` describes them all, with labels in English and French).

## Writing

Nothing is written unless a command arrives on a `set` topic **and** the matching switch is on.

| Level | Settings | Topic | Needs |
|---|---|---|---|
| 1, user | room normal and setback temperature, hot-water setpoint (15 settings), weekly programs | `weishaupt/<circuit>/set`, `weishaupt/<circuit>/program/<id>/set` | `writes_enabled` |
| 2, installer | every other writable setting of the system manager, heating modules and room units (heating curve, flow limits, holiday dates, legionella, cascade…) | `weishaupt/<circuit>/installer/set` | `writes_enabled` and `installer_writes_enabled`, and `"confirm": true` in the payload |
| boilers | `bc1`, `bc2` | none | read only, always |

The field `"mqtt_level"` of each message in `bridge/metadata` says `user`, `installer` or
`readonly`. The nine room-unit settings without an established range (`ControlVariation`,
`HolidayStartThird`, `HolidayEndThird`) stay read only.

### Switch writes on and off

```bash
mosquitto_pub -h <broker-address> -q 1 -t weishaupt/bridge/config/set -m '{"writes_enabled": true}'
mosquitto_pub -h <broker-address> -q 1 -t weishaupt/bridge/config/set -m '{"installer_writes_enabled": true}'
mosquitto_pub -h <broker-address> -q 1 -t weishaupt/bridge/config/set -m '{"writes_enabled": false}'
```

The answer comes on `weishaupt/bridge/result` (`"status": "ok", "changed": {…}`) and
`bridge/info` shows the new state under `"writes"`. With the integration, its **Writes** switch
sends the first and the third of these; switching it off also switches the installer level off,
and nothing in the integration switches the installer level on: send the second command yourself
before changing an installer-level setting from the integration, and again after each time its
**Writes** switch was off. The add-on option `installer_writes_enabled` only counts until the
first change at run time.

### Write a setting

Subscribe to the results first, then publish:

```bash
mosquitto_sub -h <broker-address> -t 'weishaupt/+/result' -t weishaupt/bridge/journal -v
mosquitto_pub -h <broker-address> -q 1 -t weishaupt/fs1/set -m '{"RoomNormalTemp": 20.5, "correlation_id": "a1"}'
mosquitto_pub -h <broker-address> -q 1 -t weishaupt/fs1/installer/set -m '{"SupplyMaxTemp": 75.0, "confirm": true}'
```

**Which copy to write.** A room unit (`fs1`–`fs3`) and the heating module of the same circuit
(`em1`–`em3`) each hold a copy of the circuit settings (curve, flow limits, room setpoints). On
the test installation the regulation follows the room unit's copy; changing the heating module's
copy did not move the flow setpoint. Write the `fs` one unless you know your installation differs
(notice of the definitions, *Known limits*).

For each setting the bridge: checks level, topic, step and range without touching the bus; reads
the present value from the device (and the linked setting: setback stays at or below normal, flow
minimum at or below maximum); writes; reads back from the device. The result, on
`weishaupt/<circuit>/result` and on `weishaupt/bridge/journal`:

```json
{"timestamp": "…", "correlation_id": "a1", "kind": "setting", "circuit": "fs1",
 "target": "RoomNormalTemp", "requested": 20.5, "source": "ff", "status": "ok",
 "before": 21.0, "after": 20.5}
```

| `status` | Meaning |
|---|---|
| `ok` | written, and read back with the requested value |
| `unchanged` | the device already held the value; nothing sent |
| `confirmation_required` | installer-level setting without `"confirm": true`; nothing sent; the answer carries `current`, `min`, `max`, `unit` |
| `refused` | blocked before any write; `reason` and `detail` say why |
| `not_taken` | sent and acknowledged, but the read-back shows another value |
| `error` | ebusd or the bus failed |

Refusals you will meet: `writes_disabled`, `installer_writes_disabled`,
`installer_topic_required` (an installer-level setting sent to `/set`), `wrong_level_topic` (a
user-level setting sent to `/installer/set`), `out_of_range`, `invalid_step`, `linked_bound`,
`rate_limit`, `register_interval`, `read_only` (also the answer for a name the bridge does not
know, and for any setting while the device was not found), `shared_with` (the value is kept by
another device: write it there), `invalid_payload`.

Results and the journal are **not retained**: a subscriber that connects later does not see them.

### Write a weekly program

Program ids: `heating1`, `heating2`, `heating3`, `dhw_stored`, `circulation_stored` on the room
units; `dhw`, `circulation` on `em1`. The number in `heating1`–`heating3` is the number of the
program in the room unit, which holds three heating programs to choose from; it is not a heating
circuit (the circuit is the `fs1`–`fs3` of the topic). On the test installation hot water and
circulation follow the programs of `em1`; the `…_stored` ones of the room units are kept there
and not used (notice of the definitions). The retained topic `weishaupt/<circuit>/program/<id>` shows
each one with `"writable"`; on the test installation the programs of `fs2` are kept by `fs1`
(`"shared_with"`) and written there.

```bash
mosquitto_pub -h <broker-address> -q 1 -t weishaupt/fs1/program/heating1/set -m '{"days": {"1": ["06:30-18:00"], "6": []}}'
```

Days are `"1"` (Monday) to `"7"`; a day is a list of at most three `HH:MM-HH:MM` periods on
quarter hours, not overlapping; `[]` is off all day; days not named are left alone. Only the slots
that differ are written, and none is written if they do not all fit in the hourly budget
(`program_limit`).

### Set the date and time

The system manager broadcasts the date and time once a minute and takes over the time of any such
broadcast it hears. The bridge can send one, carrying the clock of the machine it runs on:

```bash
mosquitto_pub -h <broker-address> -q 1 -t weishaupt/bridge/config/set -m '{"sync_clock": true}'
```

- **Check the bridge's clock and time zone first**: the whole installation takes them, programs
  included. Home Assistant gives its time zone to its add-ons; with Docker, see
  [Install with Docker](#install-with-docker). Either way, compare `"requested"` in the first
  answer with your own clock.
- It needs `writes_enabled` and counts as one write of the hourly budget. `"clock"` in
  `bridge/info` must show `"writable": true` (ebusd loaded the write line of the broadcast, which
  it does once it has found the devices).
- The broadcast goes out at second 00 of the next minute, so the answer on
  `weishaupt/bridge/result` (and the entry on `weishaupt/bridge/journal`, `"kind": "clock"`) comes
  up to a minute after the command. Other commands wait meanwhile.
- Nothing is sent, and the answer is `refused`, when ebusd has not heard a broadcast of the system
  manager yet (`clock_unknown`: wait a minute after a start), or when the broadcast could not go
  out within 2 s of second 00 (`clock_late`: sent late it would set the installation behind; ask
  again).
- The option `clock_sync_enabled` does the same every 30 minutes, the first time 30 minutes after
  the start. With writes off each attempt is refused (`writes_disabled`) and nothing else happens.
- To undo: there is nothing to restore; set the time again, here or on the system manager.

**Not checked on a bus in this version.** The telegram and the timing are those the integration
[ha-weishaupt](https://github.com/aghulas/ha-weishaupt) uses for its *set clock* button, which was
checked on the test installation; the bridge's own sending was only run against a simulated ebusd.
Watch the time shown on a room unit after the first use.

### The guards, in one place

- Two switches, both off by default; the installer level needs both, and a `"confirm": true`.
- Boiler automats are never written. Structural settings (circuit type, boiler sequence…) are
  never written.
- Range, step and linked bounds are checked against the device's present values.
- Every write is read back from the device; `ok` means the device holds the value.
- At most `max_writes_per_hour` writes in any hour (default 30), and at least 10 s between two
  writes of the same register. The count is kept in memory: a restart clears it.
- ebusd accepts from the bridge only reads and two forms of write (one 16-bit register, the clock
  broadcast), each matched against a write line of the definitions before it is sent.
- **Do not write while the vendor's service tool is connected to the bus**: both use the address
  `ff`.

## Undo, and going back

- **A setting:** write the old value the same way. It is the `"before"` of the result; note it,
  because results are not retained. The present value is also in the retained
  `weishaupt/<circuit>/state` before you write.
- **A program:** note the retained `weishaupt/<circuit>/program/<id>` before you write, and send
  its `"days"` back.
- **Stop all writing:** `{"writes_enabled": false}` on `weishaupt/bridge/config/set`, then check
  `"writes"` in `bridge/info`. Setting the add-on option to `false` is not enough once the switch
  was changed at run time (the stored value wins).
- **Stop the bridge:** stop the add-on or the container. Nothing stays on the heating system: the
  controllers go on by themselves with the settings they hold. The retained topics stay in the
  broker with their last values and `bridge/state` turns `offline`.
- **Remove the bridge:** stop and uninstall the add-on (or remove the container and its volume);
  that also deletes the stored switches. To clear the retained topics from the broker:
  `mosquitto_sub -h <broker-address> -t 'weishaupt/#' --remove-retained -W 5` (it clears every
  retained message it receives during five seconds; the option exists in the Mosquitto 2 tools,
  and was not run against the test installation). The add-on repository can then be removed from
  the store.
- **Back to a plain ebusd:** stop the bridge, start your ebusd add-on again; in the integration,
  *⋮ → Reconfigure → ebusd (the list, and which one to use)*.
- **Back to an earlier version of the add-on:** the store offers only the latest. Make a backup of
  the add-on (Home Assistant backup) before an update and restore it.

## Known limits

- **One installation checked** (see the top).
- **Clock: sending not checked on a bus in 0.1.4** (see
  [Set the date and time](#set-the-date-and-time)). It is written from the user-level switch and
  without a confirmation, although the definitions class it as an installer-level setting. Up to
  0.1.3 the option `clock_sync_enabled` stopped all reading after the first pass and the command
  `sync_clock` failed: do not use the clock with those versions.
- **Health follows ebusd's word.** `bridge/state` turns `offline` when ebusd says it has no
  signal, about a minute after ebusd noticed. A device that stops answering while the bus is up
  does not change it: its values simply stop moving (`"_updated"`).
- **Settings are read once an hour** (every 5 minutes up to 0.1.3, by mistake): a change made on a
  controller shows late unless you ask for a `refresh`.
- **Network adapters only as an add-on**; no TLS to the broker; no MQTT discovery.
- **Integration:** version 1.4.1 cannot create its entry over MQTT alone (see above).
- **Room unit 2, two copies of each circuit setting, absent sensors, the vendor's service tool:**
  the limits of the definitions apply as their release notice gives them.
- `docs/automation-school-holidays.yaml` is an untested sketch: it sends the holiday dates to
  `/set`, where the bridge refuses them (`installer_topic_required`); they are installer-level.

## Words used here

- **Circuit**, in topics, options and logs: the code ebusd gives a device, not a heating circuit.
  `bc1`, `bc2`: the boiler automats (the burner control units of boilers 1 and 2). `ka`: the
  system manager (WCM-KA, also called cascade manager). `em1`: heating circuit 1, the system
  manager's own direct circuit; `em2`, `em3`: the heating modules (WCM-EM) of circuits 2 and 3.
  `fs1`–`fs3`: the room units (WCM-FS) by the circuit they are addressed for. `broadcast`: messages
  sent to all, such as the date and time. An installation with two heating modules therefore shows
  `em1`, `em2` and `em3`.
- **User / installer level.** The same split under three names: *user* here is *comfort* in the
  integration and *End user level* in the contract and the vendor's manuals; *installer* is
  *regulation*, and *Heating engineer level*. *Structural* settings are never written.
- **`ebus_device`, `ens:`** : how ebusd names its adapter (`--device`). `ens:<address>:<port>` is
  a network adapter speaking ebusd's enhanced protocol at high speed; your adapter's documentation
  gives its form, and <https://github.com/john30/ebusd/wiki/2.-Run> lists them all.
- **`"source"`** in a result: the bus address the write was sent from, `ff` (the address of the
  vendor's service tool, from which the controllers take most settings) or `31` (ebusd's own). The
  bridge chooses it from the definitions.
- **Checksum (`checksum=present`, `"run_time_checksum"`).** A write telegram to these controllers
  carries a checksum; the bundled ebusd computes it (`present`). An official ebusd does not
  (`absent`) and sends the fixed byte of the definitions, the *placeholder*, which the system
  manager, heating modules and room units accept: that is what `allow_stock_ebusd` permits.
- **Linked bound:** a limit that depends on another setting (setback at or below normal).
  **`shared_with`:** the value is kept by another device and written there. **`stored_only`:** a
  program the device keeps but does not use.
- **`correlation_id`:** any text you choose; the result carries it back, to match answers to
  commands. **`-q 1`:** MQTT quality of service 1, delivery acknowledged by the broker.
- **Retained:** the broker keeps the last message of the topic and gives it to every new
  subscriber.

## Report a problem

On <https://github.com/aghulas/weishaupt-ebus/issues>, with the bridge version, the payload of
`weishaupt/bridge/info`, the lines of the log around the problem, what you published and the
result you got. Replace the adapter's and the broker's addresses.

## Development

```bash
python3 -m unittest discover -s tests -v
```

The tests run against the bundled metadata, without a bus and without a broker.
