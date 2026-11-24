"""Core Weishaupt engine: ebusd polling, decoding, and guarded level-1/level-2 writes."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from . import __version__, clock, faults
from .config import Config
from .ebusd import (
    EbusdClient,
    EbusdConnectionError,
    EbusdError,
    EbusdInfo,
    EbusdNoData,
    EbusdNotFound,
)
from .guard import WriteGuard, WriteRefused
from .model import Field, Message, Model
from .programs import (
    DayError,
    Program,
    SlotSpec,
    _time,
    parse_day,
    program_specs,
    range_of_raw,
    raw_of_fields,
    raws_of_day,
)
from .writes import WriteSpec, capability, linked, write_specs

_LOGGER = logging.getLogger(__name__)
PROBE_SETTING = "ka/BlockingTime"
BOILER_CIRCUITS = frozenset({"bc1", "bc2"})


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _parse_scalar(field: Field, raw_text: str) -> Any:
    """Convert one ebusd field string to a clean JSON scalar (None for absent/replacement)."""
    text = raw_text.strip()
    if not text or text == "-":
        return None
    if field.values is not None:
        # Boolean-like 0=off / 1=on stays string or bool? Keep ebusd value name for value lists
        # except pure on/off? Keep ebusd value name (matches metadata value_labels) — clean & unambiguous.
        return text
    try:
        num = float(text)
    except ValueError:
        return text
    if field.absent is not None and abs(num - field.absent) < 1e-6:
        return None
    if num.is_integer() and (field.divisor is None or field.divisor == 1) and (field.step is None or field.step >= 1):
        return int(num)
    return num


def decode_message(message: Message, raw_values: tuple[str, ...]) -> tuple[bool, Any]:
    """Decode an ebusd reading for `message`.

    Returns `(valid, decoded)`. `valid` is False when a `guard` field (`firstbyte`) is non-zero
    (the caller must discard the reading and keep the previous value).
    """
    for field in message.fields:
        if field.guard and field.index < len(raw_values):
            val = raw_values[field.index].strip()
            if val not in ("0", "0.0"):
                return False, None

    usable = [f for f in message.fields if not f.skip and not f.guard]
    if not usable:
        return True, None
    if len(usable) == 1:
        f = usable[0]
        raw = raw_values[f.index] if f.index < len(raw_values) else ""
        return True, _parse_scalar(f, raw)

    out: dict[str, Any] = {}
    for f in usable:
        raw = raw_values[f.index] if f.index < len(raw_values) else ""
        out[f.name] = _parse_scalar(f, raw)
    return True, out


def _ranges_to_list(ranges: list[tuple[int, int]]) -> list[str]:
    return [f"{_time(s)}-{_time(e)}" for s, e in ranges]


class WeishauptEngine:
    """Stateful Weishaupt controller sitting between `EbusdClient` and the MQTT bridge."""

    def __init__(self, config: Config, client: EbusdClient | None = None) -> None:
        self.config = config
        self.client = client or EbusdClient(config.ebusd_host, config.ebusd_port, timeout=config.ebusd_timeout)
        self.raw_metadata: dict[str, Any] = json.loads(Path(config.metadata_path).read_text(encoding="utf-8"))
        self.model: Model = Model.from_dict(self.raw_metadata)

        self.guard = WriteGuard(
            writes_enabled=config.writes_enabled,
            installer_writes_enabled=config.installer_writes_enabled,
            max_per_hour=config.max_writes_per_hour,
        )
        self._load_persisted_switches()

        self.info: EbusdInfo | None = None
        self.present_circuits: list[str] = []
        self.own_address: str | None = None
        self.checksum_capability: str = "unknown"
        self.specs: dict[str, WriteSpec] = {}
        self.placeholders: dict[str, str] = {}
        self.programs: dict[str, Program] = {}
        self.slot_specs: dict[str, SlotSpec] = {}

        # Latest decoded state per circuit / program / faults
        self.states: dict[str, dict[str, Any]] = {}
        self.programs_state: dict[str, dict[str, dict[str, Any]]] = {}
        self.faults_state: dict[str, dict[str, Any]] = {}
        self.last_clock: clock.Broadcast | None = None
        self.last_clock_sync: str | None = None

        # Pre-index program groups by (circuit, group_id)
        self._groups_by_circuit: dict[str, dict[str, Any]] = {}
        for group in self.model.groups.values():
            if group.family in ("program", "dhw_program", "circ_program"):
                self._groups_by_circuit.setdefault(group.circuit, {})[group.id] = group

    def _load_persisted_switches(self) -> None:
        path = self.config.state_file
        if path is None or not path.is_file():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if isinstance(data.get("writes_enabled"), bool):
            self.guard.writes_enabled = data["writes_enabled"]
        if isinstance(data.get("installer_writes_enabled"), bool):
            self.guard.installer_writes_enabled = data["installer_writes_enabled"]

    def _save_persisted_switches(self) -> None:
        path = self.config.state_file
        if path is None:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "writes_enabled": self.guard.writes_enabled,
                        "installer_writes_enabled": self.guard.installer_writes_enabled,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        except OSError as err:
            _LOGGER.warning("Could not persist switch state to %s: %s", path, err)

    async def start(self, *, wait_for_slaves: bool = False) -> None:
        """Query ebusd info, load definitions for present circuits, and probe write capability."""
        self.info = await self.client.info()
        slave_addresses = {
            a.address for a in self.info.addresses.values() if a.role == "slave" and not a.own
        }
        if wait_for_slaves and 0 < len(slave_addresses) < 6:
            for _ in range(5):
                await asyncio.sleep(2.0)
                self.info = await self.client.info()
                new_slaves = {
                    a.address for a in self.info.addresses.values() if a.role == "slave" and not a.own
                }
                if len(new_slaves) >= 6 or len(new_slaves) == len(slave_addresses):
                    slave_addresses = new_slaves
                    break
                slave_addresses = new_slaves

        present_devices = self.model.present_devices(slave_addresses)
        circuits: list[str] = []
        for dev in present_devices.values():
            for c in dev.circuits:
                if c.circuit not in circuits:
                    circuits.append(c.circuit)
        if "broadcast" not in circuits and "broadcast" in {m.circuit for m in self.model.messages.values()}:
            circuits.append("broadcast")
        self.present_circuits = circuits

        for addr in self.info.addresses.values():
            if addr.own and addr.role == "master":
                self.own_address = addr.address
                break

        loaded: dict[str, list[Any]] = {}
        for circuit in self.present_circuits:
            try:
                loaded[circuit] = await self.client.definitions(circuit, write=True)
            except EbusdError as err:
                _LOGGER.debug("Could not load write definitions for %s: %s", circuit, err)
                loaded[circuit] = []

        self.specs, _not_writable, self.placeholders = write_specs(self.model, loaded)
        self.programs, self.slot_specs, _prog_issues = program_specs(self.model, loaded)

        # Probe run-time checksum capability without writing to the bus
        probe_spec = self.specs.get(PROBE_SETTING) or next(iter(self.specs.values()), None)
        if probe_spec is not None and probe_spec.key in self.placeholders:
            try:
                val = probe_spec.minimum + probe_spec.step
                if val > probe_spec.maximum:
                    val = probe_spec.minimum
                correct = await self.client.probe_write(probe_spec.telegram(val))
                placeholder = await self.client.probe_write(
                    probe_spec.placeholder_telegram(val, self.placeholders[probe_spec.key])
                )
                self.checksum_capability = capability(correct, placeholder)
            except EbusdError as err:
                _LOGGER.warning("Checksum capability probe failed: %s", err)
                self.checksum_capability = "unknown"
        else:
            self.checksum_capability = "unknown"

    async def refresh_devices(self) -> list[str]:
        """Check for newly discovered slave devices and load definitions if any new circuit appears."""
        try:
            self.info = await self.client.info()
        except EbusdError:
            return []
        slave_addresses = {
            a.address for a in self.info.addresses.values() if a.role == "slave" and not a.own
        }
        present_devices = self.model.present_devices(slave_addresses)
        new_circuits: list[str] = []
        for dev in present_devices.values():
            for c in dev.circuits:
                if c.circuit not in self.present_circuits:
                    self.present_circuits.append(c.circuit)
                    new_circuits.append(c.circuit)
        if new_circuits:
            loaded: dict[str, list[Any]] = {}
            for circuit in self.present_circuits:
                try:
                    loaded[circuit] = await self.client.definitions(circuit, write=True)
                except EbusdError:
                    loaded[circuit] = []
            self.specs, _not_writable, self.placeholders = write_specs(self.model, loaded)
            self.programs, self.slot_specs, _prog_issues = program_specs(self.model, loaded)
            _LOGGER.info("Discovered new circuits: %s (total %s)", new_circuits, self.present_circuits)
        return new_circuits

    def bridge_info(self) -> dict[str, Any]:
        """Payload for `weishaupt/bridge/info`."""
        return {
            "bridge_version": __version__,
            "definitions_version": self.model.version,
            "ebusd_version": self.info.version if self.info else None,
            "run_time_checksum": self.checksum_capability,
            "bus_signal": self.info.signal if self.info else False,
            "circuits": list(self.present_circuits),
            "writes": {
                "enabled": self.guard.writes_enabled,
                "installer_enabled": self.guard.installer_writes_enabled,
                "allow_stock_ebusd": self.config.allow_stock_ebusd,
                "used_last_hour": self.guard.used_last_hour,
                "max_per_hour": self.guard.max_per_hour,
            },
            "clock": {
                "last_sync": self.last_clock_sync,
                "outside_temp": self.last_clock.outside_temp if self.last_clock else None,
            },
        }

    def filtered_metadata(self) -> dict[str, Any]:
        """Schema-1 metadata enriched with write access level (`user` / `installer` / `readonly`)."""
        meta = json.loads(json.dumps(self.raw_metadata))
        for key, msg in meta.get("messages", {}).items():
            spec = self.specs.get(key)
            if spec is not None:
                msg["mqtt_level"] = "installer" if spec.risk == "regulation" else "user"
            else:
                msg["mqtt_level"] = "readonly"
        return meta

    async def poll_messages(self, poll_classes: tuple[str, ...]) -> list[str]:
        """Poll messages matching `poll_classes` across present circuits. Returns changed circuits."""
        changed: list[str] = []
        for circuit in self.present_circuits:
            circuit_state = self.states.setdefault(circuit, {})
            circuit_changed = False
            for msg in self.model.messages.values():
                if msg.circuit != circuit:
                    continue
                if msg.kind == "program_slot" or msg.family == "fault_history":
                    continue
                if msg.poll not in poll_classes:
                    continue
                try:
                    max_age = 3600 if msg.passive else int(self.config.poll_cyclic_interval)
                    raw = await self.client.read(circuit, msg.name, max_age=max_age)
                except (EbusdNoData, EbusdNotFound):
                    continue
                except EbusdError as err:
                    _LOGGER.debug("Read %s failed: %s", msg.key, err)
                    continue
                valid, decoded = decode_message(msg, raw)
                if not valid:
                    continue
                if circuit_state.get(msg.name) != decoded:
                    circuit_state[msg.name] = decoded
                    circuit_changed = True
            if circuit_changed:
                circuit_state["_updated"] = _now_iso()
                changed.append(circuit)
        return changed

    async def poll_programs(self) -> list[tuple[str, str]]:
        """Read weekly schedules for present circuits. Returns changed `(circuit, program_id)` pairs."""
        changed: list[tuple[str, str]] = []
        for circuit in self.present_circuits:
            groups = self._groups_by_circuit.get(circuit, {})
            for prog_id, group in groups.items():
                days_map: dict[str, list[str]] = {}
                shared_with: str | None = None
                stored_only = False
                for day in range(1, 8):
                    ranges: list[tuple[int, int]] = []
                    for slot in range(1, 4):
                        idx = (day - 1) * 3 + (slot - 1)
                        if idx >= len(group.members):
                            continue
                        key = group.members[idx]
                        msg = self.model.messages.get(key)
                        if msg is None:
                            continue
                        if msg.shared_with:
                            shared_with = msg.shared_with.rsplit("/", 1)[0] + "/" + prog_id
                        if msg.write and getattr(msg.write, "stored_only", False):
                            stored_only = True
                        try:
                            raw_fields = await self.client.read(circuit, msg.name, max_age=self.config.program_cache_age)
                        except EbusdError:
                            continue
                        raw_val = raw_of_fields(raw_fields)
                        if raw_val is not None:
                            r = range_of_raw(raw_val)
                            if r is not None:
                                ranges.append(r)
                    ranges.sort()
                    days_map[str(day)] = _ranges_to_list(ranges)

                prog_key = f"{circuit}/{prog_id}"
                payload = {
                    "circuit": circuit,
                    "program": prog_id,
                    "writable": prog_key in self.programs and shared_with is None,
                    "shared_with": shared_with,
                    "stored_only": stored_only,
                    "days": days_map,
                }
                prev = self.programs_state.setdefault(circuit, {}).get(prog_id)
                prev_cmp = {k: v for k, v in prev.items() if k != "_updated"} if prev else None
                if prev_cmp != payload:
                    payload["_updated"] = _now_iso()
                    self.programs_state[circuit][prog_id] = payload
                    changed.append((circuit, prog_id))
        return changed

    async def poll_faults(self) -> list[str]:
        """Read boiler fault history and counters (`bc1`, `bc2`). Returns changed circuits."""
        changed: list[str] = []
        for circuit in self.present_circuits:
            if circuit not in BOILER_CIRCUITS:
                continue
            raw_map: dict[str, tuple[str, ...]] = {}
            for msg in self.model.messages.values():
                if msg.circuit != circuit:
                    continue
                if msg.family in ("fault_history", "fault_counters") or msg.name == "FaultPointer":
                    try:
                        raw_map[msg.key] = await self.client.read(circuit, msg.name, max_age=self.config.faults_cache_age)
                    except EbusdError:
                        continue

            hist_group = next(
                (g for g in self.model.groups.values() if g.circuit == circuit and g.family == "fault_history"),
                None,
            )
            pointer_vals = raw_map.get(f"{circuit}/FaultPointer")
            pointer = pointer_vals[0] if pointer_vals else None
            history_list = faults.history(self.model, raw_map.get, hist_group, pointer) if hist_group else []
            counters_list = faults.counters(self.model, raw_map.get, circuit)

            payload = {
                "circuit": circuit,
                "history": history_list or [],
                "counters": counters_list,
            }
            prev = self.faults_state.get(circuit)
            prev_cmp = {k: v for k, v in prev.items() if k != "_updated"} if prev else None
            if prev_cmp != payload:
                payload["_updated"] = _now_iso()
                self.faults_state[circuit] = payload
                changed.append(circuit)
        return changed

    def _resolve_requested_value(self, spec: WriteSpec, value: Any) -> float:
        """Normalize a JSON value (number or value-list name) into the float expected by `WriteSpec`."""
        if spec.values is not None:
            if isinstance(value, bool):
                target_name = "on" if value else "off"
                for raw, name in spec.values.items():
                    if name.lower() == target_name:
                        return float(raw)
                raise WriteRefused("out_of_range", value=value, minimum=min(spec.values), maximum=max(spec.values))
            if isinstance(value, str):
                for raw, name in spec.values.items():
                    if name == value or name.lower() == value.lower():
                        return float(raw)
                raise WriteRefused("invalid_payload", reason=f"unknown value {value!r} for {spec.name}")
            if isinstance(value, (int, float)) and int(value) in spec.values:
                return float(int(value))
            raise WriteRefused("out_of_range", value=value, minimum=min(spec.values), maximum=max(spec.values))

        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise WriteRefused("invalid_payload", reason=f"expected number for {spec.name}, got {value!r}")
        return float(value)

    def _telegram_for(self, spec: WriteSpec, value: float) -> str:
        if self.checksum_capability == "present":
            return spec.telegram(value)
        if self.checksum_capability == "absent":
            if not self.config.allow_stock_ebusd or spec.circuit in BOILER_CIRCUITS:
                raise WriteRefused("stock_ebusd_not_allowed")
            placeholder = self.placeholders.get(spec.key)
            if not placeholder:
                raise WriteRefused("capability_unknown", reason="missing placeholder checksum")
            return spec.placeholder_telegram(value, placeholder)
        raise WriteRefused("capability_unknown", reason="probe did not succeed")

    def _display_value(self, spec: WriteSpec, raw: int) -> Any:
        if spec.values is not None:
            return spec.values.get(raw, raw)
        val = spec.value_of(raw)
        return int(val) if spec.decimals == 0 else round(val, spec.decimals)

    async def write_setting(
        self,
        circuit: str,
        name: str,
        requested: Any,
        *,
        topic_level: Literal["user", "installer"],
        confirm: bool = False,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute a guarded setting write according to docs/mqtt-contract.md."""
        key = f"{circuit}/{name}"
        base: dict[str, Any] = {
            "timestamp": _now_iso(),
            "correlation_id": correlation_id,
            "kind": "setting",
            "circuit": circuit,
            "target": name,
            "requested": requested,
        }

        msg = self.model.messages.get(key)
        if msg is None:
            return {**base, "status": "refused", "reason": "read_only", "detail": f"unknown message {key}"}
        if msg.shared_with:
            return {
                **base,
                "status": "refused",
                "reason": "shared_with",
                "detail": f"stored on {msg.shared_with}",
            }
        spec = self.specs.get(key)
        if spec is None or circuit in BOILER_CIRCUITS:
            return {**base, "status": "refused", "reason": "read_only"}

        is_installer = spec.risk == "regulation"
        try:
            # 1. Level-topic separation (Option B)
            if is_installer and topic_level != "installer":
                raise WriteRefused("installer_topic_required", setting=key)
            if not is_installer and topic_level != "user":
                raise WriteRefused("wrong_level_topic", setting=key)

            # 2. Switch check
            self.guard.check_level(is_installer=is_installer)

            # 3. Value parsing & static range/step
            num_val = self._resolve_requested_value(spec, requested)
            if not spec.on_step(num_val):
                raise WriteRefused("invalid_step", value=requested, step=spec.step, minimum=spec.minimum)
            if not (spec.minimum <= num_val <= spec.maximum):
                raise WriteRefused("out_of_range", value=requested, minimum=spec.minimum, maximum=spec.maximum)

            # 4. Installer confirmation flag
            if is_installer and not confirm:
                current_cached = self.states.get(circuit, {}).get(name)
                return {
                    **base,
                    "status": "confirmation_required",
                    "reason": "regulation_setting_requires_confirm",
                    "current": current_cached,
                    "min": spec.minimum,
                    "max": spec.maximum,
                    "unit": spec.unit,
                }

            # 5. Source address & checksum capability
            if spec.ebusd_source is None and self.own_address and self.own_address != "31":
                raise WriteRefused("source_not_own", setting=key, own=self.own_address)
            telegram = self._telegram_for(spec, num_val)

            # 6. Pre-read target & linked bounds
            try:
                before_vals = await self.client.read_from_bus(circuit, name)
            except EbusdError as err:
                raise WriteRefused("read_failed", setting=key, reason=str(err)) from err
            before_raw = spec.raw_of_values(before_vals)
            if before_raw is None:
                raise WriteRefused("read_failed", setting=key, reason=f"unparseable {before_vals!r}")
            before_display = self._display_value(spec, before_raw)

            low_bound, high_bound = spec.minimum, spec.maximum
            linked_used = False
            # linked() returns (must_stay_above, must_stay_below)
            must_stay_above, must_stay_below = linked(spec, self.specs)
            if must_stay_above is not None:
                try:
                    other_vals = await self.client.read_from_bus(must_stay_above.circuit, must_stay_above.name)
                    other_raw = must_stay_above.raw_of_values(other_vals)
                except EbusdError:
                    other_raw = None
                if other_raw is None:
                    raise WriteRefused("linked_unknown", other=must_stay_above.key)
                low_bound = max(low_bound, must_stay_above.value_of(other_raw))
                linked_used = True
            if must_stay_below is not None:
                try:
                    other_vals = await self.client.read_from_bus(must_stay_below.circuit, must_stay_below.name)
                    other_raw = must_stay_below.raw_of_values(other_vals)
                except EbusdError:
                    other_raw = None
                if other_raw is None:
                    raise WriteRefused("linked_unknown", other=must_stay_below.key)
                high_bound = min(high_bound, must_stay_below.value_of(other_raw))
                linked_used = True

            requested_raw = spec.raw(num_val)
            if before_raw == requested_raw:
                return {**base, "status": "unchanged", "before": before_display, "after": before_display}

            # 7. Guard check (bounds + rate limit)
            self.guard.check_register(
                spec.register_id,
                num_val,
                low_bound,
                high_bound,
                is_installer=is_installer,
                linked=linked_used,
            )

            # 8. Send write
            await self.client.write_register(circuit, telegram, spec.ebusd_source)
            self.guard.record(spec.register_id)

            # 9. Read-back verification
            try:
                after_vals = await self.client.read_from_bus(circuit, name)
                after_raw = spec.raw_of_values(after_vals)
            except EbusdError as err:
                return {
                    **base,
                    "source": spec.source,
                    "status": "error",
                    "reason": f"read_back_failed: {err}",
                    "before": before_display,
                }

            after_display = self._display_value(spec, after_raw) if after_raw is not None else None
            if after_raw == requested_raw:
                self.states.setdefault(circuit, {})[name] = after_display
                self.states[circuit]["_updated"] = _now_iso()
                return {
                    **base,
                    "source": spec.source,
                    "status": "ok",
                    "before": before_display,
                    "after": after_display,
                }
            return {
                **base,
                "source": spec.source,
                "status": "not_taken",
                "before": before_display,
                "after": after_display,
            }

        except WriteRefused as err:
            return {**base, "status": "refused", "reason": err.code, "detail": str(err)}
        except EbusdConnectionError as err:
            return {**base, "status": "error", "reason": "unavailable", "detail": str(err)}
        except EbusdError as err:
            return {**base, "status": "error", "reason": "ebusd_error", "detail": str(err)}

    async def write_program(
        self,
        circuit: str,
        program_id: str,
        days_payload: dict[str, Any],
        *,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Write one or more days of a weekly program with upfront budget check and minimal diff."""
        prog_key = f"{circuit}/{program_id}"
        base: dict[str, Any] = {
            "timestamp": _now_iso(),
            "correlation_id": correlation_id,
            "kind": "program",
            "circuit": circuit,
            "target": program_id,
        }

        prog = self.programs.get(prog_key)
        if prog is None:
            # Check if it's a shared store
            existing = self.programs_state.get(circuit, {}).get(program_id)
            if existing and existing.get("shared_with"):
                return {**base, "status": "refused", "reason": "shared_with", "detail": f"stored on {existing['shared_with']}"}
            return {**base, "status": "refused", "reason": "read_only"}

        try:
            self.guard.check_level(is_installer=False)
            if not isinstance(days_payload, dict) or not days_payload:
                raise WriteRefused("invalid_payload", reason="'days' must be a non-empty object")

            # 1. Validate all days upfront
            parsed_days: dict[int, list[int]] = {}
            for day_key, spec_val in days_payload.items():
                try:
                    day_num = int(day_key)
                except ValueError as err:
                    raise WriteRefused("invalid_payload", reason=f"invalid day {day_key!r}") from err
                if not 1 <= day_num <= 7:
                    raise WriteRefused("invalid_payload", reason=f"day must be 1..7, got {day_num}")
                if isinstance(spec_val, list):
                    text = ", ".join(str(item) for item in spec_val) if spec_val else "-"
                elif isinstance(spec_val, str):
                    text = spec_val
                else:
                    raise WriteRefused("invalid_payload", reason=f"day {day_num} must be list of ranges or string")
                try:
                    ranges = parse_day(text)
                except DayError as err:
                    raise WriteRefused(f"bad_day_{err.code}", day=day_num, part=err.part) from err
                parsed_days[day_num] = raws_of_day(ranges)

            # 2. Read current slots and build minimal diff
            to_write: list[tuple[SlotSpec, int]] = []
            unchanged_count = 0
            for day_num, target_raws in sorted(parsed_days.items()):
                day_slot_keys = prog.slots[day_num]
                for slot_key, target_raw in zip(day_slot_keys, target_raws):
                    slot_spec = self.slot_specs[slot_key]
                    try:
                        cur_vals = await self.client.read_from_bus(circuit, slot_spec.name)
                    except EbusdError as err:
                        raise WriteRefused("read_failed", setting=slot_spec.key, reason=str(err)) from err
                    cur_raw = slot_spec.raw_of_values(cur_vals)
                    if cur_raw == target_raw:
                        unchanged_count += 1
                    else:
                        to_write.append((slot_spec, target_raw))

            if not to_write:
                return {**base, "status": "unchanged", "slots_written": 0, "slots_unchanged": unchanged_count}

            # 3. All-or-nothing budget check
            self.guard.check_budget(len(to_write))

            # 4. Write each changed slot and read back
            written = 0
            for slot_spec, target_raw in to_write:
                telegram = self._telegram_for(slot_spec, float(target_raw))
                await self.client.write_register(circuit, telegram, slot_spec.ebusd_source)
                self.guard.record(slot_spec.register_id)
                after_vals = await self.client.read_from_bus(circuit, slot_spec.name)
                if slot_spec.raw_of_values(after_vals) != target_raw:
                    return {
                        **base,
                        "status": "not_taken",
                        "slots_written": written,
                        "slots_unchanged": unchanged_count,
                        "failed_slot": slot_spec.name,
                    }
                written += 1

            return {
                **base,
                "status": "ok",
                "slots_written": written,
                "slots_unchanged": unchanged_count,
            }

        except WriteRefused as err:
            return {**base, "status": "refused", "reason": err.code, "detail": str(err)}
        except EbusdError as err:
            return {**base, "status": "error", "reason": "ebusd_error", "detail": str(err)}

    async def handle_bridge_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Handle `weishaupt/bridge/config/set` commands."""
        corr = payload.get("correlation_id")
        base: dict[str, Any] = {
            "timestamp": _now_iso(),
            "correlation_id": corr,
            "kind": "bridge_config",
        }
        changed: dict[str, Any] = {}

        if "writes_enabled" in payload:
            if not isinstance(payload["writes_enabled"], bool):
                return {**base, "status": "refused", "reason": "invalid_payload", "detail": "writes_enabled must be bool"}
            self.guard.writes_enabled = payload["writes_enabled"]
            changed["writes_enabled"] = self.guard.writes_enabled
            self._save_persisted_switches()

        if "installer_writes_enabled" in payload:
            if not isinstance(payload["installer_writes_enabled"], bool):
                return {**base, "status": "refused", "reason": "invalid_payload", "detail": "installer_writes_enabled must be bool"}
            self.guard.installer_writes_enabled = payload["installer_writes_enabled"]
            changed["installer_writes_enabled"] = self.guard.installer_writes_enabled
            self._save_persisted_switches()

        if "refresh" in payload:
            targets = payload["refresh"]
            if isinstance(targets, str):
                targets = [targets]
            if not isinstance(targets, list):
                return {**base, "status": "refused", "reason": "invalid_payload", "detail": "refresh must be string or list"}
            refreshed: list[str] = []
            for item in targets:
                if "/" not in str(item):
                    continue
                circuit, msg_name = str(item).split("/", 1)
                msg = self.model.messages.get(f"{circuit}/{msg_name}")
                if msg is None:
                    continue
                try:
                    raw = await self.client.read_from_bus(circuit, msg_name)
                except EbusdError:
                    continue
                valid, decoded = decode_message(msg, raw)
                if valid:
                    self.states.setdefault(circuit, {})[msg_name] = decoded
                    self.states[circuit]["_updated"] = _now_iso()
                    refreshed.append(f"{circuit}/{msg_name}")
            changed["refreshed"] = refreshed

        if payload.get("sync_clock") is True:
            try:
                self.guard.check_level(is_installer=False)
                last = await self.client.last_broadcast_of_clock()
                self.last_clock = last
                now = datetime.now().astimezone()
                tg = clock.telegram(now, last.outside_temp if last else None)
                await self.client.write_broadcast(tg, lambda: True)
                self.last_clock_sync = _now_iso()
                changed["sync_clock"] = self.last_clock_sync
            except WriteRefused as err:
                return {**base, "status": "refused", "reason": err.code, "detail": str(err)}
            except EbusdError as err:
                return {**base, "status": "error", "reason": "ebusd_error", "detail": str(err)}

        return {**base, "status": "ok", "changed": changed}
