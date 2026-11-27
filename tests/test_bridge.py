"""Unit tests for weishaupt2mqtt against the real 1.3.1 metadata (no bus, no broker)."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from weishaupt2mqtt.bridge import MqttBridge
from weishaupt2mqtt.config import Config
from weishaupt2mqtt import clock
from weishaupt2mqtt.ebusd import (
    BusAddress,
    Definition,
    EbusdConnectionError,
    EbusdInfo,
    EbusdLate,
    EbusdNoData,
    FieldDefinition,
)
from weishaupt2mqtt.engine import WeishauptEngine, decode_message
from weishaupt2mqtt.writes import crc


def _wcrc_def(circuit: str, name: str, zz: str, reg: str, ftype: str = "UIN", div: str = "10", unit: str = "°C") -> Definition:
    """Build a synthetic ebusd `find -f` write definition matching `writes.py`."""
    reg_bytes = bytes.fromhex(reg)
    placeholder = f"{crc(reg_bytes):02x}"
    fields = (FieldDefinition("value", "m", ftype, div, unit),)
    return Definition(
        direction="w",
        circuit=circuit,
        name=name,
        comment="[WCRC] test",
        fields=fields,
        zz=zz,
        pbsb="5023",
        id=f"{placeholder}{reg}",
    )


def _slot_def(circuit: str, name: str, zz: str, reg: str) -> Definition:
    reg_bytes = bytes.fromhex(reg)
    placeholder = f"{crc(reg_bytes):02x}"
    fields = (
        FieldDefinition("totime", "m", "TTQ", "", ""),
        FieldDefinition("toenabled", "m", "BI7", "0=Enabled;1=Disabled", ""),
        FieldDefinition("fromtime", "m", "TTQ", "", ""),
        FieldDefinition("fromenabled", "m", "BI7", "0=Enabled;1=Disabled", ""),
    )
    return Definition(
        direction="w",
        circuit=circuit,
        name=name,
        comment="[WCRC] slot",
        fields=fields,
        zz=zz,
        pbsb="5023",
        id=f"{placeholder}{reg}",
    )


_DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _hc1_slot_defs() -> list[Definition]:
    defs: list[Definition] = []
    for day_idx, day in enumerate(_DAYS, start=1):
        for slot in (1, 2, 3):
            reg = f"{(day_idx << 4) | (slot - 1):02x}14"
            defs.append(_slot_def("fs1", f"HCProgram1{day}{slot}", "35", reg))
    return defs


class FakeEbusdClient:
    """In-memory stand-in for EbusdClient."""

    def __init__(self, *, stock_ebusd: bool = False) -> None:
        self.stock_ebusd = stock_ebusd
        self.values: dict[tuple[str, str], tuple[str, ...]] = {
            ("ka", "OutsideTemp"): ("7.5",),
            ("ka", "DHWTemp"): ("54.2",),
            ("ka", "BlockingTime"): ("5",),
            ("fs1", "RoomNormalTemp"): ("20.0",),
            ("fs1", "RoomSetbackTemp"): ("16.0",),
            ("em2", "SupplyMaxTemp"): ("70.0",),
            ("em2", "SupplyMinTemp"): ("25.0",),
            ("fs1", "HCProgram1Mon1"): ("18:00", "Enabled", "06:30", "Enabled"),
            ("fs1", "HCProgram1Mon2"): ("00:00", "Disabled", "00:00", "Disabled"),
            ("fs1", "HCProgram1Mon3"): ("00:00", "Disabled", "00:00", "Disabled"),
        }
        self.writes: list[tuple[str, str, str | None]] = []
        self.reject_take: set[tuple[str, str]] = set()
        self.defs: dict[str, list[Definition]] = {
            "ka": [_wcrc_def("ka", "BlockingTime", "15", "2500", "UIN", "1", "min")],
            "fs1": [
                _wcrc_def("fs1", "RoomNormalTemp", "35", "0102", "UIN", "10", "°C"),
                _wcrc_def("fs1", "RoomSetbackTemp", "35", "0202", "UIN", "10", "°C"),
                *_hc1_slot_defs(),
            ],
            "em2": [
                _wcrc_def("em2", "SupplyMaxTemp", "51", "0502", "UIN", "10", "°C"),
                _wcrc_def("em2", "SupplyMinTemp", "51", "0602", "UIN", "10", "°C"),
            ],
            "bc1": [],
            "broadcast": [
                Definition("w", "Broadcast", "Datetime", "date and time", (), zz="fe", pbsb="0700", id=""),
            ],
        }
        self.signal = True
        self.down = False  # ebusd does not answer at all
        self.reads = 0
        # what `find -p -h -vvv -c broadcast Datetime` answers; None: ebusd holds no broadcast yet
        self.clock_find: str | None = _find_answer("10", datetime(2026, 11, 27, 10, 14, 0), 7.5,
                                                   datetime(2026, 11, 27, 10, 14, 3))
        self.broadcasts: list[tuple[str, datetime]] = []  # telegram, and the test's time when it went out
        self.now: Any = None  # the test's clock, set by the tests of the clock
        self.busy = 0.0  # seconds the connection to ebusd stays taken before a broadcast can go out

    async def info(self) -> EbusdInfo:
        if self.down:
            raise EbusdConnectionError("cannot connect to ebusd: refused")
        return EbusdInfo(
            version="ebusd 26.1.26.1" if self.stock_ebusd else "ebusd 26.1.p20261029",
            signal=self.signal,
            addresses={
                "31": BusAddress("31", "master", own=True),
                "08": BusAddress("08", "slave", manufacturer="Kromschroeder", ident="W", files=("kromschroeder/08.w.bc1.csv",)),
                "15": BusAddress("15", "slave", manufacturer="Kromschroeder", ident="W", files=("kromschroeder/15.w.ka.csv",)),
                "35": BusAddress("35", "slave", manufacturer="Kromschroeder", ident="W", files=("kromschroeder/35.w.fs1.csv",)),
                "51": BusAddress("51", "slave", manufacturer="Kromschroeder", ident="W", files=("kromschroeder/51.w.em2.csv",)),
            },
        )

    async def definitions(self, circuit: str, write: bool = False) -> list[Definition]:
        return list(self.defs.get(circuit, []))

    async def probe_write(self, telegram: str) -> str:
        # Checksum byte is telegram[8:10]; reg is telegram[10:14]; body is telegram[10:18]
        body = bytes.fromhex(telegram[10:18])
        reg = bytes.fromhex(telegram[10:14])
        got = int(telegram[8:10], 16)
        if self.stock_ebusd:
            return "matched" if got == crc(reg) else "not found"
        return "matched" if got == crc(body) else "not found"

    async def last_broadcast_of_clock(self) -> str:
        if self.clock_find is None:
            raise EbusdNoData("no data stored")
        return self.clock_find

    async def write_broadcast(self, telegram: str, in_time: Any) -> str:
        if self.busy:
            self.now.advance(self.busy)
        if not in_time():
            raise EbusdLate(f"not sent, too late: {telegram}")
        self.broadcasts.append((telegram, self.now()))
        return "done broadcast"

    async def read(self, circuit: str, name: str, max_age: int = 60) -> tuple[str, ...]:
        if self.down:
            raise EbusdConnectionError("cannot connect to ebusd: refused")
        self.reads += 1
        if (circuit, name) not in self.values:
            raise EbusdNoData(f"{circuit}/{name}")
        return self.values[(circuit, name)]

    async def read_from_bus(self, circuit: str, name: str) -> tuple[str, ...]:
        return await self.read(circuit, name, max_age=0)

    async def write_register(self, circuit: str, telegram: str, source: str | None) -> str:
        self.writes.append((circuit, telegram, source))
        reg = telegram[10:14]
        raw_val = int.from_bytes(bytes.fromhex(telegram[14:18]), "little")
        for d in self.defs.get(circuit, []):
            if d.id[2:6] == reg:
                if (circuit, d.name) in self.reject_take:
                    return "00"
                if d.name.startswith("HCProgram"):
                    end_b, start_b = raw_val & 0xFF, raw_val >> 8
                    to_en = "Disabled" if (end_b & 0x80) else "Enabled"
                    fr_en = "Disabled" if (start_b & 0x80) else "Enabled"
                    to_q, fr_q = end_b & 0x7F, start_b & 0x7F
                    self.values[(circuit, d.name)] = (
                        f"{to_q // 4:02d}:{to_q % 4 * 15:02d}",
                        to_en,
                        f"{fr_q // 4:02d}:{fr_q % 4 * 15:02d}",
                        fr_en,
                    )
                else:
                    div = int(d.fields[0].divisor_or_values or "1")
                    val = raw_val / div if div > 1 else raw_val
                    self.values[(circuit, d.name)] = (str(val),)
                break
        return "00"


def _find_answer(source: str, carried: datetime, outside: float, received: datetime) -> str:
    """The answer of ebusd to `find -p -h -vvv -c broadcast Datetime` for one broadcast."""
    frame = source + clock.telegram(carried, outside)
    return f"broadcast Datetime = {frame} / - [lastup={received:%Y-%m-%d %H:%M:%S}, active read]"


class FakeTime:
    """The wall clock and the sleep of the engine, in the hands of the test."""

    def __init__(self, start: datetime) -> None:
        self.value = start
        self.slept: list[float] = []

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.advance(seconds)


class FakePublisher:
    def __init__(self) -> None:
        self.messages: list[tuple[str, Any, bool]] = []

    async def publish(self, topic: str, payload: str, *, retain: bool = False, qos: int = 1) -> None:
        try:
            parsed: Any = json.loads(payload)
        except ValueError:
            parsed = payload
        self.messages.append((topic, parsed, retain))

    def last(self, topic: str) -> Any:
        for t, p, _ in reversed(self.messages):
            if t == topic:
                return p
        raise AssertionError(f"No message published on {topic}")

    def all(self, topic: str) -> list[Any]:
        return [p for t, p, _ in self.messages if t == topic]


class TestWeishaupt2Mqtt(unittest.IsolatedAsyncioTestCase):
    async def _make_bridge(
        self,
        *,
        writes_enabled: bool = True,
        installer_writes_enabled: bool = False,
        stock_ebusd: bool = False,
        allow_stock_ebusd: bool = False,
        max_writes_per_hour: int = 30,
        state_file: Path | None = None,
        **more: Any,
    ) -> tuple[FakeEbusdClient, FakePublisher, WeishauptEngine, MqttBridge]:
        cfg = Config(
            **more,
            writes_enabled=writes_enabled,
            installer_writes_enabled=installer_writes_enabled,
            allow_stock_ebusd=allow_stock_ebusd,
            max_writes_per_hour=max_writes_per_hour,
            state_file=state_file,
        )
        fake_ebusd = FakeEbusdClient(stock_ebusd=stock_ebusd)
        engine = WeishauptEngine(cfg, client=fake_ebusd)
        await engine.start()
        pub = FakePublisher()
        bridge = MqttBridge(cfg, engine, pub)
        return fake_ebusd, pub, engine, bridge

    async def test_startup_and_state_decoding(self) -> None:
        _, pub, engine, bridge = await self._make_bridge()
        await bridge.publish_startup()

        self.assertEqual(pub.last("weishaupt/bridge/state"), "online")
        info = pub.last("weishaupt/bridge/info")
        self.assertEqual(info["run_time_checksum"], "present")
        self.assertTrue(info["bus_signal"])
        self.assertIn("fs1", info["circuits"])
        self.assertFalse(info["writes"]["installer_enabled"])

        # Absent sensor (-100.0 on ka) decodes to None
        outside_msg = engine.model.messages["ka/OutsideTemp"]
        valid, val = decode_message(outside_msg, ("-100.0",))
        self.assertTrue(valid)
        self.assertIsNone(val)

        # Memory read guard field (firstbyte != 0) is discarded
        bc_temps = engine.model.messages["bc1/Temperatures"]
        valid, _ = decode_message(bc_temps, ("12", "0", "0", "0", "0", "0"))
        self.assertFalse(valid)

    async def test_level1_user_write_succeeds_and_reads_back(self) -> None:
        fake, pub, _, bridge = await self._make_bridge(writes_enabled=True)
        await bridge.handle_message(
            "weishaupt/fs1/set",
            json.dumps({"RoomNormalTemp": 20.5, "correlation_id": "c-1"}),
        )
        res = pub.last("weishaupt/fs1/result")
        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["correlation_id"], "c-1")
        self.assertEqual(res["before"], 20.0)
        self.assertEqual(res["after"], 20.5)
        self.assertEqual(res["source"], "ff")
        self.assertEqual(len(fake.writes), 1)
        self.assertEqual(fake.writes[0][2], "ff")

    async def test_level_separation_option_b(self) -> None:
        fake, pub, _, bridge = await self._make_bridge(writes_enabled=True, installer_writes_enabled=True)

        # Level 2 setting on /set -> refused with installer_topic_required
        await bridge.handle_message("weishaupt/em2/set", json.dumps({"SupplyMaxTemp": 75.0}))
        self.assertEqual(pub.last("weishaupt/em2/result")["status"], "refused")
        self.assertEqual(pub.last("weishaupt/em2/result")["reason"], "installer_topic_required")

        # Level 1 setting on /installer/set -> refused with wrong_level_topic
        await bridge.handle_message("weishaupt/fs1/installer/set", json.dumps({"RoomNormalTemp": 20.5, "confirm": True}))
        self.assertEqual(pub.last("weishaupt/fs1/result")["status"], "refused")
        self.assertEqual(pub.last("weishaupt/fs1/result")["reason"], "wrong_level_topic")

        # Level 2 on /installer/set without confirm -> confirmation_required
        await bridge.handle_message("weishaupt/em2/installer/set", json.dumps({"SupplyMaxTemp": 75.0}))
        res = pub.last("weishaupt/em2/result")
        self.assertEqual(res["status"], "confirmation_required")
        self.assertEqual(res["min"], 8.0)
        self.assertEqual(res["max"], 82.0)
        self.assertEqual(len(fake.writes), 0)

        # Level 2 on /installer/set WITH confirm: true -> ok
        await bridge.handle_message("weishaupt/em2/installer/set", json.dumps({"SupplyMaxTemp": 75.0, "confirm": True}))
        res = pub.last("weishaupt/em2/result")
        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["after"], 75.0)
        self.assertEqual(len(fake.writes), 1)

    async def test_installer_switch_disabled_blocks_level2(self) -> None:
        fake, pub, _, bridge = await self._make_bridge(writes_enabled=True, installer_writes_enabled=False)
        await bridge.handle_message("weishaupt/em2/installer/set", json.dumps({"SupplyMaxTemp": 75.0, "confirm": True}))
        res = pub.last("weishaupt/em2/result")
        self.assertEqual(res["status"], "refused")
        self.assertEqual(res["reason"], "installer_writes_disabled")
        self.assertEqual(len(fake.writes), 0)

    async def test_linked_bounds_enforced(self) -> None:
        fake, pub, _, bridge = await self._make_bridge(writes_enabled=True)
        # RoomNormalTemp is 20.0 -> setting RoomSetbackTemp to 21.0 violates linked bound
        await bridge.handle_message("weishaupt/fs1/set", json.dumps({"RoomSetbackTemp": 21.0}))
        res = pub.last("weishaupt/fs1/result")
        self.assertEqual(res["status"], "refused")
        self.assertEqual(res["reason"], "linked_bound")
        self.assertEqual(len(fake.writes), 0)

    async def test_not_taken_detected_on_readback(self) -> None:
        fake, pub, _, bridge = await self._make_bridge(writes_enabled=True)
        fake.reject_take.add(("fs1", "RoomNormalTemp"))
        await bridge.handle_message("weishaupt/fs1/set", json.dumps({"RoomNormalTemp": 21.0}))
        res = pub.last("weishaupt/fs1/result")
        self.assertEqual(res["status"], "not_taken")
        self.assertEqual(res["after"], 20.0)

    async def test_stock_ebusd_write_opt_in(self) -> None:
        # Stock ebusd refused by default
        fake1, pub1, _, b1 = await self._make_bridge(writes_enabled=True, stock_ebusd=True, allow_stock_ebusd=False)
        await b1.handle_message("weishaupt/fs1/set", json.dumps({"RoomNormalTemp": 20.5}))
        self.assertEqual(pub1.last("weishaupt/fs1/result")["reason"], "stock_ebusd_not_allowed")
        self.assertEqual(len(fake1.writes), 0)

        # Stock ebusd allowed with allow_stock_ebusd=True
        fake2, pub2, _, b2 = await self._make_bridge(writes_enabled=True, stock_ebusd=True, allow_stock_ebusd=True)
        await b2.handle_message("weishaupt/fs1/set", json.dumps({"RoomNormalTemp": 20.5}))
        self.assertEqual(pub2.last("weishaupt/fs1/result")["status"], "ok")
        self.assertEqual(len(fake2.writes), 1)

    async def test_program_minimal_diff_and_budget(self) -> None:
        # Monday currently has ["06:30-18:00"] (slot 1), slots 2 & 3 disabled.
        # Adding a second slot ["06:30-18:00", "19:00-21:00"] should write ONLY 1 slot!
        fake, pub, _, bridge = await self._make_bridge(writes_enabled=True, max_writes_per_hour=5)
        await bridge.handle_message(
            "weishaupt/fs1/program/heating1/set",
            json.dumps({"days": {"1": ["06:30-18:00", "19:00-21:00"]}, "correlation_id": "p-1"}),
        )
        res = pub.last("weishaupt/fs1/result")
        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["slots_written"], 1)
        self.assertEqual(res["slots_unchanged"], 2)
        self.assertEqual(len(fake.writes), 1)

    async def test_bridge_config_switches_persist(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            sf = Path(td) / "state.json"
            _, pub, engine, bridge = await self._make_bridge(
                writes_enabled=False, installer_writes_enabled=False, state_file=sf
            )
            await bridge.handle_message(
                "weishaupt/bridge/config/set",
                json.dumps({"writes_enabled": True, "installer_writes_enabled": True}),
            )
            self.assertEqual(pub.last("weishaupt/bridge/result")["status"], "ok")
            self.assertTrue(engine.guard.writes_enabled)
            self.assertTrue(engine.guard.installer_writes_enabled)
            persisted = json.loads(sf.read_text(encoding="utf-8"))
            self.assertEqual(persisted, {"writes_enabled": True, "installer_writes_enabled": True})

    # ---- reading loop ---------------------------------------------------------------------------------------

    async def test_reading_pass_with_clock_sync_enabled_goes_on(self) -> None:
        """0.1.3 called a method that does not exist at the end of the first pass, which ended the loop."""
        fake, pub, engine, bridge = await self._make_bridge(clock_sync_enabled=True, clock_sync_interval=1800.0)
        await bridge.publish_startup()
        engine._clock_attempt -= 1800.0  # the periodic broadcast is due
        engine._now, engine._sleep = (now := FakeTime(datetime(2026, 11, 27, 10, 14, 20))), now.sleep
        fake.now = now
        await bridge.poll_pass()
        self.assertIsNone(engine.read_error)
        self.assertEqual(len(fake.broadcasts), 1)
        self.assertEqual(pub.last("weishaupt/bridge/journal")["kind"], "clock")
        self.assertTrue(pub.last("weishaupt/bridge/journal")["periodic"])
        self.assertEqual(pub.last("weishaupt/ka/state")["OutsideTemp"], 7.5)
        # the next pass reads again, and the clock is not due again
        fake.values[("ka", "OutsideTemp")] = ("8.0",)
        bridge.tick = 0  # a pass with every group, as the first one
        await bridge.poll_pass()
        self.assertEqual(pub.last("weishaupt/ka/state")["OutsideTemp"], 8.0)
        self.assertEqual(len(fake.broadcasts), 1)
        self.assertEqual(pub.last("weishaupt/bridge/state"), "online")

    async def test_periodic_clock_waits_one_interval(self) -> None:
        fake, _, engine, bridge = await self._make_bridge(clock_sync_enabled=True)
        await bridge.poll_pass()
        self.assertEqual(fake.broadcasts, [])
        self.assertFalse(engine.clock_due())

    async def test_failing_step_is_logged_and_the_loop_survives(self) -> None:
        fake, pub, engine, bridge = await self._make_bridge()
        await bridge.publish_startup()
        good = engine.poll_programs

        async def broken() -> list[tuple[str, str]]:
            raise AttributeError("no such thing")

        engine.poll_programs = broken  # type: ignore[method-assign]
        with self.assertLogs("weishaupt2mqtt.bridge", level="ERROR") as logs:
            await bridge.poll_pass()
        self.assertIn("step programs failed", logs.output[0])
        # the other steps of the same pass were done
        self.assertEqual(pub.last("weishaupt/ka/state")["OutsideTemp"], 7.5)
        self.assertEqual(pub.last("weishaupt/bridge/state"), "offline")
        health = pub.last("weishaupt/bridge/info")["health"]
        self.assertEqual((health["ok"], health["reason"]), (False, "reading_failed"))
        self.assertIn("programs: AttributeError: no such thing", health["detail"])
        # and a later pass that works brings the bridge back
        engine.poll_programs = good  # type: ignore[method-assign]
        bridge.tick = 0
        await bridge.poll_pass()
        self.assertEqual(pub.last("weishaupt/bridge/state"), "online")
        self.assertTrue(pub.last("weishaupt/bridge/info")["health"]["ok"])

    async def test_poll_loop_outlives_a_failing_pass(self) -> None:
        import asyncio

        _, _, _, bridge = await self._make_bridge(poll_passive_interval=0.0)
        calls = 0

        async def failing_pass() -> None:
            nonlocal calls
            calls += 1
            if calls >= 3:
                raise asyncio.CancelledError
            raise RuntimeError("pass broke")

        bridge.poll_pass = failing_pass  # type: ignore[method-assign]
        with self.assertLogs("weishaupt2mqtt.bridge", level="ERROR"):
            with self.assertRaises(asyncio.CancelledError):
                await bridge.poll_loop()
        self.assertEqual(calls, 3)

    # ---- health ---------------------------------------------------------------------------------------------

    async def test_bus_signal_lost_and_back(self) -> None:
        fake, pub, _, bridge = await self._make_bridge()
        await bridge.publish_startup()
        await bridge.poll_pass()
        infos = len(pub.all("weishaupt/bridge/info"))
        await bridge.poll_pass()  # nothing changed: nothing republished
        self.assertEqual(len(pub.all("weishaupt/bridge/info")), infos)
        self.assertEqual(pub.all("weishaupt/bridge/state"), ["online"])

        fake.signal = False
        bridge.tick = 0  # a pass that asks ebusd for its state
        await bridge.poll_pass()
        info = pub.last("weishaupt/bridge/info")
        self.assertFalse(info["bus_signal"])
        self.assertEqual(info["health"]["reason"], "no_bus_signal")
        self.assertEqual(pub.last("weishaupt/bridge/state"), "offline")

        fake.signal = True
        bridge.tick = 0
        await bridge.poll_pass()
        self.assertTrue(pub.last("weishaupt/bridge/info")["bus_signal"])
        self.assertEqual(pub.all("weishaupt/bridge/state"), ["online", "offline", "online"])

    async def test_ebusd_unreachable_ends_the_pass_and_is_retried(self) -> None:
        fake, pub, engine, bridge = await self._make_bridge()
        await bridge.publish_startup()
        bridge.tick = 1  # a pass that does not ask ebusd for its state first
        fake.down = True
        await bridge.poll_pass()
        self.assertFalse(engine.ebusd_reachable)
        info = pub.last("weishaupt/bridge/info")
        self.assertEqual(info["health"]["reason"], "ebusd_unreachable")
        self.assertFalse(info["bus_signal"])
        self.assertEqual(pub.last("weishaupt/bridge/state"), "offline")
        # while it is down, a pass only asks for its state
        reads = fake.reads
        await bridge.poll_pass()
        self.assertEqual(fake.reads, reads)
        fake.down = False
        await bridge.poll_pass()
        self.assertGreater(fake.reads, reads)
        self.assertEqual(pub.last("weishaupt/bridge/state"), "online")

    async def test_stalled_reading_loop_is_reported(self) -> None:
        _, pub, engine, bridge = await self._make_bridge()
        await bridge.publish_startup()
        engine.last_activity -= 301.0
        await bridge.publish_health()  # what `watch_health` does beside the reading loop
        self.assertEqual(pub.last("weishaupt/bridge/state"), "offline")
        self.assertEqual(pub.last("weishaupt/bridge/info")["health"]["reason"], "reading_stalled")

    # ---- commands -------------------------------------------------------------------------------------------

    async def test_handler_exception_stays_inside(self) -> None:
        _, pub, engine, bridge = await self._make_bridge()

        async def broken(payload: dict[str, Any]) -> dict[str, Any]:
            raise AttributeError("'str' object has no attribute 'outside_temp'")

        engine.handle_bridge_config = broken  # type: ignore[method-assign]
        with self.assertLogs("weishaupt2mqtt.bridge", level="ERROR"):
            await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": True}))
        res = pub.last("weishaupt/bridge/result")
        self.assertEqual((res["status"], res["reason"]), ("error", "internal_error"))

        class Broken(Exception):
            pass

        bridge.transport_errors = (Broken,)

        async def lost(payload: dict[str, Any]) -> dict[str, Any]:
            raise Broken

        engine.handle_bridge_config = lost  # type: ignore[method-assign]
        with self.assertRaises(Broken):  # the MQTT connection's own errors still end the loop
            await bridge.handle_message("weishaupt/bridge/config/set", "{}")

    # ---- clock ----------------------------------------------------------------------------------------------

    async def _clock_bridge(self, at: datetime, **more: Any) -> tuple[FakeEbusdClient, FakePublisher, WeishauptEngine, MqttBridge, FakeTime]:
        fake, pub, engine, bridge = await self._make_bridge(**more)
        now = FakeTime(at)
        engine._now, engine._sleep = now, now.sleep
        fake.now = now
        return fake, pub, engine, bridge, now

    async def test_sync_clock_goes_out_at_second_00_of_the_minute_it_carries(self) -> None:
        fake, pub, engine, bridge, now = await self._clock_bridge(datetime(2026, 11, 27, 10, 14, 20, 250000))
        await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": True, "correlation_id": "k1"}))
        res = pub.last("weishaupt/bridge/result")
        self.assertEqual(res["status"], "ok", res)
        self.assertEqual(res["correlation_id"], "k1")
        target = datetime(2026, 11, 27, 10, 15, 0)
        # one broadcast, sent at second 00 exactly, carrying that minute and the master's outside temperature
        self.assertEqual(fake.broadcasts, [(clock.telegram(target, 7.5), target)])
        self.assertEqual(fake.broadcasts[0][0], "fe0700098007001510271105" + "26")
        self.assertAlmostEqual(sum(now.slept), 39.75)
        self.assertTrue(engine._bus_free.is_set())
        entry = pub.last("weishaupt/bridge/journal")
        self.assertEqual((entry["kind"], entry["status"], entry["source"]), ("clock", "ok", "31"))
        self.assertEqual(entry["requested"], "2026-11-27 10:15:00")
        self.assertEqual(entry["late"], 0.0)
        info = pub.last("weishaupt/bridge/info")
        self.assertEqual(info["clock"]["outside_temp"], 7.5)
        self.assertTrue(info["clock"]["writable"])
        self.assertIsNotNone(info["clock"]["last_sync"])
        self.assertEqual(info["writes"]["used_last_hour"], 1)

    async def test_sync_clock_given_up_when_late(self) -> None:
        fake, pub, engine, bridge, _ = await self._clock_bridge(datetime(2026, 11, 27, 10, 14, 50))
        fake.busy = clock.LATE + 0.5  # the connection to ebusd is taken past the deadline
        await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": True}))
        res = pub.last("weishaupt/bridge/result")
        self.assertEqual((res["status"], res["reason"]), ("refused", "clock_late"))
        self.assertEqual(fake.broadcasts, [])
        self.assertIsNone(engine.last_clock_sync)
        self.assertTrue(engine._bus_free.is_set())

    async def test_sync_clock_slightly_late_is_sent(self) -> None:
        fake, pub, _, bridge, _ = await self._clock_bridge(datetime(2026, 11, 27, 10, 14, 50))
        fake.busy = clock.LATE - 0.5
        await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": True}))
        self.assertEqual(pub.last("weishaupt/bridge/result")["status"], "ok")
        self.assertEqual(pub.last("weishaupt/bridge/journal")["late"], clock.LATE - 0.5)
        self.assertEqual(len(fake.broadcasts), 1)

    async def test_sync_clock_without_a_broadcast_held_is_refused(self) -> None:
        for answer in (None, "broadcast Datetime = no data stored", ""):
            fake, pub, _, bridge, now = await self._clock_bridge(datetime(2026, 11, 27, 10, 14, 50))
            fake.clock_find = answer
            await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": True}))
            res = pub.last("weishaupt/bridge/result")
            self.assertEqual((res["status"], res["reason"]), ("refused", "clock_unknown"), answer)
            self.assertEqual(fake.broadcasts, [])
            self.assertEqual(now.slept, [])
            self.assertEqual(pub.last("weishaupt/bridge/journal")["reason"], "clock_unknown")

    async def test_sync_clock_refusals_before_anything_is_sent(self) -> None:
        when = datetime(2026, 11, 27, 10, 14, 50)
        fake, pub, _, bridge, _ = await self._clock_bridge(when, writes_enabled=False)
        await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": True}))
        self.assertEqual(pub.last("weishaupt/bridge/result")["reason"], "writes_disabled")

        fake, pub, engine, bridge, _ = await self._clock_bridge(when)
        engine.clock_writable = False  # ebusd without the write definition of the broadcast
        await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": True}))
        self.assertEqual(pub.last("weishaupt/bridge/result")["reason"], "not_writable")

        fake, pub, engine, bridge, _ = await self._clock_bridge(when)
        engine.own_address = "01"
        await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": True}))
        self.assertEqual(pub.last("weishaupt/bridge/result")["reason"], "source_not_own")

        fake, pub, _, bridge, _ = await self._clock_bridge(when)
        await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": "now"}))
        self.assertEqual(pub.last("weishaupt/bridge/result")["reason"], "invalid_payload")
        # twice within ten seconds: the second one is refused by the write guard
        await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": True}))
        await bridge.handle_message("weishaupt/bridge/config/set", json.dumps({"sync_clock": True}))
        self.assertEqual(pub.last("weishaupt/bridge/result")["reason"], "register_interval")
        self.assertEqual(len(fake.broadcasts), 1)

    async def test_reading_waits_while_the_broadcast_goes_out(self) -> None:
        import asyncio

        fake, _, engine, _ = await self._make_bridge()
        engine._bus_free.clear()
        read = asyncio.create_task(engine.poll_messages(("passive",)))
        await asyncio.sleep(0.01)
        self.assertEqual(fake.reads, 0)
        engine._bus_free.set()
        await read
        self.assertGreater(fake.reads, 0)

    # ---- configuration --------------------------------------------------------------------------------------

    def test_polling_defaults_are_the_same_everywhere(self) -> None:
        from unittest.mock import patch

        with patch.dict(os.environ, {}, clear=True):
            loaded = Config.from_sources()
        for name in ("poll_passive_interval", "poll_cyclic_interval", "poll_program_interval",
                     "poll_faults_interval", "clock_sync_interval", "clock_sync_enabled"):
            self.assertEqual(getattr(loaded, name), getattr(Config(), name), name)
        self.assertEqual(loaded.poll_program_interval, 3600.0)

    def test_supervisor_mqtt_auto_discovery(self) -> None:
        from unittest.mock import MagicMock, patch

        with patch.dict(os.environ, {"SUPERVISOR_TOKEN": "mock-token"}):
            with patch("urllib.request.urlopen") as mock_urlopen:
                mock_resp = MagicMock()
                mock_resp.read.return_value = (
                    b'{"result": "ok", "data": {"host": "core-mosquitto", "port": 1883, '
                    b'"username": "auto-user", "password": "auto-password"}}'
                )
                mock_resp.__enter__.return_value = mock_resp
                mock_urlopen.return_value = mock_resp

                cfg = Config.from_sources()
                self.assertEqual(cfg.mqtt_username, "auto-user")
                self.assertEqual(cfg.mqtt_password, "auto-password")
                self.assertEqual(cfg.mqtt_host, "core-mosquitto")


if __name__ == "__main__":
    unittest.main()
