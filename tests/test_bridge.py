"""Unit tests for weishaupt2mqtt against the real 1.3.1 metadata (no bus, no broker)."""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from weishaupt2mqtt.bridge import MqttBridge
from weishaupt2mqtt.config import Config
from weishaupt2mqtt.ebusd import BusAddress, Definition, EbusdInfo, EbusdNoData, FieldDefinition
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
        }

    async def info(self) -> EbusdInfo:
        return EbusdInfo(
            version="ebusd 26.1.26.1" if self.stock_ebusd else "ebusd 26.1.p20261029",
            signal=True,
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

    async def read(self, circuit: str, name: str, max_age: int = 60) -> tuple[str, ...]:
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
    ) -> tuple[FakeEbusdClient, FakePublisher, WeishauptEngine, MqttBridge]:
        cfg = Config(
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

    def test_supervisor_mqtt_auto_discovery(self) -> None:
        import os
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
