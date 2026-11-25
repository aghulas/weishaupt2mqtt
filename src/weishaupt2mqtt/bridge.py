"""MQTT bridge exposing `WeishauptEngine` per docs/mqtt-contract.md."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Protocol

from .config import Config
from .engine import WeishauptEngine

_LOGGER = logging.getLogger(__name__)


class MqttPublisher(Protocol):
    async def publish(self, topic: str, payload: str, *, retain: bool = False, qos: int = 1) -> None: ...


class MqttBridge:
    """Routes MQTT topics to/from `WeishauptEngine`."""

    def __init__(self, config: Config, engine: WeishauptEngine, publisher: MqttPublisher) -> None:
        self.config = config
        self.engine = engine
        self.pub = publisher
        self.base = config.base_topic.strip("/")

    def topic(self, suffix: str) -> str:
        return f"{self.base}/{suffix.lstrip('/')}"

    def subscriptions(self) -> list[str]:
        return [
            self.topic("bridge/config/set"),
            self.topic("+/set"),
            self.topic("+/installer/set"),
            self.topic("+/program/+/set"),
        ]

    async def publish_startup(self) -> None:
        await self.pub.publish(self.topic("bridge/state"), "online", retain=True, qos=1)
        await self.pub.publish(
            self.topic("bridge/info"),
            json.dumps(self.engine.bridge_info(), ensure_ascii=False),
            retain=True,
            qos=1,
        )
        await self.pub.publish(
            self.topic("bridge/metadata"),
            json.dumps(self.engine.filtered_metadata(), ensure_ascii=False),
            retain=True,
            qos=1,
        )

    async def publish_bridge_info(self) -> None:
        await self.pub.publish(
            self.topic("bridge/info"),
            json.dumps(self.engine.bridge_info(), ensure_ascii=False),
            retain=True,
            qos=1,
        )

    async def publish_circuit_state(self, circuit: str) -> None:
        state = self.engine.states.get(circuit)
        if state is not None:
            await self.pub.publish(
                self.topic(f"{circuit}/state"),
                json.dumps(state, ensure_ascii=False),
                retain=True,
                qos=0,
            )

    async def publish_program(self, circuit: str, program_id: str) -> None:
        prog = self.engine.programs_state.get(circuit, {}).get(program_id)
        if prog is not None:
            await self.pub.publish(
                self.topic(f"{circuit}/program/{program_id}"),
                json.dumps(prog, ensure_ascii=False),
                retain=True,
                qos=0,
            )

    async def publish_faults(self, circuit: str) -> None:
        data = self.engine.faults_state.get(circuit)
        if data is not None:
            await self.pub.publish(
                self.topic(f"{circuit}/faults"),
                json.dumps(data, ensure_ascii=False),
                retain=True,
                qos=0,
            )

    async def handle_message(self, topic: str, raw_payload: str | bytes) -> None:
        """Dispatch an incoming MQTT command message."""
        prefix = self.base + "/"
        if not topic.startswith(prefix):
            return
        rel = topic[len(prefix):]
        parts = rel.split("/")

        text = raw_payload.decode("utf-8", errors="replace") if isinstance(raw_payload, bytes) else raw_payload
        try:
            payload = json.loads(text)
        except ValueError as err:
            result_topic = self.topic("bridge/result") if parts[0] == "bridge" else self.topic(f"{parts[0]}/result")
            await self.pub.publish(
                result_topic,
                json.dumps({"status": "refused", "reason": "invalid_payload", "detail": str(err)}),
                retain=False,
                qos=1,
            )
            return

        if not isinstance(payload, dict):
            result_topic = self.topic("bridge/result") if parts[0] == "bridge" else self.topic(f"{parts[0]}/result")
            await self.pub.publish(
                result_topic,
                json.dumps({"status": "refused", "reason": "invalid_payload", "detail": "JSON object required"}),
                retain=False,
                qos=1,
            )
            return

        # 1. bridge/config/set
        if parts == ["bridge", "config", "set"]:
            res = await self.engine.handle_bridge_config(payload)
            await self.pub.publish(self.topic("bridge/result"), json.dumps(res, ensure_ascii=False), retain=False, qos=1)
            await self.publish_bridge_info()
            for item in res.get("changed", {}).get("refreshed", []):
                await self.publish_circuit_state(item.split("/", 1)[0])
            return

        # 2. <circuit>/set  (End user level)  or  <circuit>/installer/set  (Heating engineer level)
        if (len(parts) == 2 and parts[1] == "set") or (len(parts) == 3 and parts[1] == "installer" and parts[2] == "set"):
            circuit = parts[0]
            topic_level = "installer" if len(parts) == 3 else "user"
            confirm = bool(payload.get("confirm", False))
            corr = payload.get("correlation_id")
            control_keys = {"confirm", "correlation_id"}
            settings = {k: v for k, v in payload.items() if k not in control_keys}
            if not settings:
                await self.pub.publish(
                    self.topic(f"{circuit}/result"),
                    json.dumps(
                        {
                            "correlation_id": corr,
                            "circuit": circuit,
                            "status": "refused",
                            "reason": "invalid_payload",
                            "detail": "no settings in payload",
                        }
                    ),
                    retain=False,
                    qos=1,
                )
                return

            any_written = False
            for name, value in settings.items():
                res = await self.engine.write_setting(
                    circuit,
                    name,
                    value,
                    topic_level=topic_level,
                    confirm=confirm,
                    correlation_id=corr,
                )
                await self.pub.publish(
                    self.topic(f"{circuit}/result"),
                    json.dumps(res, ensure_ascii=False),
                    retain=False,
                    qos=1,
                )
                await self.pub.publish(
                    self.topic("bridge/journal"),
                    json.dumps(res, ensure_ascii=False),
                    retain=False,
                    qos=1,
                )
                if res.get("status") == "ok":
                    any_written = True
            if any_written:
                await self.publish_circuit_state(circuit)
                await self.publish_bridge_info()
            return

        # 3. <circuit>/program/<program_id>/set
        if len(parts) == 4 and parts[1] == "program" and parts[3] == "set":
            circuit, program_id = parts[0], parts[2]
            corr = payload.get("correlation_id")
            days = payload.get("days")
            res = await self.engine.write_program(circuit, program_id, days, correlation_id=corr)
            await self.pub.publish(
                self.topic(f"{circuit}/result"),
                json.dumps(res, ensure_ascii=False),
                retain=False,
                qos=1,
            )
            await self.pub.publish(
                self.topic("bridge/journal"),
                json.dumps(res, ensure_ascii=False),
                retain=False,
                qos=1,
            )
            if res.get("status") == "ok":
                changed = await self.engine.poll_programs()
                for c, pid in changed:
                    await self.publish_program(c, pid)
                await self.publish_bridge_info()
            return


class _AiomqttAdapter:
    def __init__(self, client: Any) -> None:
        self._client = client

    async def publish(self, topic: str, payload: str, *, retain: bool = False, qos: int = 1) -> None:
        await self._client.publish(topic, payload=payload, retain=retain, qos=qos)


async def run_forever(config: Config) -> None:
    """Connect to ebusd and MQTT broker and run polling + command loops."""
    import aiomqtt

    engine = WeishauptEngine(config)
    await engine.start(wait_for_slaves=True)
    _LOGGER.info(
        "Connected to ebusd (%s), definitions %s, circuits=%s, checksum=%s",
        engine.info.version if engine.info else "?",
        engine.model.version,
        engine.present_circuits,
        engine.checksum_capability,
    )

    will = aiomqtt.Will(topic=f"{config.base_topic.strip('/')}/bridge/state", payload="offline", qos=1, retain=True)
    while True:
        try:
            async with aiomqtt.Client(
                hostname=config.mqtt_host,
                port=config.mqtt_port,
                username=config.mqtt_username,
                password=config.mqtt_password,
                will=will,
            ) as client:
                bridge = MqttBridge(config, engine, _AiomqttAdapter(client))
                for sub in bridge.subscriptions():
                    await client.subscribe(sub, qos=1)
                await bridge.publish_startup()

                async def poll_loop() -> None:
                    tick = 0
                    while True:
                        for c in await engine.poll_messages(("passive",)):
                            await bridge.publish_circuit_state(c)
                        if tick % max(1, int(config.poll_cyclic_interval / max(1.0, config.poll_passive_interval))) == 0:
                            new_c = await engine.refresh_devices()
                            if new_c:
                                await bridge.publish_bridge_info()
                            for c in await engine.poll_messages(("cyclic",)):
                                await bridge.publish_circuit_state(c)
                        if tick % max(1, int(config.poll_program_interval / max(1.0, config.poll_passive_interval))) == 0:
                            for c in await engine.poll_messages(("on_demand",)):
                                await bridge.publish_circuit_state(c)
                            for c, pid in await engine.poll_programs():
                                await bridge.publish_program(c, pid)
                            for c in await engine.poll_faults():
                                await bridge.publish_faults(c)
                        if config.clock_sync_enabled and tick % max(1, int(config.clock_sync_interval / max(1.0, config.poll_passive_interval))) == 0:
                            await engine.update_bridge_config({"sync_clock": True})
                            await bridge.publish_bridge_info()
                        tick += 1
                        await asyncio.sleep(config.poll_passive_interval)

                poller = asyncio.create_task(poll_loop())
                try:
                    async for message in client.messages:
                        await bridge.handle_message(str(message.topic), message.payload)
                finally:
                    poller.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as err:
            _LOGGER.warning("MQTT connection error (%s); reconnecting in 5 s", err)
            await asyncio.sleep(5.0)
