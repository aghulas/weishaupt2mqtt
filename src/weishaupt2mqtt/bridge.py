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
        self.tick = 0  # reading passes done on this connection
        # errors of the MQTT connection itself: they leave `handle_message` and the reading loop (reconnection)
        self.transport_errors: tuple[type[BaseException], ...] = ()
        self._state: str | None = None  # last `bridge/state` published
        self._info: str | None = None  # last `bridge/info` published

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
        self._state = self._info = None
        await self.publish_health()
        await self.pub.publish(
            self.topic("bridge/metadata"),
            json.dumps(self.engine.filtered_metadata(), ensure_ascii=False),
            retain=True,
            qos=1,
        )

    async def publish_bridge_info(self) -> None:
        self._info = json.dumps(self.engine.bridge_info(), ensure_ascii=False)
        await self.pub.publish(self.topic("bridge/info"), self._info, retain=True, qos=1)

    async def publish_health(self) -> None:
        """Publish `bridge/state` and `bridge/info` if they changed. `online` means values are being read:
        ebusd answers, it has a signal on the bus and the reading loop works (`health` in `bridge/info`)."""
        info = self.engine.bridge_info()
        state = "online" if info["health"]["ok"] else "offline"
        if state != self._state:
            if self._state is not None:
                _LOGGER.warning("Bridge state %s: %s", state, info["health"]["reason"] or "reading again")
            self._state = state
            await self.pub.publish(self.topic("bridge/state"), state, retain=True, qos=1)
        if json.dumps(info, ensure_ascii=False) != self._info:
            await self.publish_bridge_info()

    async def publish_offline(self) -> None:
        """Before a deliberate disconnection: the broker only sends the last will on an unexpected one."""
        self._state = "offline"
        await self.pub.publish(self.topic("bridge/state"), "offline", retain=True, qos=1)

    def _due(self, interval: float) -> bool:
        return self.tick % max(1, int(interval / max(1.0, self.config.poll_passive_interval))) == 0

    async def poll_pass(self) -> None:
        """One reading pass. Each step is tried on its own: the exception of one is logged, kept for `bridge/info`
        (`health.reason` = `reading_failed`) and does not stop the others, nor the next pass. An ebusd that does
        not answer ends the pass at once."""
        engine, config = self.engine, self.config
        errors: list[str] = []

        async def step(name: str, action: Any, always: bool = False) -> None:
            if not (always or engine.ebusd_reachable):
                return
            engine.touch()
            try:
                await action()
            except self.transport_errors:
                raise
            except Exception as err:  # noqa: BLE001 - the reading loop must outlive any failure of a pass
                _LOGGER.exception("Reading pass %d, step %s failed", self.tick, name)
                errors.append(f"{name}: {type(err).__name__}: {err}")

        async def states(*classes: str) -> None:
            for circuit in await engine.poll_messages(classes):
                await self.publish_circuit_state(circuit)

        async def devices() -> None:
            if await engine.refresh_devices():
                await self.publish_bridge_info()

        async def programs() -> None:
            for circuit, program_id in await engine.poll_programs():
                await self.publish_program(circuit, program_id)

        async def fault_memory() -> None:
            for circuit in await engine.poll_faults():
                await self.publish_faults(circuit)

        async def clock_broadcast() -> None:
            entry = await engine.sync_clock(periodic=True)
            await self.pub.publish(self.topic("bridge/journal"), json.dumps(entry, ensure_ascii=False), retain=False, qos=1)

        try:
            if self._due(config.poll_cyclic_interval) or not engine.ebusd_reachable:
                await step("devices", devices, always=True)
            await step("passive", lambda: states("passive"))
            if self._due(config.poll_cyclic_interval):
                await step("cyclic", lambda: states("cyclic"))
            if self._due(config.poll_program_interval):
                await step("settings", lambda: states("on_demand"))
                await step("programs", programs)
            if self._due(config.poll_faults_interval):
                await step("faults", fault_memory)
            if engine.clock_due():
                await step("clock", clock_broadcast)
        finally:
            self.tick += 1
            engine.touch()
            engine.read_error = "; ".join(errors) or None
        await self.publish_health()

    async def poll_loop(self) -> None:
        """Reading passes for as long as the connection lasts. Only cancellation and a failure of the MQTT
        connection end it."""
        while True:
            try:
                await self.poll_pass()
            except self.transport_errors:
                raise
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Reading pass failed outside its steps")
            await asyncio.sleep(self.config.poll_passive_interval)

    async def watch_health(self, period: float = 30.0) -> None:
        """Beside the reading loop: publish the state when it changes while a pass is long or stuck."""
        while True:
            await asyncio.sleep(period)
            await self.publish_health()

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
        """Handle an incoming MQTT command. No exception of a handler leaves this method (only a failure of the
        MQTT connection does): it is logged and answered as `status: "error", reason: "internal_error"`."""
        try:
            await self._dispatch(topic, raw_payload)
        except self.transport_errors:
            raise
        except Exception as err:  # noqa: BLE001 - one bad command must not take the command loop down
            _LOGGER.exception("Command on %s failed", topic)
            first = topic[len(self.base) + 1:].split("/")[0] if topic.startswith(self.base + "/") else "bridge"
            await self.pub.publish(
                self.topic(f"{first}/result"),
                json.dumps({"status": "error", "reason": "internal_error", "detail": f"{type(err).__name__}: {err}"}),
                retain=False,
                qos=1,
            )

    async def _dispatch(self, topic: str, raw_payload: str | bytes) -> None:
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
            if "clock" in res:
                await self.pub.publish(
                    self.topic("bridge/journal"), json.dumps(res["clock"], ensure_ascii=False), retain=False, qos=1
                )
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
                bridge.transport_errors = (aiomqtt.MqttError,)
                for sub in bridge.subscriptions():
                    await client.subscribe(sub, qos=1)
                await bridge.publish_startup()

                async def listen() -> None:
                    async for message in client.messages:
                        await bridge.handle_message(str(message.topic), message.payload)

                tasks = {
                    asyncio.create_task(bridge.poll_loop(), name="reading loop"),
                    asyncio.create_task(bridge.watch_health(), name="health watch"),
                    asyncio.create_task(listen(), name="command loop"),
                }
                try:
                    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                # None of the three ends by itself: whichever did, say so and start over on a new connection.
                for task in done:
                    error = task.exception()
                    if error is None or not isinstance(error, aiomqtt.MqttError):
                        _LOGGER.error("The %s ended (%r): restarting on a new connection", task.get_name(), error)
                        await bridge.publish_offline()
                    if error is not None:
                        raise error
                raise RuntimeError("a loop of the bridge ended")
        except asyncio.CancelledError:
            raise
        except (aiomqtt.MqttError, OSError) as err:
            _LOGGER.warning("MQTT connection error (%s); reconnecting in 5 s", err)
            await asyncio.sleep(5.0)
        except Exception as err:  # noqa: BLE001 - already logged with its loop above; never leave the bridge down
            _LOGGER.warning("Bridge loops stopped (%r); starting again in 5 s", err)
            await asyncio.sleep(5.0)
