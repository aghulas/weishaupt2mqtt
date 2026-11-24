"""Runtime configuration for weishaupt2mqtt (env vars, JSON file, or HA add-on options.json)."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .guard import MAX_PER_HOUR, hourly_limit

DEFAULT_METADATA_PATH = Path(__file__).resolve().parents[2] / "data" / "metadata.json"


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Config:
    """Bridge configuration."""

    # ebusd connection
    ebusd_host: str = "127.0.0.1"
    ebusd_port: int = 8888
    ebusd_timeout: float = 10.0

    # MQTT broker
    mqtt_host: str = "127.0.0.1"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    base_topic: str = "weishaupt"

    # Definitions metadata
    metadata_path: Path = DEFAULT_METADATA_PATH

    # Polling intervals (seconds)
    poll_passive_interval: float = 15.0
    poll_cyclic_interval: float = 60.0
    poll_program_interval: float = 3600.0
    poll_faults_interval: float = 3600.0
    program_cache_age: int = 86400
    faults_cache_age: int = 86400

    # Writes & safety
    writes_enabled: bool = False
    installer_writes_enabled: bool = False
    allow_stock_ebusd: bool = False
    max_writes_per_hour: int = MAX_PER_HOUR

    # Clock broadcast
    clock_sync_enabled: bool = False
    clock_sync_interval: float = 1800.0

    # Persistence of runtime switches (writes_enabled / installer_writes_enabled)
    state_file: Path | None = None

    @classmethod
    def from_sources(cls, options_path: Path | None = None) -> "Config":
        """Load from an optional JSON file (e.g. /data/options.json), overridden by env vars."""
        raw: dict[str, Any] = {}
        if options_path is not None and options_path.is_file():
            raw = json.loads(options_path.read_text(encoding="utf-8"))

        def get(key: str, env: str, default: Any) -> Any:
            if env in os.environ and os.environ[env] != "":
                return os.environ[env]
            return raw.get(key, default)

        metadata_val = get("metadata_path", "WEISHAUPT_METADATA_PATH", str(DEFAULT_METADATA_PATH))
        state_val = get("state_file", "WEISHAUPT_STATE_FILE", None)
        base_topic = str(get("base_topic", "WEISHAUPT_BASE_TOPIC", "weishaupt")).strip("/") or "weishaupt"

        return cls(
            ebusd_host=str(get("ebusd_host", "EBUSD_HOST", "127.0.0.1")),
            ebusd_port=int(get("ebusd_port", "EBUSD_PORT", 8888)),
            ebusd_timeout=float(get("ebusd_timeout", "EBUSD_TIMEOUT", 10.0)),
            mqtt_host=str(get("mqtt_host", "MQTT_HOST", "127.0.0.1")),
            mqtt_port=int(get("mqtt_port", "MQTT_PORT", 1883)),
            mqtt_username=get("mqtt_username", "MQTT_USERNAME", None) or None,
            mqtt_password=get("mqtt_password", "MQTT_PASSWORD", None) or None,
            base_topic=base_topic,
            metadata_path=Path(metadata_val),
            poll_passive_interval=float(get("poll_passive_interval", "POLL_PASSIVE_INTERVAL", 15.0)),
            poll_cyclic_interval=float(get("poll_cyclic_interval", "POLL_CYCLIC_INTERVAL", 60.0)),
            poll_program_interval=float(get("poll_program_interval", "POLL_PROGRAM_INTERVAL", 300.0)),
            poll_faults_interval=float(get("poll_faults_interval", "POLL_FAULTS_INTERVAL", 300.0)),
            writes_enabled=_bool(get("writes_enabled", "WRITES_ENABLED", False)),
            installer_writes_enabled=_bool(get("installer_writes_enabled", "INSTALLER_WRITES_ENABLED", False)),
            allow_stock_ebusd=_bool(get("allow_stock_ebusd", "ALLOW_STOCK_EBUSD", False)),
            max_writes_per_hour=hourly_limit(int(get("max_writes_per_hour", "MAX_WRITES_PER_HOUR", MAX_PER_HOUR))),
            clock_sync_enabled=_bool(get("clock_sync_enabled", "CLOCK_SYNC_ENABLED", False)),
            clock_sync_interval=float(get("clock_sync_interval", "CLOCK_SYNC_INTERVAL", 1800.0)),
            state_file=Path(state_val) if state_val else None,
        )
