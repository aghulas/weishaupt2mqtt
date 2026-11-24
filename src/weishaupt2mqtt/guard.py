"""Write guard for weishaupt2mqtt: level switches, bounds, and EEPROM rate limits."""
from __future__ import annotations

import math
import time
from collections import deque
from collections.abc import Callable

MIN_INTERVAL = 10.0
MAX_PER_HOUR = 30
MAX_PER_HOUR_RANGE = (1, 200)

REASONS = {
    "writes_disabled": "writes are disabled",
    "installer_writes_disabled": "installer-level writes are disabled",
    "installer_topic_required": "{setting} is an installer-level setting: publish to /installer/set",
    "wrong_level_topic": "{setting} is a user-level setting: publish to /set",
    "confirmation_required": "{setting} is an installer-level setting: include '\"confirm\": true'",
    "read_only": "{setting} is read-only",
    "shared_with": "{setting} is stored on {target}: write there instead",
    "not_writable": "{setting} is not writable: {reason}",
    "stock_ebusd_not_allowed": "ebusd does not compute the run-time checksum (enable allow_stock_ebusd for non-boiler controllers)",
    "capability_unknown": "ebusd checksum capability could not be verified ({reason})",
    "source_not_own": "{setting} requires ebusd own address 31 (current own address: {own})",
    "invalid_step": "{value} is not on a step of {step} from {minimum}",
    "out_of_range": "{value} is outside [{minimum}, {maximum}]",
    "linked_bound": "{value} violates linked bound [{minimum}, {maximum}]",
    "linked_unknown": "linked setting {other} could not be read",
    "rate_limit": "hourly write limit ({limit}) reached",
    "register_interval": "register was written less than {seconds} s ago",
    "program_limit": "{needed} slots to write exceeds {left} writes left within the hour (limit {limit})",
    "read_failed": "{setting} could not be read before write ({reason})",
    "unavailable": "ebusd is not reachable",
    "invalid_payload": "invalid command payload: {reason}",
    "bad_day_format": "{day}: {part!r} is not a valid range (expected HH:MM-HH:MM)",
    "bad_day_quarter": "{day}: {part}: times must be in 15-minute steps (00:00 to 24:00)",
    "bad_day_order": "{day}: {part}: start must be before end",
    "bad_day_too_many": "{day}: at most 3 intervals per day",
    "bad_day_overlap": "{day}: intervals overlap",
}


class WriteRefused(Exception):
    """Raised when a write is blocked before anything is sent to the bus."""

    def __init__(self, code: str, **placeholders: object) -> None:
        self.code = code
        self.placeholders = {key: str(value) for key, value in placeholders.items()}
        template = REASONS.get(code, code)
        super().__init__(template.format(**self.placeholders))


def hourly_limit(value: object) -> int:
    """Validate and return the hourly write limit."""
    if value is None:
        return MAX_PER_HOUR
    if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
        raise ValueError(f"not an integer: {value!r}")
    low, high = MAX_PER_HOUR_RANGE
    if not low <= int(value) <= high:
        raise ValueError(f"{value} is outside {low} to {high}")
    return int(value)


class WriteGuard:
    """Enforces level switches, bounds, per-register cooldown, and hourly write budget."""

    def __init__(
        self,
        writes_enabled: bool = False,
        installer_writes_enabled: bool = False,
        clock: Callable[[], float] = time.time,
        max_per_hour: int = MAX_PER_HOUR,
    ) -> None:
        self.writes_enabled = writes_enabled
        self.installer_writes_enabled = installer_writes_enabled
        self.max_per_hour = hourly_limit(max_per_hour)
        self._clock = clock
        self._writes: deque[float] = deque()
        self._last_by_register: dict[str, float] = {}

    def _prune(self, now: float) -> None:
        while self._writes and now - self._writes[0] > 3600.0:
            self._writes.popleft()

    @property
    def used_last_hour(self) -> int:
        self._prune(self._clock())
        return len(self._writes)

    @property
    def left_this_hour(self) -> int:
        return max(0, self.max_per_hour - self.used_last_hour)

    def check_level(self, *, is_installer: bool) -> None:
        if not self.writes_enabled:
            raise WriteRefused("writes_disabled")
        if is_installer and not self.installer_writes_enabled:
            raise WriteRefused("installer_writes_disabled")

    def check_budget(self, needed: int = 1) -> None:
        now = self._clock()
        self._prune(now)
        left = max(0, self.max_per_hour - len(self._writes))
        if needed > left:
            if needed == 1:
                raise WriteRefused("rate_limit", limit=self.max_per_hour)
            raise WriteRefused("program_limit", needed=needed, left=left, limit=self.max_per_hour)

    def check_register(
        self,
        register_id: str,
        value: float,
        minimum: float,
        maximum: float,
        *,
        is_installer: bool = False,
        linked: bool = False,
    ) -> None:
        self.check_level(is_installer=is_installer)
        if math.isnan(value) or not minimum <= value <= maximum:
            code = "linked_bound" if linked else "out_of_range"
            raise WriteRefused(code, value=value, minimum=minimum, maximum=maximum)
        now = self._clock()
        self._prune(now)
        if len(self._writes) >= self.max_per_hour:
            raise WriteRefused("rate_limit", limit=self.max_per_hour)
        last = self._last_by_register.get(register_id)
        if last is not None and now - last < MIN_INTERVAL:
            raise WriteRefused("register_interval", seconds=f"{MIN_INTERVAL:.0f}")

    def record(self, register_id: str) -> None:
        now = self._clock()
        self._prune(now)
        self._writes.append(now)
        self._last_by_register[register_id] = now
