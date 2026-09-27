"""按键的简易熔断器：closed -> open -> half-open -> closed/open。

- 连续失败达到阈值 -> OPEN，期间 check_allowed 抛 CircuitOpenError（快速失败）
- 打开满 recovery_seconds -> HALF_OPEN，放行一次探测
- 探测成功 -> CLOSED 复位；探测失败 -> 重新 OPEN
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

from app.errors import CircuitOpenError


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class _Stat:
    state: BreakerState = BreakerState.CLOSED
    failures: int = 0
    opened_at: float = 0.0


@dataclass
class CircuitBreaker:
    """按键熔断器。"""

    failure_threshold: int = 3
    recovery_seconds: float = 2.0
    time_func: Callable[[], float] = time.monotonic
    _stats: dict[str, _Stat] = field(default_factory=dict)

    def _stat(self, key: str) -> _Stat:
        return self._stats.setdefault(key, _Stat())

    def state_of(self, key: str) -> BreakerState:
        """返回键当前的熔断状态；OPEN 到期自动转 HALF_OPEN。"""
        stat = self._stat(key)
        if stat.state is BreakerState.OPEN:
            if self.time_func() - stat.opened_at >= self.recovery_seconds:
                stat.state = BreakerState.HALF_OPEN
        return stat.state

    def check_allowed(self, key: str) -> None:
        """若熔断打开则抛 CircuitOpenError，携带重试等待时间。"""
        stat = self._stat(key)
        if self.state_of(key) is BreakerState.OPEN:
            retry_after = self.recovery_seconds - (self.time_func() - stat.opened_at)
            raise CircuitOpenError(key, max(retry_after, 0.0))

    def record_success(self, key: str) -> None:
        """回源成功：复位为 CLOSED。"""
        stat = self._stat(key)
        stat.state = BreakerState.CLOSED
        stat.failures = 0

    def record_failure(self, key: str) -> None:
        """回源失败：累计失败，达到阈值打开熔断。"""
        stat = self._stat(key)
        stat.failures += 1
        if stat.state is BreakerState.HALF_OPEN or stat.failures >= self.failure_threshold:
            stat.state = BreakerState.OPEN
            stat.opened_at = self.time_func()

    def reset(self, key: str) -> None:
        """手动复位单个键。"""
        self._stats.pop(key, None)
