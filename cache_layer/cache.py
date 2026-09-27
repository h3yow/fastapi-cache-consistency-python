"""查询缓存层：TTL 过期、空结果缓存、并发回源合并、熔断降级。"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from enum import Enum
from typing import Awaitable, Callable, Optional


class _Empty:
    """空结果哨兵：用于区分“缓存了空结果”和“没缓存”。"""

    def __repr__(self) -> str:
        return "EMPTY"


EMPTY = _Empty()


class Source(str, Enum):
    HIT = "hit"            # 缓存命中（含空结果命中）
    BACKEND = "backend"    # 本次请求真正触发了回源
    JOINED = "joined"      # 并发等待方，复用他人回源结果
    DEGRADED = "degraded"  # 底层不可用 / 熔断，返回降级结果


@dataclass
class CacheResult:
    value: Optional[list[str]]  # None 表示空结果或降级无值
    source: Source
    error: Optional[str] = None  # 降级时的失败原因（不外抛异常）


class CircuitBreaker:
    """连续失败达到阈值后断开，冷却期内直接拒绝，冷却结束半开试探。"""

    def __init__(self, failure_threshold: int = 3, cooldown: float = 5.0,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown
        self._clock = clock
        self._failures = 0
        self._opened_at: Optional[float] = None

    @property
    def is_open(self) -> bool:
        return self._opened_at is not None

    def allow(self) -> bool:
        if self._opened_at is None:
            return True
        if self._clock() - self._opened_at >= self.cooldown:
            return True  # 半开：放行一次试探
        return False

    def on_success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def on_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.failure_threshold and self._opened_at is None:
            self._opened_at = self._clock()


class QueryCache:
    """读接口缓存：命中不回源；未命中单飞回源；失败不写缓存并降级。"""

    def __init__(
        self,
        loader: Callable[[str], Awaitable[list[str]]],
        ttl: float = 30.0,
        breaker: Optional[CircuitBreaker] = None,
        load_timeout: float = 2.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._loader = loader
        self._ttl = ttl
        self._breaker = breaker or CircuitBreaker(clock=clock)
        self._load_timeout = load_timeout
        self._clock = clock
        self._store: dict[str, tuple[object, float]] = {}
        self._inflight: dict[str, asyncio.Task[CacheResult]] = {}

    def _lookup(self, key: str) -> Optional[CacheResult]:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if self._clock() >= expires_at:
            del self._store[key]  # 过期必须重新回源，不得返回旧值
            return None
        return CacheResult(value=None if value is EMPTY else value,  # type: ignore[arg-type]
                           source=Source.HIT)

    async def get(self, key: str) -> CacheResult:
        hit = self._lookup(key)
        if hit is not None:
            return hit

        task = self._inflight.get(key)
        if task is not None:
            result = await asyncio.shield(task)
            if result.source is Source.BACKEND:
                return replace(result, source=Source.JOINED)
            return result

        task = asyncio.ensure_future(self._load(key))
        self._inflight[key] = task
        try:
            return await asyncio.shield(task)
        finally:
            self._inflight.pop(key, None)

    async def _load(self, key: str) -> CacheResult:
        if not self._breaker.allow():
            return CacheResult(value=None, source=Source.DEGRADED,
                               error="circuit open: backend recently failing")
        try:
            value = await asyncio.wait_for(self._loader(key), timeout=self._load_timeout)
        except Exception as exc:  # 底层异常（含超时）不透出，转为降级结果
            self._breaker.on_failure()
            return CacheResult(value=None, source=Source.DEGRADED,
                               error=f"{type(exc).__name__}: {exc}")
        self._breaker.on_success()
        stored: object = EMPTY if len(value) == 0 else value
        self._store[key] = (stored, self._clock() + self._ttl)
        return CacheResult(value=None if stored is EMPTY else value,
                           source=Source.BACKEND)
