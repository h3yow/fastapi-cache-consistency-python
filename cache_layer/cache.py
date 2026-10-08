"""查询缓存层：TTL 过期、空结果缓存、并发回源合并、按键隔离熔断降级。

容量上限与 LRU 淘汰见 QueryCache 的 max_capacity 参数。
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from enum import Enum
from typing import Awaitable, Callable, Optional

logger = logging.getLogger("cache_layer")


class _Empty:
    """空结果哨兵：用于区分“缓存了空结果”和“没缓存”。"""

    def __repr__(self) -> str:
        return "EMPTY"


EMPTY = _Empty()


class Source(str, Enum):
    HIT = "hit"            # 缓存命中（含空结果命中）
    BACKEND = "backend"    # 本次请求真正触发了回源
    JOINED = "joined"      # 并发等待方，复用他人回源结果
    DEGRADED = "degraded"  # 底层不可用 / 该键熔断中，返回降级结果


class DegradeReason(str, Enum):
    """对外固定、可枚举的降级类别：不含任何底层异常类型名或调用栈。"""

    TIMEOUT = "timeout"              # 回源超时
    UNAVAILABLE = "unavailable"      # 连不上 / 底层不可用
    CIRCUIT_OPEN = "circuit_open"    # 该键熔断冷却中，被直接挡住


@dataclass
class CacheResult:
    value: Optional[list[str]]  # None 表示空结果或降级无值
    source: Source
    reason: Optional[DegradeReason] = None  # 降级类别（对外安全）；命中/回源时为 None


class CircuitBreaker:
    """单个键的熔断状态机：失败计数 -> 断开 -> 冷却 -> 半开试探 -> 闭合/重开。"""

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

    @property
    def failures(self) -> int:
        return self._failures

    def remaining_cooldown(self, now: Optional[float] = None) -> float:
        if self._opened_at is None:
            return 0.0
        now = self._clock() if now is None else now
        return max(0.0, self.cooldown - (now - self._opened_at))

    def allow(self, now: Optional[float] = None) -> bool:
        if self._opened_at is None:
            return True
        now = self._clock() if now is None else now
        if now - self._opened_at >= self.cooldown:
            return True  # 半开：放行一次试探
        return False

    def on_success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def on_failure(self, now: Optional[float] = None) -> None:
        now = self._clock() if now is None else now
        self._failures += 1
        if self._failures >= self.failure_threshold:
            # 首次达到阈值断开；半开试探失败则用当前时间重新计时冷却
            self._opened_at = now


class QueryCache:
    """读接口缓存。

    - 命中不回源；未命中单飞回源；失败不写缓存并降级。
    - 熔断状态按键独立：每个键的失败、降级、恢复互不影响。
    """

    def __init__(
        self,
        loader: Callable[[str], Awaitable[list[str]]],
        ttl: float = 30.0,
        failure_threshold: int = 3,
        cooldown: float = 5.0,
        load_timeout: float = 2.0,
        max_capacity: Optional[int] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._loader = loader
        self._ttl = ttl
        self._failure_threshold = failure_threshold
        self._cooldown = cooldown
        self._load_timeout = load_timeout
        if max_capacity is not None and max_capacity <= 0:
            raise ValueError("max_capacity 必须为正整数或 None（不限容量）")
        self._max_capacity = max_capacity
        self._clock = clock
        # OrderedDict：队首为最久未用，队尾为最近使用；淘汰只动这里，不影响 _inflight
        self._store: OrderedDict[str, tuple[object, float]] = OrderedDict()
        self._inflight: dict[str, asyncio.Task[CacheResult]] = {}
        # 每个键一份独立熔断状态；闭合（无失败）的键不在表里，状态天然隔离
        self._breakers: dict[str, CircuitBreaker] = {}

    def is_circuit_open(self, key: str) -> bool:
        """供观测/测试：某键当前是否处于熔断断开状态。"""
        breaker = self._breakers.get(key)
        return breaker is not None and breaker.is_open

    @property
    def size(self) -> int:
        """当前缓存条目数（不含在途回源）。"""
        return len(self._store)

    def _get_breaker(self, key: str) -> CircuitBreaker:
        breaker = self._breakers.get(key)
        if breaker is None:
            breaker = CircuitBreaker(
                failure_threshold=self._failure_threshold,
                cooldown=self._cooldown,
                clock=self._clock,
            )
            self._breakers[key] = breaker
        return breaker

    def _lookup(self, key: str) -> Optional[CacheResult]:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if self._clock() >= expires_at:
            del self._store[key]  # 过期必须重新回源，不得返回旧值
            logger.info("expire key=%s basis=ttl_passed", key)
            return None
        self._store.move_to_end(key)  # LRU：命中即刷新为最近使用
        return CacheResult(value=None if value is EMPTY else value,  # type: ignore[arg-type]
                           source=Source.HIT)

    def _put(self, key: str, value: object) -> None:
        """写入缓存并按 LRU 淘汰：只淘汰缓存条目，绝不影响同键在途回源合并。"""
        self._store[key] = (value, self._clock() + self._ttl)
        self._store.move_to_end(key)  # 写入也视为最近使用
        if self._max_capacity is not None:
            while len(self._store) > self._max_capacity:
                evicted_key, _ = self._store.popitem(last=False)
                logger.warning(
                    "evict key=%s basis=lru capacity=%d size=%d inflight=%s",
                    evicted_key, self._max_capacity, len(self._store),
                    evicted_key in self._inflight,
                )

    async def get(self, key: str) -> CacheResult:
        hit = self._lookup(key)
        if hit is not None:
            return hit

        task = self._inflight.get(key)
        if task is not None:
            result = await asyncio.shield(task)
            logger.info("join key=%s source=%s basis=inflight_merge", key, result.source.value)
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
        breaker = self._breakers.get(key)
        now = self._clock()
        if breaker is not None and not breaker.allow(now):
            remaining = breaker.remaining_cooldown(now)
            logger.warning(
                "degrade key=%s reason=%s basis=breaker_open failures=%d "
                "cooldown=%.3f remaining=%.3f detail=该键冷却期内直接拒绝，不访问底层",
                key, DegradeReason.CIRCUIT_OPEN.value, breaker.failures,
                breaker.cooldown, remaining,
            )
            return CacheResult(value=None, source=Source.DEGRADED,
                               reason=DegradeReason.CIRCUIT_OPEN)

        logger.info("load_start key=%s timeout=%.3f", key, self._load_timeout)
        try:
            value = await asyncio.wait_for(self._loader(key), timeout=self._load_timeout)
        except asyncio.TimeoutError:
            # 底层细节（超时阈值、异常类型）只写内部日志，对外只给固定类别
            detail = f"TimeoutError: load exceeded {self._load_timeout}s"
            self._record_failure(key, DegradeReason.TIMEOUT, detail)
            return CacheResult(value=None, source=Source.DEGRADED,
                               reason=DegradeReason.TIMEOUT)
        except Exception as exc:  # 其它底层异常（连接失败等）一律不透出
            detail = f"{type(exc).__name__}: {exc}"
            self._record_failure(key, DegradeReason.UNAVAILABLE, detail)
            return CacheResult(value=None, source=Source.DEGRADED,
                               reason=DegradeReason.UNAVAILABLE)

        if breaker is not None:
            breaker.on_success()
            self._breakers.pop(key, None)  # 试探成功：该键恢复，回到无失败状态
        stored: object = EMPTY if len(value) == 0 else value
        self._put(key, stored)
        logger.info(
            "load_ok key=%s empty=%s basis=backend_success ttl=%.3f stored=yes",
            key, stored is EMPTY, self._ttl,
        )
        return CacheResult(value=None if stored is EMPTY else value,
                           source=Source.BACKEND)

    def _record_failure(self, key: str, reason: DegradeReason, detail: str) -> None:
        breaker = self._get_breaker(key)
        breaker.on_failure()
        logger.warning(
            "degrade key=%s reason=%s basis=backend_exception failures=%d "
            "opened=%s cooldown=%.3f detail=%s",
            key, reason.value, breaker.failures, breaker.is_open,
            breaker.cooldown, detail,
        )
