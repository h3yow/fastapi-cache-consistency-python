"""查询缓存层：TTL 过期、空结果缓存、并发回源合并、按键隔离熔断降级、LRU 容量淘汰。"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from enum import Enum
from typing import Awaitable, Callable, Optional

logger = logging.getLogger("cache-layer")


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


class DegradeReason(str, Enum):
    """对外固定、可枚举的降级类别。取值稳定，不含任何底层实现细节。"""

    TIMEOUT = "timeout"            # 回源超时
    CONNECTIVITY = "connectivity"  # 连不上 / 底层不可用
    CIRCUIT_OPEN = "circuit_open"  # 该键熔断中（冷却期），请求被直接挡住


@dataclass
class CacheResult:
    value: Optional[list[str]]  # None 表示空结果或降级无值
    source: Source
    error: Optional[str] = None  # 兼容字段：对外只给固定类别，不含异常类型名/调用栈
    reason: Optional[DegradeReason] = None  # 降级类别（仅 source=degraded 时有值）


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
        self._max_capacity = max_capacity
        self._clock = clock
        self._store: OrderedDict[str, tuple[object, float]] = OrderedDict()
        self._inflight: dict[str, asyncio.Task[CacheResult]] = {}
        # 每个键各自持有熔断器：失败、断开、冷却、半开恢复互不影响
        self._breakers: dict[str, CircuitBreaker] = {}

    @property
    def size(self) -> int:
        """当前缓存条目数（含空结果条目）。"""
        return len(self._store)

    @property
    def capacity(self) -> Optional[int]:
        return self._max_capacity

    def _breaker_for(self, key: str) -> CircuitBreaker:
        breaker = self._breakers.get(key)
        if breaker is None:
            breaker = CircuitBreaker(
                failure_threshold=self._failure_threshold,
                cooldown=self._cooldown,
                clock=self._clock,
            )
            self._breakers[key] = breaker
        return breaker

    def is_circuit_open(self, key: str) -> bool:
        """供观测：指定键当前是否处于熔断断开状态（不影响其他键）。"""
        breaker = self._breakers.get(key)
        return breaker.is_open if breaker is not None else False

    def _lookup(self, key: str) -> Optional[CacheResult]:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if self._clock() >= expires_at:
            del self._store[key]  # 过期必须重新回源，不得返回旧值
            logger.info("key=%s 判定依据: 已过期，删除旧值并重新回源", key)
            return None
        self._store.move_to_end(key)  # 命中即视为最近使用，LRU 顺序前移
        return CacheResult(value=None if value is EMPTY else value,  # type: ignore[arg-type]
                           source=Source.HIT)

    def _put(self, key: str, stored: object, expires_at: float) -> None:
        """写入并按 LRU 淘汰：超容量时从最久未使用端逐出。

        正在并发回源中的键不会被淘汰（其等待方仍要复用本次结果）；
        结构上 inflight 键此时不在 _store 中，跳过检查是一道防御。
        """
        self._store[key] = (stored, expires_at)
        self._store.move_to_end(key)
        if self._max_capacity is None:
            return
        while len(self._store) > self._max_capacity:
            victim, victim_entry = self._store.popitem(last=False)
            if victim in self._inflight:
                # 理论上不会发生：inflight 键尚无条目。防御性放回并移到末端，
                # 继续淘汰下一个最久未使用的键。
                logger.warning("key=%s 判定依据: 淘汰候选正在回源，跳过", victim)
                self._store[victim] = victim_entry
                self._store.move_to_end(victim)
                continue
            logger.info(
                "key=%s 判定依据: 缓存达容量上限 %s，按 LRU 淘汰该键（淘汰后再访问重新回源）",
                victim, self._max_capacity,
            )

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
        breaker = self._breaker_for(key)
        if not breaker.allow():
            logger.info(
                "key=%s 判定依据: 该键熔断冷却期内，直接降级不回源 threshold=%s cooldown=%s",
                key, breaker.failure_threshold, self._cooldown,
            )
            return CacheResult(value=None, source=Source.DEGRADED,
                               error=DegradeReason.CIRCUIT_OPEN.value,
                               reason=DegradeReason.CIRCUIT_OPEN)
        try:
            value = await asyncio.wait_for(self._loader(key), timeout=self._load_timeout)
        except asyncio.TimeoutError:
            # 底层细节（异常类型、耗时、超时阈值）只写内部日志，不进对外结果
            breaker.on_failure()
            logger.warning(
                "key=%s 判定依据: 回源超时 timeout=%.3fs，归类 timeout（失败计数+1）",
                key, self._load_timeout, exc_info=True,
            )
            return CacheResult(value=None, source=Source.DEGRADED,
                               error=DegradeReason.TIMEOUT.value,
                               reason=DegradeReason.TIMEOUT)
        except Exception as exc:
            breaker.on_failure()
            logger.warning(
                "key=%s 判定依据: 底层连接/可用性异常 %s: %s，归类 connectivity（失败计数+1）",
                key, type(exc).__name__, exc, exc_info=True,
            )
            return CacheResult(value=None, source=Source.DEGRADED,
                               error=DegradeReason.CONNECTIVITY.value,
                               reason=DegradeReason.CONNECTIVITY)
        breaker.on_success()
        self._breakers.pop(key, None)  # 该键已恢复，清掉它的熔断状态
        stored: object = EMPTY if len(value) == 0 else value
        self._put(key, stored, self._clock() + self._ttl)
        logger.info("key=%s 判定依据: 回源成功并写入缓存 empty=%s ttl=%.1f size=%s",
                    key, stored is EMPTY, self._ttl, len(self._store))
        return CacheResult(value=None if stored is EMPTY else value,
                           source=Source.BACKEND)
