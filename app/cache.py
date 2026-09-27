"""本地查询缓存：TTL 过期、空结果缓存、同键并发回源合并。

语义：
- 命中且未过期：直接返回，绝不调用 loader；过期条目一律回源，不返回旧值。
- 空结果（默认 None）会被缓存，使用更短的 empty_ttl，与"没有缓存"区分。
- 同键并发：首个请求执行 loader，其余请求 await 同一个 Future，只回源一次。
- loader 抛异常：不写缓存，异常原样传播给所有等待方，由上层做降级。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Awaitable, Callable, Generic, TypeVar

logger = logging.getLogger("app.cache")

T = TypeVar("T")


class CacheStatus(str, Enum):
    """一次读取的最终判定。"""

    HIT = "hit"          # 命中有效缓存（含空结果缓存）
    MISS = "miss"        # 未命中，回源成功并写入
    DEGRADED = "degraded"  # 底层不可用 / 熔断，降级（由上层填充）


def _default_is_empty(value: object) -> bool:
    return value is None


@dataclass
class CacheEntry(Generic[T]):
    """单条缓存记录。"""

    value: T | None
    expires_at: float
    is_empty: bool = False

    def is_fresh(self, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        return now < self.expires_at


@dataclass
class CacheResult(Generic[T]):
    """读取结果包装，携带判定依据，供调用方与日志区分。"""

    status: CacheStatus
    value: T | None
    is_empty: bool = False
    elapsed_ms: float = 0.0
    reason: str = ""


@dataclass
class QueryCache(Generic[T]):
    """本地内存查询缓存。"""

    ttl_seconds: float = 5.0
    empty_ttl_seconds: float = 1.0
    is_empty: Callable[[T | None], bool] = _default_is_empty
    time_func: Callable[[], float] = time.monotonic
    _store: dict[str, CacheEntry[T]] = field(default_factory=dict)
    _inflight: dict[str, asyncio.Future[CacheResult[T]]] = field(default_factory=dict)

    async def get_or_load(
        self,
        key: str,
        loader: Callable[[], Awaitable[T]],
    ) -> CacheResult[T]:
        """命中直接返回；未命中回源；同键并发只回源一次。"""
        start = self.time_func()
        entry = self._store.get(key)
        now = start

        if entry is not None and entry.is_fresh(now):
            elapsed = (self.time_func() - start) * 1000
            basis = "empty-result cached" if entry.is_empty else "fresh entry"
            result = CacheResult(
                status=CacheStatus.HIT,
                value=entry.value,
                is_empty=entry.is_empty,
                elapsed_ms=elapsed,
                reason=basis,
            )
            logger.info(
                "key=%s decision=hit elapsed_ms=%.3f basis=%s",
                key, elapsed, basis,
            )
            return result

        miss_basis = "no entry" if entry is None else "entry expired"

        inflight = self._inflight.get(key)
        if inflight is not None:
            # 已有同键回源在途：等待同一次结果，不再触发 loader
            shared = await inflight
            elapsed = (self.time_func() - start) * 1000
            logger.info(
                "key=%s decision=join-inflight elapsed_ms=%.3f basis=%s",
                key, elapsed, miss_basis,
            )
            return shared

        future: asyncio.Future[CacheResult[T]] = (
            asyncio.get_running_loop().create_future()
        )
        self._inflight[key] = future
        try:
            logger.info("key=%s decision=load elapsed_ms=%.3f basis=%s",
                        key, 0.0, miss_basis)
            value = await loader()
            empty = self.is_empty(value)
            ttl = self.empty_ttl_seconds if empty else self.ttl_seconds
            self._store[key] = CacheEntry(
                value=value,
                expires_at=self.time_func() + ttl,
                is_empty=empty,
            )
            elapsed = (self.time_func() - start) * 1000
            result = CacheResult(
                status=CacheStatus.MISS,
                value=value,
                is_empty=empty,
                elapsed_ms=elapsed,
                reason="origin loaded" + (" (empty)" if empty else ""),
            )
            future.set_result(result)
            logger.info(
                "key=%s decision=loaded elapsed_ms=%.3f basis=%s empty=%s ttl=%.3f",
                key, elapsed, miss_basis, empty, ttl,
            )
            return result
        except BaseException as exc:
            # 回源失败：不写缓存，并把同一失败传播给所有等待方
            if not future.done():
                future.set_exception(exc)
            # 标记异常已取走，避免无等待方时 Future 析构告警；
            # 已在 await 该 future 的等待方仍会收到同一个异常。
            if not future.cancelled():
                future.exception()
            elapsed = (self.time_func() - start) * 1000
            logger.warning(
                "key=%s decision=load-failed elapsed_ms=%.3f basis=%s error=%s",
                key, elapsed, miss_basis, type(exc).__name__,
            )
            raise
        finally:
            self._inflight.pop(key, None)

    def invalidate(self, key: str) -> None:
        """删除单个键。"""
        self._store.pop(key, None)

    def clear(self) -> None:
        """清空全部缓存。"""
        self._store.clear()
