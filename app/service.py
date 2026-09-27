"""读服务：缓存 + 熔断 + 底层存储的组合，对外返回可区分的结果。

降级规则（对外可预期，不抛出底层异常）：
- 熔断 OPEN：不访问底层，直接降级，reason="circuit-open"
- 回源超时：降级，reason="timeout"
- 底层连接不上：降级，reason="unavailable"
- 成功命中 / 回源：source 分别为 "cache" / "origin"
空结果与降级用 found + source + reason 三者区分。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.breaker import CircuitBreaker
from app.cache import CacheResult, CacheStatus, QueryCache
from app.errors import (
    CircuitOpenError,
    StorageError,
    StorageTimeoutError,
    StorageUnavailableError,
)
from app.storage import UserStorage


@dataclass
class ReadOutcome:
    """对外读取结果。"""

    found: bool
    source: str          # "cache" | "origin" | "degraded"
    data: dict[str, Any] | None
    reason: str


class UserService:
    """用户读服务。"""

    def __init__(
        self,
        storage: UserStorage,
        cache: QueryCache[dict[str, Any]] | None = None,
        breaker: CircuitBreaker | None = None,
    ) -> None:
        self.storage = storage
        self.cache = cache if cache is not None else QueryCache[dict[str, Any]]()
        self.breaker = breaker if breaker is not None else CircuitBreaker()

    async def get_user(self, user_id: str) -> ReadOutcome:
        """读取用户：优先缓存，未命中回源，异常时降级。"""
        key = f"user:{user_id}"

        try:
            self.breaker.check_allowed(key)
        except CircuitOpenError as exc:
            return ReadOutcome(
                found=False,
                source="degraded",
                data=None,
                reason=f"circuit-open (retry_after={exc.retry_after:.3f}s)",
            )

        async def load() -> dict[str, Any] | None:
            return await self.storage.get_user(user_id)

        try:
            result: CacheResult[dict[str, Any]] = await self.cache.get_or_load(key, load)
        except StorageTimeoutError:
            self.breaker.record_failure(key)
            return ReadOutcome(False, "degraded", None, "timeout while loading from storage")
        except StorageUnavailableError:
            self.breaker.record_failure(key)
            return ReadOutcome(False, "degraded", None, "storage unavailable")
        except StorageError as exc:
            self.breaker.record_failure(key)
            return ReadOutcome(False, "degraded", None, f"storage error: {type(exc).__name__}")

        self.breaker.record_success(key)
        source = "cache" if result.status is CacheStatus.HIT else "origin"
        return ReadOutcome(
            found=not result.is_empty,
            source=source,
            data=None if result.is_empty else result.value,
            reason=result.reason,
        )
