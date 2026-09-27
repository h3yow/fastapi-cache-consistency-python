"""底层存储：协议定义与可注入故障的内存模拟实现。

故障模式用于本地复现三类回源场景：
- ok: 正常返回
- timeout: 模拟超时，抛 StorageTimeoutError
- unavailable: 模拟连接不上，抛 StorageUnavailableError
"""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import Protocol

from app.errors import StorageTimeoutError, StorageUnavailableError


class UserStorage(Protocol):
    """用户查询存储协议。"""

    async def get_user(self, user_id: str) -> dict | None:
        """按 ID 查询用户；不存在返回 None；不可用时抛 StorageError。"""
        ...


class FaultMode(str, Enum):
    OK = "ok"
    TIMEOUT = "timeout"
    UNAVAILABLE = "unavailable"


class InMemoryUserStorage:
    """带故障注入的内存存储，调用次数用于断言回源次数。"""

    def __init__(
        self,
        data: dict[str, dict] | None = None,
        fault: FaultMode = FaultMode.OK,
        latency: float = 0.0,
    ) -> None:
        self._data = dict(data or {})
        self.fault = fault
        self.latency = latency
        self.calls = 0

    def set_fault(self, fault: FaultMode) -> None:
        self.fault = fault

    async def get_user(self, user_id: str) -> dict | None:
        self.calls += 1
        if self.latency:
            await asyncio.sleep(self.latency)
        if self.fault is FaultMode.TIMEOUT:
            raise StorageTimeoutError(f"storage timeout: user_id={user_id!r}")
        if self.fault is FaultMode.UNAVAILABLE:
            raise StorageUnavailableError(f"storage unavailable: user_id={user_id!r}")
        return self._data.get(user_id)
