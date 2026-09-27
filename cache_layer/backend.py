"""底层存储模拟：可注入延迟与故障，用于本地复现降级/恢复场景。"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


class BackendUnavailableError(Exception):
    """底层存储不可用（连接失败 / 超时 / 熔断期间调用）。"""


@dataclass
class FakeBackend:
    """可编程故障的底层存储。

    - latency: 每次查询的模拟耗时（秒）
    - fail_times: 接下来多少次调用抛 BackendUnavailableError
    - down: 置 True 后所有调用都失败，置 False 表示恢复
    - calls: 记录真实回源次数（测试断言用）
    """

    latency: float = 0.0
    fail_times: int = 0
    down: bool = False
    calls: int = 0
    _data: dict[str, list[str]] = field(default_factory=dict)

    def seed(self, key: str, value: list[str]) -> None:
        self._data[key] = value

    async def fetch(self, key: str) -> list[str]:
        self.calls += 1
        if self.latency:
            await asyncio.sleep(self.latency)
        if self.down or self.fail_times > 0:
            if self.fail_times > 0:
                self.fail_times -= 1
            raise BackendUnavailableError(f"backend unavailable for key={key!r}")
        return list(self._data.get(key, []))
