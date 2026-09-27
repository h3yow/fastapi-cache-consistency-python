"""缓存层与存储层的异常定义。

调用方通过不同的异常类型区分失败原因：
- StorageTimeoutError: 回源超时
- StorageUnavailableError: 底层连接不可用
- CircuitOpenError: 熔断打开期间快速失败
"""

from __future__ import annotations


class CacheError(Exception):
    """缓存层异常基类。"""


class StorageError(CacheError):
    """底层存储异常基类。"""


class StorageTimeoutError(StorageError):
    """回源超时。"""


class StorageUnavailableError(StorageError):
    """底层存储连接不上 / 不可用。"""


class CircuitOpenError(StorageError):
    """熔断器打开，请求被快速拒绝。"""

    def __init__(self, key: str, retry_after: float) -> None:
        super().__init__(f"circuit open for key={key!r}, retry_after={retry_after:.3f}s")
        self.key = key
        self.retry_after = retry_after
