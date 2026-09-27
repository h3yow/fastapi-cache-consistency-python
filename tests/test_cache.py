"""缓存层测试：并发回源、过期、空结果、底层异常、熔断与恢复。"""

from __future__ import annotations

import asyncio

import pytest

from app.breaker import CircuitBreaker
from app.cache import QueryCache
from app.service import UserService
from app.storage import FaultMode, InMemoryUserStorage


class FakeClock:
    """可控时钟，用于过期与熔断恢复的确定性测试。"""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_service(
    clock: FakeClock,
    *,
    data: dict[str, dict] | None = None,
    fault: FaultMode = FaultMode.OK,
    latency: float = 0.0,
    ttl: float = 5.0,
    empty_ttl: float = 1.0,
    threshold: int = 2,
    recovery: float = 1.0,
) -> tuple[UserService, InMemoryUserStorage]:
    storage = InMemoryUserStorage(data=data, fault=fault, latency=latency)
    service = UserService(
        storage=storage,
        cache=QueryCache(
            ttl_seconds=ttl, empty_ttl_seconds=empty_ttl, time_func=clock
        ),
        breaker=CircuitBreaker(
            failure_threshold=threshold, recovery_seconds=recovery, time_func=clock
        ),
    )
    return service, storage


async def test_hit_does_not_requery() -> None:
    clock = FakeClock()
    service, storage = make_service(clock, data={"u1": {"id": "u1", "name": "Alice"}})

    first = await service.get_user("u1")
    second = await service.get_user("u1")

    assert first.source == "origin" and first.found
    assert second.source == "cache" and second.found
    assert storage.calls == 1, "命中时不得再访问底层存储"


async def test_expired_entry_reloads_and_never_returns_stale() -> None:
    clock = FakeClock()
    service, storage = make_service(clock, data={"u1": {"id": "u1", "name": "v1"}})

    assert (await service.get_user("u1")).data["name"] == "v1"
    storage._data["u1"] = {"id": "u1", "name": "v2"}

    clock.advance(4.0)  # 未过期：仍命中
    assert (await service.get_user("u1")).data["name"] == "v1"
    assert storage.calls == 1

    clock.advance(2.0)  # 超过 ttl=5：必须回源拿新值
    outcome = await service.get_user("u1")
    assert outcome.source == "origin"
    assert outcome.data["name"] == "v2", "过期后必须回源，不得返回旧值"
    assert storage.calls == 2


async def test_empty_result_cached_and_distinguishable() -> None:
    clock = FakeClock()
    service, storage = make_service(clock, data={})

    first = await service.get_user("ghost")
    assert first.source == "origin" and not first.found

    second = await service.get_user("ghost")
    assert second.source == "cache" and not second.found
    assert storage.calls == 1, "空结果要缓存，不能反复打底层"

    # 空结果缓存与"真的查到了"可区分
    assert second.data is None and second.found is False

    clock.advance(2.0)  # 超过 empty_ttl=1：空缓存过期，重新回源
    third = await service.get_user("ghost")
    assert third.source == "origin"
    assert storage.calls == 2


async def test_concurrent_load_single_flight() -> None:
    clock = FakeClock()
    service, storage = make_service(
        clock, data={"u1": {"id": "u1", "name": "Alice"}}, latency=0.05
    )

    results = await asyncio.gather(*(service.get_user("u1") for _ in range(10)))

    assert storage.calls == 1, "并发回源底层只应触发一次"
    assert all(r.found and r.data == results[0].data for r in results), \
        "等待方应拿到同一次回源结果"


async def test_load_failure_not_cached_and_reasons_distinguishable() -> None:
    clock = FakeClock()
    service, storage = make_service(clock, data={"u1": {"id": "u1"}}, threshold=100)

    storage.set_fault(FaultMode.TIMEOUT)
    out_timeout = await service.get_user("u1")
    assert out_timeout.source == "degraded" and "timeout" in out_timeout.reason

    storage.set_fault(FaultMode.UNAVAILABLE)
    out_unavail = await service.get_user("u1")
    assert out_unavail.source == "degraded" and "unavailable" in out_unavail.reason

    assert out_timeout.reason != out_unavail.reason, "失败原因必须可区分"
    assert storage.calls == 2, "失败结果不得写入缓存，每次都应尝试回源"

    storage.set_fault(FaultMode.OK)
    recovered = await service.get_user("u1")
    assert recovered.source == "origin" and recovered.found
    assert storage.calls == 3


async def test_circuit_breaker_opens_and_recovers() -> None:
    clock = FakeClock()
    service, storage = make_service(
        clock, data={"u1": {"id": "u1"}}, fault=FaultMode.UNAVAILABLE,
        threshold=2, recovery=1.0,
    )

    await service.get_user("u1")
    await service.get_user("u1")
    assert storage.calls == 2

    third = await service.get_user("u1")
    assert third.source == "degraded" and "circuit-open" in third.reason
    assert storage.calls == 2, "熔断期间不得再访问底层"

    storage.set_fault(FaultMode.OK)
    clock.advance(1.0)  # 超过 recovery：半开探测
    recovered = await service.get_user("u1")
    assert recovered.source == "origin" and recovered.found
    assert storage.calls == 3, "恢复后应能重新正常回源"

    again = await service.get_user("u1")
    assert again.source == "cache", "恢复后缓存应正常工作"


async def test_concurrent_load_failure_propagates_to_all_waiters() -> None:
    clock = FakeClock()
    service, storage = make_service(
        clock, data={"u1": {"id": "u1"}}, fault=FaultMode.TIMEOUT,
        latency=0.05, threshold=100,
    )

    results = await asyncio.gather(
        *(service.get_user("u1") for _ in range(5))
    )

    assert storage.calls == 1, "并发失败也只回源一次"
    assert all(r.source == "degraded" and "timeout" in r.reason for r in results), \
        "所有等待方应拿到同一次失败并各自降级"
