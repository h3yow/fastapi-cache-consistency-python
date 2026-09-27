"""缓存层行为测试：并发回源、过期、空结果、底层异常。

每条用例打印：键、命中/回源、耗时、判定依据。
"""
import asyncio
import logging
import time

import pytest

from cache_layer.backend import BackendUnavailableError, FakeBackend
from cache_layer.cache import CircuitBreaker, QueryCache, Source

logger = logging.getLogger("cache-test")


def report(key: str, result, elapsed: float, basis: str) -> None:
    logger.info(
        "key=%s source=%s value=%s error=%s elapsed=%.4fs 判定依据: %s",
        key, result.source.value, result.value, result.error, elapsed, basis,
    )


async def timed(cache: QueryCache, key: str):
    start = time.perf_counter()
    result = await cache.get(key)
    return result, time.perf_counter() - start


async def test_concurrent_singleflight():
    """并发回源：N 个请求同时打同一键，底层只触发一次，等待方复用同一次结果。"""
    backend = FakeBackend(latency=0.05)
    backend.seed("hot", ["v1"])
    cache = QueryCache(backend.fetch, ttl=10)

    start = time.perf_counter()
    results = await asyncio.gather(*[cache.get("hot") for _ in range(8)])
    elapsed = time.perf_counter() - start

    sources = [r.source for r in results]
    report("hot", results[0], elapsed,
           f"8 并发来源分布={ [s.value for s in sources] }, backend.calls={backend.calls}")
    assert sources.count(Source.BACKEND) == 1, "只允许一个请求真正回源"
    assert sources.count(Source.JOINED) == 7, "其余请求应复用同一次回源结果"
    assert backend.calls == 1, "底层只能被触发一次"
    assert all(r.value == ["v1"] for r in results), "等待方必须拿到同一次结果"


async def test_expiry_refetches():
    """过期：TTL 内命中不回源，过期后必须重新回源而不是返回旧值。"""
    backend = FakeBackend()
    backend.seed("k", ["old"])
    cache = QueryCache(backend.fetch, ttl=0.1)

    r1, t1 = await timed(cache, "k")
    report("k", r1, t1, "首次访问应为回源")
    assert r1.source is Source.BACKEND and backend.calls == 1

    r2, t2 = await timed(cache, "k")
    report("k", r2, t2, "TTL 内应命中，不再回源")
    assert r2.source is Source.HIT and backend.calls == 1

    backend.seed("k", ["new"])  # 底层数据已更新
    await asyncio.sleep(0.12)
    r3, t3 = await timed(cache, "k")
    report("k", r3, t3, "过期后必须重新回源拿到新值")
    assert r3.source is Source.BACKEND
    assert r3.value == ["new"], "过期后不得返回旧值"
    assert backend.calls == 2


async def test_empty_result_cached():
    """空结果：缓存空结果并与未命中区分，不反复打底层。"""
    backend = FakeBackend()
    cache = QueryCache(backend.fetch, ttl=10)

    r1, t1 = await timed(cache, "ghost")
    report("ghost", r1, t1, "空结果首次为回源，value=None")
    assert r1.source is Source.BACKEND and r1.value is None

    r2, t2 = await timed(cache, "ghost")
    report("ghost", r2, t2, "空结果应被缓存，第二次为命中而非再次回源")
    assert r2.source is Source.HIT, "空结果不得被当成未命中"
    assert r2.value is None
    assert backend.calls == 1, "空结果命中不得再访问底层"


async def test_backend_failure_degrades_and_recovers():
    """底层异常：失败不写缓存、异常不透出、熔断后快速降级、恢复后正常回源。"""
    backend = FakeBackend()
    backend.seed("k", ["v"])
    breaker = CircuitBreaker(failure_threshold=2, cooldown=0.1)
    cache = QueryCache(backend.fetch, ttl=10, breaker=breaker)

    backend.down = True
    r1, t1 = await timed(cache, "k")
    report("k", r1, t1, "底层宕机应降级，异常不透出、不写缓存")
    assert r1.source is Source.DEGRADED
    assert r1.error and "BackendUnavailableError" in r1.error, "失败原因要可区分"
    assert r1.value is None

    r2, _ = await timed(cache, "k")
    assert breaker.is_open, "连续失败达到阈值应熔断"
    calls_before = backend.calls
    r3, t3 = await timed(cache, "k")
    report("k", r3, t3, "熔断期间应直接降级，不访问底层")
    assert r3.source is Source.DEGRADED and "circuit open" in (r3.error or "")
    assert backend.calls == calls_before, "熔断期间不得访问底层"

    backend.down = False
    await asyncio.sleep(0.12)  # 等待冷却结束，半开试探
    r4, t4 = await timed(cache, "k")
    report("k", r4, t4, "恢复后应重新正常回源")
    assert r4.source is Source.BACKEND and r4.value == ["v"]
    assert not breaker.is_open, "恢复后熔断器应闭合"

    r5, _ = await timed(cache, "k")
    assert r5.source is Source.HIT, "恢复后写入的缓存应可命中"


async def test_failed_load_not_cached():
    """回源失败不得写入缓存：下次仍尝试回源而非命中一个错误结果。"""
    backend = FakeBackend(fail_times=1)
    backend.seed("k", ["v"])
    cache = QueryCache(backend.fetch, ttl=10,
                       breaker=CircuitBreaker(failure_threshold=99))

    r1, t1 = await timed(cache, "k")
    report("k", r1, t1, "首次回源失败应降级且不落缓存")
    assert r1.source is Source.DEGRADED

    r2, t2 = await timed(cache, "k")
    report("k", r2, t2, "失败未落缓存，第二次应重新回源成功")
    assert r2.source is Source.BACKEND and r2.value == ["v"]
    assert backend.calls == 2
