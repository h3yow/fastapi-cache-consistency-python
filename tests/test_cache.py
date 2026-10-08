"""缓存层行为测试。

覆盖场景：
1. 并发回源合并（单飞）          2. TTL 过期重新回源
3. 空结果缓存                    4. 底层异常降级与自动恢复
5. 失败不写缓存                  6. 按键隔离（失败/降级/恢复互不影响）
7. 容量上限与 LRU 淘汰           8. 淘汰后重新回源（含空结果）
9. 淘汰窗口内并发仍合并          10. 降级类别可区分且不泄露底层细节
11. 超时类别                     12. 多键同时失败/同时恢复不串状态

每条用例打印：键、来源、耗时与判定依据。
"""
import asyncio
import logging
import time

import pytest

from cache_layer.backend import BackendUnavailableError, FakeBackend
from cache_layer.cache import (
    DegradeReason,
    QueryCache,
    Source,
)

logger = logging.getLogger("cache-test")


def report(scene: str, key: str, result, elapsed: float, basis: str) -> None:
    logger.info(
        "[%s] key=%s source=%s value=%s reason=%s elapsed=%.4fs 判定依据: %s",
        scene, key, result.source.value, result.value,
        result.reason.value if result.reason else None, elapsed, basis,
    )


async def timed(cache: QueryCache, key: str):
    start = time.perf_counter()
    result = await cache.get(key)
    return result, time.perf_counter() - start


async def test_concurrent_singleflight():
    """场景1：N 个请求同时打同一键，底层只触发一次，等待方复用同一次结果。"""
    backend = FakeBackend(latency=0.05)
    backend.seed("hot", ["v1"])
    cache = QueryCache(backend.fetch, ttl=10)

    start = time.perf_counter()
    results = await asyncio.gather(*[cache.get("hot") for _ in range(8)])
    elapsed = time.perf_counter() - start

    sources = [r.source for r in results]
    report("并发合并", "hot", results[0], elapsed,
           f"8 并发来源分布={[s.value for s in sources]}, backend.calls={backend.calls}")
    assert sources.count(Source.BACKEND) == 1, "只允许一个请求真正回源"
    assert sources.count(Source.JOINED) == 7, "其余请求应复用同一次回源结果"
    assert backend.calls == 1, "底层只能被触发一次"
    assert all(r.value == ["v1"] for r in results), "等待方必须拿到同一次结果"


async def test_expiry_refetches():
    """场景2：TTL 内命中不回源，过期后必须重新回源而不是返回旧值。"""
    backend = FakeBackend()
    backend.seed("k", ["old"])
    cache = QueryCache(backend.fetch, ttl=0.1)

    r1, t1 = await timed(cache, "k")
    report("过期", "k", r1, t1, "首次访问应为回源")
    assert r1.source is Source.BACKEND and backend.calls == 1

    r2, t2 = await timed(cache, "k")
    report("过期", "k", r2, t2, "TTL 内应命中，不再回源")
    assert r2.source is Source.HIT and backend.calls == 1

    backend.seed("k", ["new"])  # 底层数据已更新
    await asyncio.sleep(0.12)
    r3, t3 = await timed(cache, "k")
    report("过期", "k", r3, t3, "过期后必须重新回源拿到新值")
    assert r3.source is Source.BACKEND
    assert r3.value == ["new"], "过期后不得返回旧值"
    assert backend.calls == 2


async def test_empty_result_cached():
    """场景3：缓存空结果并与未命中区分，不反复打底层。"""
    backend = FakeBackend()
    cache = QueryCache(backend.fetch, ttl=10)

    r1, t1 = await timed(cache, "ghost")
    report("空结果", "ghost", r1, t1, "空结果首次为回源，value=None")
    assert r1.source is Source.BACKEND and r1.value is None

    r2, t2 = await timed(cache, "ghost")
    report("空结果", "ghost", r2, t2, "空结果应被缓存，第二次为命中而非再次回源")
    assert r2.source is Source.HIT, "空结果不得被当成未命中"
    assert r2.value is None
    assert backend.calls == 1, "空结果命中不得再访问底层"


async def test_backend_failure_degrades_and_recovers():
    """场景4：失败不写缓存、异常不透出、该键熔断后快速降级、冷却后试探恢复。"""
    backend = FakeBackend()
    backend.seed("k", ["v"])
    cache = QueryCache(backend.fetch, ttl=10, failure_threshold=2, cooldown=0.1)

    backend.down = True
    r1, t1 = await timed(cache, "k")
    report("降级恢复", "k", r1, t1, "底层宕机应降级，对外只给固定类别 unavailable")
    assert r1.source is Source.DEGRADED
    assert r1.reason is DegradeReason.UNAVAILABLE, "失败类别要可区分"
    assert r1.value is None

    r2, _ = await timed(cache, "k")
    assert cache.is_circuit_open("k"), "该键连续失败达到阈值应熔断"
    calls_before = backend.calls
    r3, t3 = await timed(cache, "k")
    report("降级恢复", "k", r3, t3, "该键熔断期间应直接降级 circuit_open，不访问底层")
    assert r3.source is Source.DEGRADED
    assert r3.reason is DegradeReason.CIRCUIT_OPEN
    assert backend.calls == calls_before, "熔断期间不得访问底层"

    backend.down = False
    await asyncio.sleep(0.12)  # 等待冷却结束，半开试探
    r4, t4 = await timed(cache, "k")
    report("降级恢复", "k", r4, t4, "冷却结束后该键独立试探，恢复正常回源")
    assert r4.source is Source.BACKEND and r4.value == ["v"]
    assert not cache.is_circuit_open("k"), "试探成功后该键熔断器应闭合"

    r5, _ = await timed(cache, "k")
    assert r5.source is Source.HIT, "恢复后写入的缓存应可命中"


async def test_failed_load_not_cached():
    """场景5：回源失败不得写入缓存：下次仍尝试回源而非命中一个错误结果。"""
    backend = FakeBackend(fail_times=1)
    backend.seed("k", ["v"])
    cache = QueryCache(backend.fetch, ttl=10, failure_threshold=99)

    r1, t1 = await timed(cache, "k")
    report("失败不落缓存", "k", r1, t1, "首次回源失败应降级且不落缓存")
    assert r1.source is Source.DEGRADED

    r2, t2 = await timed(cache, "k")
    report("失败不落缓存", "k", r2, t2, "失败未落缓存，第二次应重新回源成功")
    assert r2.source is Source.BACKEND and r2.value == ["v"]
    assert backend.calls == 2


# ---------------- 本轮新增：按键隔离 ----------------

async def test_per_key_isolation_during_degrade():
    """场景6a：一个键失败熔断，其它键该命中命中、该回源回源，不被一起挡住。"""
    backend = FakeBackend()
    backend.seed("biz-A", ["a1"])
    backend.seed("biz-B", ["b1"])
    cache = QueryCache(backend.fetch, ttl=10, failure_threshold=2, cooldown=10)

    # 先让 biz-B 正常缓存
    rb, _ = await timed(cache, "biz-B")
    assert rb.source is Source.BACKEND and rb.value == ["b1"]

    # biz-A 连续失败直至熔断（FakeBackend 对所有键失败，模拟共享底层故障）
    backend.down = True
    await cache.get("biz-A")
    await cache.get("biz-A")
    assert cache.is_circuit_open("biz-A"), "biz-A 应进入熔断"
    a_calls = backend.calls

    # biz-B 是另一个键：缓存命中，不被 biz-A 的熔断挡住
    rb2, tb2 = await timed(cache, "biz-B")
    report("按键隔离", "biz-B", rb2, tb2,
           f"biz-A 熔断中，biz-B 仍应命中；backend.calls 未增加({backend.calls}=={a_calls})")
    assert rb2.source is Source.HIT and rb2.value == ["b1"]
    assert backend.calls == a_calls, "命中不应访问底层，更不应受别的键熔断影响"

    # 未缓存的另一个键 biz-C：走自己的回源路径，失败也只记自己的账
    rc, tc = await timed(cache, "biz-C")
    report("按键隔离", "biz-C", rc, tc, "biz-C 首次失败，只算自己的第 1 次失败，不继承 biz-A")
    assert rc.source is Source.DEGRADED and rc.reason is DegradeReason.UNAVAILABLE
    assert not cache.is_circuit_open("biz-C"), "biz-C 失败次数不够，不应被别的键连带熔断"
    assert cache.is_circuit_open("biz-A"), "biz-A 仍应保持熔断，状态互不串改"


async def test_per_key_recovery_is_independent():
    """场景6b：冷却过后每个键独立试探恢复；一个键没好不影响另一个键恢复。"""
    backend = FakeBackend()
    backend.seed("a", ["va"])
    backend.seed("b", ["vb"])
    cache = QueryCache(backend.fetch, ttl=10, failure_threshold=1, cooldown=0.1)

    backend.down = True
    ra0 = await cache.get("a")
    rb0 = await cache.get("b")
    assert ra0.reason is rb0.reason is DegradeReason.UNAVAILABLE
    assert cache.is_circuit_open("a") and cache.is_circuit_open("b")

    # 两键一起进入冷却；恢复后用“只对 a 成功、b 仍失败”的加载器验证互不影响
    async def selective_loader(key: str) -> list[str]:
        if key == "a":
            return await backend_healthy.fetch(key)
        raise BackendUnavailableError("b still down")

    backend_healthy = FakeBackend()
    backend_healthy.seed("a", ["va"])
    cache._loader = selective_loader  # type: ignore[attr-defined]

    await asyncio.sleep(0.12)
    ra, ta = await timed(cache, "a")
    report("独立恢复", "a", ra, ta, "冷却结束 a 试探成功，应闭合恢复并写缓存")
    assert ra.source is Source.BACKEND and ra.value == ["va"]
    assert not cache.is_circuit_open("a")

    rb, tb = await timed(cache, "b")
    report("独立恢复", "b", rb, tb, "b 试探仍失败应继续熔断，不影响已恢复的 a")
    assert rb.source is Source.DEGRADED
    assert cache.is_circuit_open("b"), "b 没好就继续封自己的"
    assert not cache.is_circuit_open("a"), "a 已恢复，不得被 b 重新拖垮"

    ra2, _ = await timed(cache, "a")
    assert ra2.source is Source.HIT, "a 恢复后写入的缓存应可命中"


async def test_multiple_keys_fail_and_recover_together():
    """场景12：多键同一时间一起失败、一起恢复，各自熔断/恢复判定必须对得上、不串。"""
    backend = FakeBackend()
    for k in ("x", "y", "z"):
        backend.seed(k, [f"v-{k}"])
    cache = QueryCache(backend.fetch, ttl=10, failure_threshold=2, cooldown=0.1)

    backend.down = True
    round1 = await asyncio.gather(*(cache.get(k) for k in ("x", "y", "z")))
    round2 = await asyncio.gather(*(cache.get(k) for k in ("x", "y", "z")))
    for k, r1, r2 in zip(("x", "y", "z"), round1, round2):
        assert r1.reason is DegradeReason.UNAVAILABLE
        assert r2.reason in (DegradeReason.UNAVAILABLE, DegradeReason.CIRCUIT_OPEN)
        assert cache.is_circuit_open(k), f"{k} 两次失败后应独立熔断"
    logger.info("[多键齐失败] x/y/z 各自 failures=2、均独立熔断，判定依据=每键独立计数器")

    backend.down = False
    await asyncio.sleep(0.12)
    recovered = await asyncio.gather(*(cache.get(k) for k in ("x", "y", "z")))
    for k, r in zip(("x", "y", "z"), recovered):
        report("多键齐恢复", k, r, 0.0, "冷却同时结束，各自半开试探成功，回源拿各自的值")
        assert r.source is Source.BACKEND and r.value == [f"v-{k}"]
        assert not cache.is_circuit_open(k)


# ---------------- 本轮新增：容量与淘汰 ----------------

async def test_lru_eviction_on_capacity():
    """场景7：超过最大容量时按 LRU 淘汰最久未用的键，缓存条目数不超上限。"""
    backend = FakeBackend()
    for i in range(4):
        backend.seed(f"k{i}", [f"v{i}"])
    cache = QueryCache(backend.fetch, ttl=100, max_capacity=2)

    for i in range(3):
        r, _ = await timed(cache, f"k{i}")
        assert r.source is Source.BACKEND
    logger.info("[LRU] 判定依据: 容量2写入k2后，最久未用的 k0 应被淘汰；当前 size=%d", cache.size)
    assert cache.size == 2, "容量上限必须被严格遵守"
    assert "k0" not in cache._store  # type: ignore[attr-defined]

    # 访问 k1 刷新其 LRU 位置，再写入 k3，应淘汰 k2 而非 k1
    r1, _ = await timed(cache, "k1")
    assert r1.source is Source.HIT
    r3, _ = await timed(cache, "k3")
    assert r3.source is Source.BACKEND
    r1b, t = await timed(cache, "k1")
    report("LRU", "k1", r1b, t, "k1 刚被访问刷新过位置，不应被淘汰，应命中")
    assert r1b.source is Source.HIT and r1b.value == ["v1"]
    r2, t = await timed(cache, "k2")
    report("LRU", "k2", r2, t, "k2 才是此刻最久未用，淘汰后再访问应回源拿新值")
    assert r2.source is Source.BACKEND and r2.value == ["v2"]


async def test_evicted_key_refetches_fresh_value():
    """场景8a：被淘汰的键下次访问正常回源，拿到的是底层最新值，不是旧值。"""
    backend = FakeBackend()
    backend.seed("k", ["old"])
    cache = QueryCache(backend.fetch, ttl=100, max_capacity=1)

    r1, _ = await timed(cache, "k")
    assert r1.value == ["old"]
    # 写入另一个键把 k 挤出容量
    backend.seed("other", ["o"])
    ro, _ = await timed(cache, "other")
    assert ro.source is Source.BACKEND and cache.size == 1

    backend.seed("k", ["fresh"])
    r2, t = await timed(cache, "k")
    report("淘汰回源", "k", r2, t, "k 已被淘汰，再访问必须回源，拿到底层最新值而非旧值")
    assert r2.source is Source.BACKEND and r2.value == ["fresh"]


async def test_evicted_empty_result_can_recache():
    """场景8b：空结果被淘汰后再访问，和第一次一样重新回源并重新缓存为空结果。"""
    backend = FakeBackend()
    cache = QueryCache(backend.fetch, ttl=100, max_capacity=1)

    r1, _ = await timed(cache, "ghost")
    assert r1.source is Source.BACKEND and r1.value is None
    assert cache.size == 1, "空结果也占容量条目"

    backend.seed("filler", ["x"])
    await cache.get("filler")  # 挤掉 ghost
    assert "ghost" not in cache._store  # type: ignore[attr-defined]
    calls_before = backend.calls

    r2, t = await timed(cache, "ghost")
    report("空结果淘汰", "ghost", r2, t,
           f"空结果被淘汰后再访问应重新回源(calls {calls_before}->{backend.calls})，并重新缓存")
    assert r2.source is Source.BACKEND and r2.value is None
    r3, t3 = await timed(cache, "ghost")
    report("空结果淘汰", "ghost", r3, t3, "重新缓存后再次访问应命中，不再回源")
    assert r3.source is Source.HIT and r3.value is None


async def test_eviction_during_inflight_keeps_merge():
    """场景9：同键回源在途时，其它写入触发淘汰，等待方仍合并到同一次结果。"""
    backend = FakeBackend(latency=0.05)
    backend.seed("hot", ["v-hot"])
    backend.seed("c", ["v-c"])
    cache = QueryCache(backend.fetch, ttl=100, max_capacity=1)

    async def driver():
        # 先占住唯一容量的是 c；随后发起 hot 的在途回源，
        # hot 回源完成写入会淘汰 c，但 hot 的等待方绝不能被冲散
        rc = await cache.get("c")
        assert rc.source is Source.BACKEND
        hot_results = await asyncio.gather(*[cache.get("hot") for _ in range(6)])
        return hot_results

    start = time.perf_counter()
    results = await driver()
    elapsed = time.perf_counter() - start
    sources = [r.source for r in results]
    report("淘汰期并发", "hot", results[0], elapsed,
           f"6 并发来源分布={[s.value for s in sources]}, backend.calls={backend.calls}")
    assert sources.count(Source.BACKEND) == 1, "淘汰只动缓存，不能触发第二次回源"
    assert sources.count(Source.JOINED) == 5, "等待方必须仍然合并到同一次结果"
    assert all(r.value == ["v-hot"] for r in results)


# ---------------- 本轮新增：降级类别与信息边界 ----------------

async def test_degrade_reasons_are_distinct_and_safe(caplog):
    """场景10：unavailable / circuit_open / timeout 三种类别可区分；
    对外结果不带底层异常类型名，具体类型只出现在内部日志。"""
    backend = FakeBackend()
    cache = QueryCache(backend.fetch, ttl=10, failure_threshold=1, cooldown=10)

    # 1) 连接类失败：对外只给固定枚举值
    backend.down = True
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="cache_layer"):
        r1, t1 = await timed(cache, "k")
    report("降级类别", "k", r1, t1, "连接失败 -> unavailable，响应不得含异常类型名")
    assert r1.reason is DegradeReason.UNAVAILABLE
    serialized = repr(r1)
    assert "BackendUnavailableError" not in serialized, "对外结果不得带出底层异常类型名"
    assert "Traceback" not in serialized
    internal = " ".join(rec.getMessage() for rec in caplog.records)
    assert "BackendUnavailableError" in internal, "具体异常类型名应只出现在内部日志"

    # 2) 熔断期：类别切换为 circuit_open，且不访问底层
    calls_before = backend.calls
    r2, t2 = await timed(cache, "k")
    report("降级类别", "k", r2, t2, "冷却中被挡 -> circuit_open，可与超时/连不上区分")
    assert r2.reason is DegradeReason.CIRCUIT_OPEN
    assert backend.calls == calls_before

    # 3) 所有降级结果都只能是三类枚举之一，且 reason 字段类型固定
    for r in (r1, r2):
        assert isinstance(r.reason, DegradeReason)
        assert r.reason in set(DegradeReason)


async def test_timeout_reason():
    """场景11：回源超时单独归类为 timeout，与“连不上”区分开。"""
    async def slow_loader(key: str) -> list[str]:
        await asyncio.sleep(0.2)
        return ["never"]

    cache = QueryCache(slow_loader, ttl=10, load_timeout=0.03,
                       failure_threshold=99)
    r, elapsed = await timed(cache, "slow")
    report("超时", "slow", r, elapsed, "回源超过 load_timeout -> timeout，不透出 asyncio 异常")
    assert r.source is Source.DEGRADED
    assert r.reason is DegradeReason.TIMEOUT
    assert "TimeoutError" not in repr(r), "超时响应也不得带底层异常类型名"
