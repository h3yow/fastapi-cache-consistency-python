"""缓存层行为测试。

覆盖场景：
- 并发回源单飞、TTL 过期重取、空结果缓存、失败不落缓存、异常后降级与恢复；
- 按键隔离：一个键熔断不影响其他键、多键同时失败/同时恢复各自独立；
- 容量上限与 LRU 淘汰：被淘汰键正常回源、空结果可重新缓存、淘汰窗口并发单飞；
- 降级类别：timeout / connectivity / circuit_open 可区分，且对外不泄露异常类型名与调用栈。

每条用例通过 report() 与 cache-layer 内部日志打印：键、来源、值、对外错误类别、
耗时以及判定依据；底层异常细节只出现在内部日志。
"""
import asyncio
import logging
import time

import pytest

from cache_layer.backend import FakeBackend
from cache_layer.cache import DegradeReason, QueryCache, Source

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
    cache = QueryCache(backend.fetch, ttl=10, failure_threshold=2, cooldown=0.1)

    backend.down = True
    r1, t1 = await timed(cache, "k")
    report("k", r1, t1, "底层宕机应降级，异常不透出、不写缓存")
    assert r1.source is Source.DEGRADED
    assert r1.reason is DegradeReason.CONNECTIVITY, "失败类别要可区分"
    assert r1.error == "connectivity", "对外只给固定类别，不带异常类型名"
    assert r1.value is None

    r2, _ = await timed(cache, "k")
    assert cache.is_circuit_open("k"), "连续失败达到阈值应熔断"
    calls_before = backend.calls
    r3, t3 = await timed(cache, "k")
    report("k", r3, t3, "熔断期间应直接降级，不访问底层")
    assert r3.source is Source.DEGRADED and r3.error == "circuit_open"
    assert r3.reason is DegradeReason.CIRCUIT_OPEN
    assert backend.calls == calls_before, "熔断期间不得访问底层"

    backend.down = False
    await asyncio.sleep(0.12)  # 等待冷却结束，半开试探
    r4, t4 = await timed(cache, "k")
    report("k", r4, t4, "恢复后应重新正常回源")
    assert r4.source is Source.BACKEND and r4.value == ["v"]
    assert not cache.is_circuit_open("k"), "恢复后熔断器应闭合"

    r5, _ = await timed(cache, "k")
    assert r5.source is Source.HIT, "恢复后写入的缓存应可命中"


async def test_failed_load_not_cached():
    """回源失败不得写入缓存：下次仍尝试回源而非命中一个错误结果。"""
    backend = FakeBackend(fail_times=1)
    backend.seed("k", ["v"])
    cache = QueryCache(backend.fetch, ttl=10,
                       failure_threshold=99)

    r1, t1 = await timed(cache, "k")
    report("k", r1, t1, "首次回源失败应降级且不落缓存")
    assert r1.source is Source.DEGRADED

    r2, t2 = await timed(cache, "k")
    report("k", r2, t2, "失败未落缓存，第二次应重新回源成功")
    assert r2.source is Source.BACKEND and r2.value == ["v"]
    assert backend.calls == 2


async def test_per_key_isolation_other_key_unaffected():
    """按键隔离：一个键熔断冷却时，另一个键照常命中/回源，互不阻挡。"""
    backend = FakeBackend()
    backend.seed("a", ["a1"])
    backend.seed("b", ["b1"])
    cache = QueryCache(backend.fetch, ttl=10, failure_threshold=1, cooldown=1.0)

    # a 键连续失败 1 次即熔断
    backend.down = True
    ra1, ta1 = await timed(cache, "a")
    report("a", ra1, ta1, "a 首次失败，记 connectivity 并进入熔断")
    assert ra1.source is Source.DEGRADED
    assert ra1.reason is DegradeReason.CONNECTIVITY
    assert cache.is_circuit_open("a")
    assert not cache.is_circuit_open("b"), "b 键从未失败，不应受 a 牵连"

    calls_before = backend.calls
    ra2, ta2 = await timed(cache, "a")
    report("a", ra2, ta2, "a 冷却期内应直接 circuit_open，不访问底层")
    assert ra2.source is Source.DEGRADED
    assert ra2.reason is DegradeReason.CIRCUIT_OPEN
    assert backend.calls == calls_before, "a 熔断中不得访问底层"

    backend.down = False
    rb1, tb1 = await timed(cache, "b")
    report("b", rb1, tb1, "a 在熔断，b 仍应正常回源")
    assert rb1.source is Source.BACKEND and rb1.value == ["b1"]

    rb2, tb2 = await timed(cache, "b")
    report("b", rb2, tb2, "b 二次访问应命中，不回源")
    assert rb2.source is Source.HIT and rb2.value == ["b1"]

    ra3, ta3 = await timed(cache, "a")
    report("a", ra3, ta3, "a 仍在冷却，b 的成功不得让 a 提前恢复")
    assert ra3.source is Source.DEGRADED
    assert ra3.reason is DegradeReason.CIRCUIT_OPEN
    assert cache.is_circuit_open("a"), "a 的熔断状态必须独立保留"

async def test_per_key_isolation_simultaneous_fail_and_recover():
    """多个键同时失败、冷却结束后各自独立半开试探，恢复判定互不串。"""
    data = {"a": ["a1"], "b": ["b1"]}
    state = {"failing": True, "b_still_down": False}

    async def loader(key):
        if state["failing"]:
            raise OSError(f"connection refused: {key}")
        if state["b_still_down"] and key == "b":
            raise OSError("connection refused: b")
        return list(data.get(key, []))

    cache = QueryCache(loader, ttl=10, failure_threshold=2, cooldown=0.1)

    # 同键失败必须串行累计（并发同键会被单飞合并）。
    # 第一轮：a、b 同时各失败一次（计数各 1）
    first = await asyncio.gather(cache.get("a"), cache.get("b"))
    for key, r in zip(("a", "b"), first):
        report(key, r, 0.0, "底层全挂第一轮：各键独立记一次 connectivity")
        assert r.source is Source.DEGRADED
        assert r.reason is DegradeReason.CONNECTIVITY
    assert not cache.is_circuit_open("a") and not cache.is_circuit_open("b")

    # 第二轮：a、b 同时再失败 -> 各自达到阈值，同一时间一起熔断
    second = await asyncio.gather(cache.get("a"), cache.get("b"))
    for key, r in zip(("a", "b"), second):
        report(key, r, 0.0, "底层全挂第二轮：各键独立达到阈值并熔断")
        assert r.source is Source.DEGRADED
        assert r.reason is DegradeReason.CONNECTIVITY
    assert cache.is_circuit_open("a") and cache.is_circuit_open("b"), "两键应各自独立熔断"

    # 冷却结束：a 恢复、b 继续失败（同时试探）
    state["failing"] = False
    state["b_still_down"] = True
    await asyncio.sleep(0.12)
    ra, rb = await asyncio.gather(cache.get("a"), cache.get("b"))
    report("a", ra, 0.0, "冷却后 a 半开试探成功 -> 独立恢复并写缓存")
    report("b", rb, 0.0, "冷却后 b 半开试探仍失败 -> 保持熔断，不影响 a")
    assert ra.source is Source.BACKEND and ra.value == ["a1"]
    assert not cache.is_circuit_open("a"), "a 试探成功应独立闭合"
    assert rb.source is Source.DEGRADED and rb.reason is DegradeReason.CONNECTIVITY
    assert cache.is_circuit_open("b"), "b 试探失败必须保持熔断"

    # b 随后也恢复：各自冷却，各自试探
    state["b_still_down"] = False
    await asyncio.sleep(0.12)
    rb2, _ = await timed(cache, "b")
    report("b", rb2, 0.0, "b 再次冷却后半开试探成功 -> 独立恢复")
    assert rb2.source is Source.BACKEND and rb2.value == ["b1"]
    assert not cache.is_circuit_open("b")


async def test_capacity_lru_eviction():
    """容量淘汰：装满后按 LRU 逐出最久未使用键，被淘汰键再次访问正常回源。"""
    backend = FakeBackend()
    backend.seed("a", ["a1"])
    backend.seed("b", ["b1"])
    backend.seed("c", ["c2"])
    cache = QueryCache(backend.fetch, ttl=10, max_capacity=2)

    for k in ("a", "b"):
        r, _ = await timed(cache, k)
        assert r.source is Source.BACKEND
    assert cache.size == 2 and cache.capacity == 2

    # 访问 a，使 b 成为最久未使用；再装入 c -> 应淘汰 b
    ra_hit, _ = await timed(cache, "a")
    report("a", ra_hit, 0.0, "访问 a 提升 LRU 顺序，b 成为最久未使用")
    assert ra_hit.source is Source.HIT

    backend.seed("b", ["b2"])  # 被淘汰键底层值已更新
    rc, tc = await timed(cache, "c")
    report("c", rc, tc, "容量已满，装入 c 时按 LRU 淘汰 b，size 不超过上限")
    assert rc.source is Source.BACKEND and rc.value == ["c2"]
    assert cache.size == 2

    rb, tb = await timed(cache, "b")
    report("b", rb, tb, "b 已被淘汰，再访问必须重新回源拿新值，不得返回旧值/报错")
    assert rb.source is Source.BACKEND, "淘汰不等于命中旧值，必须回源"
    assert rb.value == ["b2"], "被淘汰键不得返回过期旧值"

    # 装入 b 后现在最久未使用的是 a -> 淘汰 a
    ra2, _ = await timed(cache, "a")
    report("a", ra2, 0.0, "b 重新装入后 a 成为最久未使用，访问 a 应已被淘汰并回源")
    assert ra2.source is Source.BACKEND and ra2.value == ["a1"]


async def test_evicted_empty_result_recaches():
    """空结果被淘汰后再访问：和第一次一样重新回源并重新缓存空结果。"""
    backend = FakeBackend()  # 无 seed -> 所有键为空
    cache = QueryCache(backend.fetch, ttl=10, max_capacity=1)

    r1, _ = await timed(cache, "ghost")
    report("ghost", r1, 0.0, "空结果首次回源，value=None")
    assert r1.source is Source.BACKEND and r1.value is None

    r2, _ = await timed(cache, "other")  # 淘汰 ghost
    assert r2.source is Source.BACKEND and cache.size == 1

    r3, t3 = await timed(cache, "ghost")
    report("ghost", r3, t3, "空结果条目被淘汰，再次访问需重新回源")
    assert r3.source is Source.BACKEND and r3.value is None
    calls = backend.calls

    r4, t4 = await timed(cache, "ghost")
    report("ghost", r4, t4, "重新回源的空结果应再次被缓存，二次访问命中不回源")
    assert r4.source is Source.HIT and r4.value is None
    assert backend.calls == calls, "重新缓存的空结果命中不得再访问底层"


async def test_eviction_during_concurrent_singleflight():
    """淘汰相关并发：容量竞争淘汰正常；被淘汰键再次并发访问仍合并为一次回源。"""
    data = {"p": ["p1"], "q": ["q1"]}
    calls = {"p": 0, "q": 0}

    async def slow_loader(key):
        calls[key] += 1
        await asyncio.sleep(0.05 if key == "p" else 0.0)
        return list(data[key])

    cache = QueryCache(slow_loader, ttl=10, max_capacity=1)

    # 场景一：p 正在慢回源途中（inflight，尚无缓存条目），6 个并发必须单飞
    p_tasks = [asyncio.ensure_future(cache.get("p")) for _ in range(6)]
    await asyncio.sleep(0.02)  # 全部挂到同一个 inflight 上，p 此时还没写入
    presults = await asyncio.gather(*p_tasks)
    psources = [r.source for r in presults]
    report("p", presults[0], 0.0,
           f"慢回源途中 6 并发必须单飞，来源分布={[x.value for x in psources]} calls={calls['p']}")
    assert psources.count(Source.BACKEND) == 1
    assert psources.count(Source.JOINED) == 5
    assert calls["p"] == 1 and all(r.value == ["p1"] for r in presults)
    assert cache.size == 1

    # 场景二：q 写入淘汰 p；p 被淘汰后再次并发访问，回源仍只能发生一次
    rq, tq = await timed(cache, "q")
    report("q", rq, tq, "q 写入占满唯一容量位，p 被 LRU 淘汰")
    assert rq.source is Source.BACKEND and rq.value == ["q1"]
    assert cache.size == 1

    p2 = await asyncio.gather(*[cache.get("p") for _ in range(4)])
    p2sources = [r.source for r in p2]
    report("p", p2[0], 0.0,
           f"p 被淘汰后 4 并发仍合并为一次回源，来源分布={[x.value for x in p2sources]} calls={calls['p']}")
    assert p2sources.count(Source.BACKEND) == 1, "淘汰后的并发同键仍只能回源一次"
    assert p2sources.count(Source.JOINED) == 3
    assert calls["p"] == 2, "淘汰后重新回源一次，并发不重复打底层"
    assert all(r.value == ["p1"] for r in p2), "等待方拿到同一次结果"


async def test_degrade_reasons_distinguishable_and_no_leak():
    """降级类别可区分：timeout / connectivity / circuit_open 三类，对外不含底层细节。"""
    async def timeout_loader(key):
        await asyncio.sleep(0.2)  # 超过下面 0.05 的超时阈值
        return ["never"]

    tc = QueryCache(timeout_loader, ttl=10, load_timeout=0.05,
                    failure_threshold=99)
    rt, tt = await timed(tc, "slow")
    report("slow", rt, tt, "回源超过 load_timeout -> timeout，且不得透出 TimeoutError")
    assert rt.source is Source.DEGRADED
    assert rt.reason is DegradeReason.TIMEOUT
    assert rt.error == "timeout"

    async def conn_loader(key):
        raise ConnectionRefusedError("dial tcp 10.0.0.9:6379: connect: secret host refused")

    cc = QueryCache(conn_loader, ttl=10, failure_threshold=99)
    rc, tc2 = await timed(cc, "broken")
    report("broken", rc, tc2, "底层连接异常 -> connectivity，异常类型名/消息不得外泄")
    assert rc.source is Source.DEGRADED
    assert rc.reason is DegradeReason.CONNECTIVITY
    assert rc.error == "connectivity"

    # 熔断中 -> circuit_open
    oc = QueryCache(conn_loader, ttl=10, failure_threshold=1, cooldown=5.0)
    await oc.get("k")
    ro, to = await timed(oc, "k")
    report("k", ro, to, "熔断冷却期内 -> circuit_open，不访问底层")
    assert ro.source is Source.DEGRADED
    assert ro.reason is DegradeReason.CIRCUIT_OPEN
    assert ro.error == "circuit_open"

    # 防泄露：所有降级结果的对外字段都不允许出现异常类型名、敏感消息片段、调用栈标记
    leaked_terms = ["Error", "Exception", "Traceback", "dial tcp", "secret",
                    "connect:", "asyncio", "raise", "File \\"]
    for name, r in (("timeout", rt), ("connectivity", rc), ("circuit_open", ro)):
        exposed = " ".join(str(x) for x in (r.error, r.reason.value if r.reason else ""))
        for term in leaked_terms:
            assert term not in exposed, f"{name} 对外结果泄露内部细节: {term!r} in {exposed!r}"
