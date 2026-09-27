"""HTTP 接口测试：正常读取、空结果、降级不透出底层异常。"""

from __future__ import annotations

import httpx

from app.api import create_app
from app.breaker import CircuitBreaker
from app.cache import QueryCache
from app.service import UserService
from app.storage import FaultMode, InMemoryUserStorage


def make_client(storage: InMemoryUserStorage) -> httpx.AsyncClient:
    service = UserService(
        storage=storage,
        cache=QueryCache(ttl_seconds=5.0, empty_ttl_seconds=1.0),
        breaker=CircuitBreaker(failure_threshold=3, recovery_seconds=2.0),
    )
    transport = httpx.ASGITransport(app=create_app(service))
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def test_get_user_ok_then_cache_hit() -> None:
    storage = InMemoryUserStorage(data={"u1": {"id": "u1", "name": "Alice"}})
    async with make_client(storage) as client:
        r1 = await client.get("/users/u1")
        r2 = await client.get("/users/u1")
    assert r1.status_code == 200 and r1.json()["source"] == "origin"
    assert r2.status_code == 200 and r2.json()["source"] == "cache"
    assert storage.calls == 1


async def test_get_user_empty_result() -> None:
    storage = InMemoryUserStorage(data={})
    async with make_client(storage) as client:
        r1 = await client.get("/users/ghost")
        r2 = await client.get("/users/ghost")
    assert r1.status_code == 200
    assert r1.json()["found"] is False and r1.json()["source"] == "origin"
    assert r2.json()["found"] is False and r2.json()["source"] == "cache"
    assert storage.calls == 1


async def test_storage_failure_returns_503_without_leaking_exception() -> None:
    storage = InMemoryUserStorage(
        data={"u1": {"id": "u1"}}, fault=FaultMode.UNAVAILABLE
    )
    async with make_client(storage) as client:
        r = await client.get("/users/u1")
    assert r.status_code == 503
    detail = r.json()["detail"]
    assert detail["source"] == "degraded"
    assert "unavailable" in detail["reason"]
    assert "Traceback" not in str(detail), "底层异常不得透出到对外接口"
