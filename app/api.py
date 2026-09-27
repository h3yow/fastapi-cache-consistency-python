"""FastAPI 对外接口。

- GET /users/{user_id}：读接口，底层异常不透出，降级时返回 503 + 明确原因。
- GET /healthz：存活检查。
- POST /admin/fault {"mode": "ok|timeout|unavailable"}：注入底层故障，便于本地验证。
- POST /admin/cache/clear：清空缓存。
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.breaker import CircuitBreaker
from app.cache import QueryCache
from app.service import ReadOutcome, UserService
from app.storage import FaultMode, InMemoryUserStorage


class FaultBody(BaseModel):
    mode: FaultMode


def _default_service() -> UserService:
    storage = InMemoryUserStorage(
        data={"u1": {"id": "u1", "name": "Alice"}},
        latency=0.05,
    )
    return UserService(
        storage=storage,
        cache=QueryCache(ttl_seconds=5.0, empty_ttl_seconds=1.0),
        breaker=CircuitBreaker(failure_threshold=3, recovery_seconds=2.0),
    )


def create_app(service: UserService | None = None) -> FastAPI:
    """构造 FastAPI 应用。"""
    app = FastAPI(title="query-cache-demo")
    service = service or _default_service()
    app.state.service = service

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/users/{user_id}")
    async def get_user(user_id: str) -> dict[str, object]:
        outcome: ReadOutcome = await service.get_user(user_id)
        body = {
            "found": outcome.found,
            "source": outcome.source,
            "data": outcome.data,
            "reason": outcome.reason,
        }
        if outcome.source == "degraded":
            raise HTTPException(status_code=503, detail=body)
        return body

    @app.post("/admin/fault")
    async def set_fault(body: FaultBody) -> dict[str, str]:
        storage = service.storage
        if not isinstance(storage, InMemoryUserStorage):
            raise HTTPException(status_code=400, detail="storage does not support fault injection")
        storage.set_fault(body.mode)
        return {"mode": body.mode.value}

    @app.post("/admin/cache/clear")
    async def clear_cache() -> dict[str, str]:
        service.cache.clear()
        return {"status": "cleared"}

    return app


app = create_app()
