import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from cache_layer.backend import FakeBackend
from cache_layer.cache import CircuitBreaker, QueryCache, Source

app = FastAPI()

backend = FakeBackend(latency=0.01)
backend.seed("items:1", ["apple", "banana"])
cache = QueryCache(backend.fetch, ttl=30.0, breaker=CircuitBreaker())


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
async def read_item(item_id: str):
    """读接口：命中直接返回；未命中单飞回源；底层异常降级为 503，不透出异常。"""
    result = await cache.get(f"items:{item_id}")
    if result.source is Source.DEGRADED:
        return JSONResponse(
            status_code=503,
            content={"detail": "backend unavailable, degraded", "reason": result.error},
        )
    return {
        "key": f"items:{item_id}",
        "value": result.value,  # None 表示已缓存的空结果
        "source": result.source.value,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
