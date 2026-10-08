import logging

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from cache_layer.backend import FakeBackend
from cache_layer.cache import DegradeReason, QueryCache, Source

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

app = FastAPI()

backend = FakeBackend(latency=0.01)
backend.seed("items:1", ["apple", "banana"])
cache = QueryCache(
    backend.fetch,
    ttl=30.0,
    failure_threshold=3,
    cooldown=5.0,
    max_capacity=1000,
)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
async def read_item(item_id: str):
    """读接口：命中直接返回；未命中单飞回源；降级返回 503 + 固定类别，不带底层细节。"""
    key = f"items:{item_id}"
    result = await cache.get(key)
    if result.source is Source.DEGRADED:
        # 对外只暴露可枚举类别；具体异常类型/消息只在服务内部日志里
        logger.warning("degraded response key=%s reason=%s", key,
                       result.reason.value if result.reason else None)
        return JSONResponse(
            status_code=503,
            content={
                "detail": "backend unavailable, degraded",
                "reason": result.reason.value if result.reason else "unknown",
            },
        )
    return {
        "key": key,
        "value": result.value,  # None 表示已缓存的空结果
        "source": result.source.value,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
