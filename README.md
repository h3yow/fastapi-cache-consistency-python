# fastapi-cache-consistency-python

在读接口前加一层本地可复现的查询缓存，挡住重复的底层查询。

## 结构

- `cache_layer/cache.py` — 缓存核心：`QueryCache`（TTL + 单飞回源）、`CircuitBreaker`（熔断降级）、`CacheResult`（结果与来源）。
- `cache_layer/backend.py` — `FakeBackend`：可编程延迟/故障的底层存储模拟，用于本地复现超时、连接失败与恢复。
- `main.py` — FastAPI 读接口 `GET /items/{item_id}`，演示命中、回源与降级。
- `tests/test_cache.py` — 并发回源、过期、空结果、底层异常四类行为测试。

## 读写语义

`QueryCache.get(key)` 返回 `CacheResult(value, source, error)`，`source` 可区分四种情况：

| source | 含义 |
|---|---|
| `hit` | 缓存命中（含空结果命中），不访问底层 |
| `backend` | 本次请求真正触发了回源，并写入缓存 |
| `joined` | 并发等待方，复用同一次回源结果 |
| `degraded` | 底层不可用或熔断中，`error` 字段给出失败原因 |

- 命中时绝不访问底层存储；未命中时回源并写入。
- 同一键的并发请求合并为一次真实回源（单飞），等待方拿到同一次结果。
- 回源失败**不写入缓存**，失败原因放在 `error` 字段，异常不向调用方透出。

## 过期与空结果规则

- 每条缓存带 TTL（`ttl` 秒）。过期后必须重新回源，绝不返回旧值。
- 空结果用 `EMPTY` 哨兵写入缓存，与“没缓存”严格区分：空结果命中返回 `source=hit, value=None`，不会反复打底层。

## 降级与恢复

- 底层超时（`load_timeout`）、连接失败等异常一律转为 `degraded` 结果，对外接口返回 503 + 失败原因，不透出异常。
- `CircuitBreaker`：连续失败达到 `failure_threshold` 后断开，`cooldown` 秒内直接降级、不访问底层；冷却结束后半开放行一次试探，成功则闭合恢复，正常回源并重新写缓存。

## 本地验证

```bash
uv run pytest -q --log-cli-level=INFO   # 单测日志打印键、命中/回源、耗时、判定依据
uv run python main.py                   # 启动服务后访问 GET /items/1
```

接口返回的 `source` 字段可直接观察命中（`hit`）、回源（`backend`）与降级（503）。
