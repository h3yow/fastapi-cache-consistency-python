# 本地查询缓存层（FastAPI 示例）

在 FastAPI 读接口前面加的一层**本地内存查询缓存**，用于挡住对底层存储的重复查询。
代码结构：

| 文件 | 职责 |
| --- | --- |
| `app/cache.py` | 缓存核心：TTL 过期、空结果缓存、同键并发回源合并（single-flight）、判定日志 |
| `app/breaker.py` | 按键熔断器：连续失败打开，冷却后半开探测，成功恢复 |
| `app/service.py` | 组合层：缓存 + 熔断 + 存储，异常在此降级为可区分的结果 |
| `app/storage.py` | 底层存储协议 + 可注入故障（超时/不可用）的内存实现 |
| `app/api.py` | FastAPI 路由：读接口、健康检查、故障注入与清缓存的管理端点 |
| `app/errors.py` | 异常类型：`StorageTimeoutError` / `StorageUnavailableError` / `CircuitOpenError` |

## 缓存读写语义

- **命中**：缓存存在且未过期时直接返回，**不会调用底层 loader**。
- **未命中**：无缓存条目时执行一次回源，成功后写入缓存再返回。
- **并发同键**：同一时刻同 key 只有首个请求真正回源；其余请求 await 同一个
  Future（日志中为 `join-inflight`），拿到的是**同一次回源的同一份结果**，
  底层只被触发一次。回源失败时，所有等待方拿到同一个失败并各自降级。
- **回源失败不写缓存**：loader 抛出任何异常都不会污染缓存，下一次请求会重新回源。

## 过期规则

- 普通结果 TTL = `ttl_seconds`（默认 5s），空结果 TTL = `empty_ttl_seconds`（默认 1s）。
- **过期即回源**：过期条目不会作为旧值返回（不做 stale-while-revalidate），
  判定依据日志记为 `basis=entry expired`。

## 空结果规则

- 空结果（默认判定为 `None`，可通过 `QueryCache(is_empty=...)` 自定义）也会写入缓存，
  与“没有缓存”严格区分：
  - 首次查不到的用户：`found=false, source=origin`
  - 空缓存 TTL 内再次查询：`found=false, source=cache`，底层零调用。
  - 空缓存过期后：重新回源，避免长期缓存“不存在”这一状态。

## 降级与恢复行为

| 场景 | 行为 | `reason` | HTTP |
| --- | --- | --- | --- |
| 回源超时 | 记录一次熔断失败，返回降级结果 | `timeout while loading from storage` | 503 |
| 连接不上 | 记录一次熔断失败，返回降级结果 | `storage unavailable` | 503 |
| 熔断打开 | 不访问底层，快速失败 | `circuit-open (retry_after=…s)` | 503 |
| 冷却后探测 | 打开满 `recovery_seconds`（默认 2s）放行一次半开探测 | 成功则复位为 closed | 200 |
| 探测失败 | 重新打开熔断 | 同上 | 503 |

- 底层异常**不会透出**到对外接口：响应体只有 `found/source/data/reason`，
  没有堆栈。
- 熔断状态按 key 独立维护，恢复后该 key 立即恢复正常回源与缓存。

## 响应示例

```json
// GET /users/u1 正常回源
{"found": true, "source": "origin", "data": {"id": "u1", "name": "Alice"}, "reason": "origin loaded"}
// 命中
{"found": true, "source": "cache", "data": {"id": "u1", "name": "Alice"}, "reason": "fresh entry"}
// 空结果（查到了“不存在”）
{"found": false, "source": "origin", "data": null, "reason": "origin loaded (empty)"}
// 降级
HTTP 503 {"detail": {"found": false, "source": "degraded", "data": null, "reason": "storage unavailable"}}
```

## 本地验证

### 1. 运行测试

```bash
uv sync
uv run pytest
```

测试覆盖：并发回源只触发一次、缓存过期重新回源、空结果缓存与未命中可区分、
回源失败不写缓存且超时/不可用/熔断原因可区分、熔断打开与半开恢复、
HTTP 层 503 不透出异常。日志会打印 `key`、`decision`（hit/load/join-inflight/…）、
`elapsed_ms` 和 `basis`（判定依据）。

### 2. 手工端到端

```bash
uv run python main.py
# 另开终端：
curl -s localhost:8000/users/u1          # 回源 source=origin
curl -s localhost:8000/users/u1          # 命中 source=cache，底层不再被调用
curl -s localhost:8000/users/missing     # 空结果 source=origin
curl -s localhost:8000/users/missing     # 空结果命中 source=cache，底层零调用

curl -s -X POST localhost:8000/admin/fault -H 'content-type: application/json' -d '{"mode":"unavailable"}'
curl -s -X POST localhost:8000/admin/cache/clear   # 清缓存，确保下一次走到回源
curl -s -i localhost:8000/users/u1       # 503 降级，reason=storage unavailable（连续 3 次后熔断）
curl -s -X POST localhost:8000/admin/fault -H 'content-type: application/json' -d '{"mode":"ok"}'
# 等待 recovery_seconds(2s) 后
curl -s localhost:8000/users/u1          # 半开探测成功，恢复正常
```
