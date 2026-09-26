# 同步辐射真空阀联锁 · 因果一致性服务

多个控制台可**离线提交**联锁操作，恢复联网后任意顺序重放；服务端按**因果前沿 +
依赖向量**原子裁决，保证**到达顺序不改变应有的因果状态**。值班员可建立包含
**2–5 个控制台**和若干阀门的轮次。页面经真实 HTTP 接口提交事件并实时呈现
阀门状态、因果前沿、等待项与放行/拒绝原因。

## 语义模型

- **事件**：`event_id`（唯一标识）、`console`、`seq`（控制台本地序号）、
  `deps`（完整依赖向量：各控制台已消费序号）、`valve`、`expected_old`（期望旧状态）、
  `new_state`。
- **因果前沿**：每台控制台已消费的最大本地序号。仅当 `seq == 前沿+1` 且
  依赖向量全部被前沿覆盖时，事件才被**原子消费**（推进前沿 + 阀门转移 + 级联放行）。
- **消费即裁决**：期望旧状态匹配 → `applied`（放行并改阀）；不匹配 →
  `rejected_precondition`（拒绝但**照样推进因果位置**，后继永不阻塞）。
- **稳定裁决**：同批可放行项按 `event_id` 字典序依次消费——并发争用同一旧状态时
  较小标识成功，另一项稳定显示预条件拒绝。
- **幂等**：同一事件原样重投返回既有结论（等待中/已放行/已拒绝）；标识复用而
  载荷变化 → `409`。跳号、过期序号、未知控制台/阀门、未来依赖（依赖自身未来序号、
  未知控制台依赖、负依赖）→ `400`，且**不改任何阀门状态、不推进前沿**。

## 运行

```bash
# 启动服务（宿主端口可配置，默认 8080）
docker compose up --build app
APP_HOST_PORT=9090 docker compose up --build app

# 健康入口与页面
curl http://localhost:8080/healthz        # {"status":"ok"}
open http://localhost:8080/               # 控制台页面
```

## 验收（verify）

```bash
docker compose up --build --exit-code-from verify --abort-on-container-exit
echo $?   # 0 = 验收通过
```

`verify` 容器依次执行并以退出码报告结果：

1. **构建检查** `go build ./...` 与静态检查 `go vet ./...`；
2. **代码测试** `go test ./...`（引擎与 API 层单元测试）；
3. **因果场景 HTTP 冒烟**（`cmd/verify`）：
   - 依赖另一控制台的事件先到达 → 显示等待；补齐前驱 → 连续放行；
   - 两控制台争用同一旧状态 → 较小事件标识成功，另一项稳定预条件拒绝；
   - 竞争失败方的后继事件不被拒绝结果阻塞；
   - 同一事件重投返回既有结论；标识复用载荷变化/跳号/未知控制台/未来依赖
     均被明确拒绝且阀门状态不变；
   - 健康入口与业务接口响应可观察。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 健康入口 |
| GET | `/` | 控制台页面 |
| POST | `/api/rounds` | 建立轮次：`{"id"?, "consoles":[2..5], "valves":{"GV1":"closed"}}` → `201` |
| GET | `/api/rounds` / `/api/rounds/{id}` | 轮次列表 / 状态（阀门、前沿、等待项、裁决记录） |
| POST | `/api/rounds/{id}/events` | 提交事件 → `200`(applied/rejected) / `202`(waiting) / `400` / `404` / `409` |

事件提交示例：

```json
{
  "event_id": "acc-b-001",
  "console": "beta",
  "seq": 1,
  "deps": {"alpha": 1, "beta": 0, "gamma": 0},
  "valve": "GV1",
  "expected_old": "closed",
  "new_state": "open"
}
```

响应携带裁决结论与最新轮次快照（`round`），页面据此渲染。

## 结构

```
cmd/app        服务入口（LISTEN_ADDR，默认 :8080）
cmd/verify     验收冒烟程序（APP_URL，退出码报告结果）
internal/causality  因果引擎：前沿、依赖向量、原子消费、级联、稳定裁决、幂等
internal/server     HTTP API + 内嵌控制台页面
scripts/verify-entry.sh  verify 容器入口：构建检查 → 测试 → 冒烟
```
