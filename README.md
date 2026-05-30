# Dify Game AI Gateway / Dify 游戏 AI 网关

## 中文说明

Dify 游戏 AI 网关是位于游戏客户端和 Dify 之间的 Go 服务层。它用于把 Dify App API Key 保留在服务端，将游戏请求转换为 Dify API 调用，把模型输出流式返回给游戏客户端，并为鉴权、限流、内容审核、可观测性和会话管理提供基础。

本仓库仍处于里程碑开发阶段。当前已实现的范围包括项目骨架、配置加载、可观测性基座、Dify client 包、Protobuf 协议与分帧编解码、TCP 接入层、JWT 鉴权、Redis 会话存储、上下文装配、限流/配额/熔断、内容审核、对话主链路编排，以及中止与会话重置对接。

### 功能特性

- 基于环境变量的配置加载，缺少必填项时 fail-fast。
- JSON 日志 helper 和 Prometheus `/metrics` 注册。
- Dify 阻塞式对话接口：`POST /chat-messages`。
- Dify SSE 流式解析，支持 `message`、`message_end`、`error`、`message_replace` 和 `ping`。
- 首个流式事件会回调 `task_id` 和 `conversation_id`，便于调用方在流活跃期间执行 Stop。
- Dify Stop 接口封装：`POST /chat-messages/{task_id}/stop`。
- Dify 文件上传接口封装：`POST /files/upload`。
- Dify 应用参数接口封装：`GET /parameters`。
- 本地 `inputs` 变量契约检查。
- 上游 HTTP 错误映射与重试：在尚未发出任何流式 delta 前，对 429/5xx 做指数退避重试。
- Protobuf 客户端协议与 4 字节长度前缀分帧编解码（`api/proto`、`internal/codec`）。
- TCP 接入层：每连接一 goroutine、心跳、连接级写串行化、按 `request_id` 多路复用（`internal/listener`、`internal/session`、`internal/mux`）。
- JWT session token 验签与 player 绑定，算法锁定到公钥族，拒绝 `alg=none` 与 HMAC alg-confusion（`internal/auth`）。
- Redis 会话存储：`conv:{player}:{npc}` 映射的增删查与 TTL，以及首次会话创建的分布式锁（`internal/store`）。
- 上下文装配：从可信 provider 组装 Dify `inputs`，仅允许变量契约内的键，来源失败时降级为部分上下文（`internal/context`）。
- 限流/配额/熔断：Redis 滑动窗口单玩家限流、每日 token 预算、全局在途上游信号量，以及熔断状态指标（`internal/limiter`）。
- 内容审核：输入审核与流式输出句级缓冲审核，支持跨 chunk 敏感内容阻断（`internal/moderation`）。
- 对话主链路编排：串起鉴权、限流、会话映射、上下文装配、输入审核、Dify 流式、输出审核、客户端回写和 token 记账（`internal/pipeline`）。
- 中止与会话重置：`StopRequest` 用缓存的 task_id 调 Dify Stop 接口并关流（不计为上游失败）；`ResetRequest` 清除会话映射，使下条消息开启新会话（`internal/pipeline`、`internal/listener`）。

尚未实现：完整进程组装（`cmd/gateway` 主程序）与 M5 联调、压测、安全加固和部署文档。

### 目录结构

```text
cmd/gateway/          网关入口
api/proto/            Protobuf 协议与生成的 Go 代码
internal/config/      环境变量加载与校验
internal/dify/        Dify REST 与 SSE client
internal/codec/       长度前缀分帧与信封编解码
internal/listener/    TCP 接入层与连接生命周期
internal/session/     连接会话状态与 player 绑定
internal/mux/         单连接写串行化与请求多路复用
internal/auth/        JWT session token 验签
internal/store/       Redis 会话映射与会话创建锁
internal/limiter/     限流、配额与熔断
internal/context/     玩家上下文拉取与 inputs 装配
internal/moderation/  输入与流式输出审核
internal/pipeline/    对话主链路编排
internal/difymock/    可编排 SSE 序列的 Dify mock（集成测试用）
internal/loadtest/    并发压测与泄漏检测（M5-T2）
internal/telemetry/   指标、JSON 日志和脱敏 helper
deploy/               部署工作区
test/                 验证脚本和后续集成测试
```

详细产品和实现计划见 [game-gateway-PDR.md](./game-gateway-PDR.md)。

### 环境要求

- Go 1.23 或更高版本
- Dify App API endpoint 和 app API key
- Redis 地址用于会话和限流存储；JWT 公钥用于 session token 鉴权

### 配置项

网关通过环境变量读取配置。

| 变量 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `GATEWAY_ADDR` | 否 | `:9000` | 后续 TCP 网关监听地址 |
| `GATEWAY_ADMIN_ADDR` | 否 | `:9001` | 后续管理 HTTP 地址，用于 metrics 和健康检查 |
| `DIFY_BASE_URL` | 是 | | Dify API base URL，例如 `http://dify-api/v1` |
| `DIFY_APP_KEYS` | 是 | | App key 映射，例如 `default=app-xxx;npc-blacksmith=app-yyy` |
| `REDIS_ADDR` | 是 | | 会话/限流存储使用的 Redis 地址 |
| `AUTH_JWT_PUBKEY` | 是 | | JWT 公钥 PEM；转义的 `\n` 会被规范化 |
| `UPSTREAM_TIMEOUT_SEC` | 否 | `60` | 规划中的上游调用超时 |
| `RATE_PER_PLAYER` | 否 | `1r/2s` | 单玩家限流 |
| `TOKEN_BUDGET_DAILY` | 否 | `100000` | 单玩家每日 token 预算 |
| `MAX_INFLIGHT_UPSTREAM` | 否 | `200` | 全局上游并发上限 |
| `MODERATION_ENABLED` | 否 | `true` | 内容审核开关 |
| `GATEWAY_SERVICE_NAME` | 否 | `game-ai-gateway` | OpenTelemetry service name |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 否 | | 可选 OTLP endpoint |

本地环境示例：

```powershell
$env:DIFY_BASE_URL = "http://localhost/v1"
$env:DIFY_APP_KEYS = "default=app-your-key"
$env:REDIS_ADDR = "localhost:6379"
$env:AUTH_JWT_PUBKEY = "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----"
```

### 构建与测试

```powershell
go test ./...
go build ./...
```

也可以使用 Makefile：

```powershell
make test
make build
make lint
```

`make proto` 使用 `protoc` 与 `protoc-gen-go` 从 `api/proto/gateway.proto` 生成 Go 代码；缺少其中任一工具时会给出明确提示并失败。

### 运行

当前 `cmd/gateway` 仍是入口骨架，只会打印启动日志：

```powershell
go run ./cmd/gateway
```

当前可直接在 Go 代码中使用 Dify client 包：

```go
client := dify.NewClient("http://dify-api/v1", "app-key", nil)

result, err := client.Chat(ctx, dify.ChatReq{
	Query:  "What gear do you sell?",
	Inputs: map[string]string{"player_level": "12"},
	User:   "player-10086:npc-blacksmith",
})
```

流式调用：

```go
result, err := client.ChatStream(
	ctx,
	dify.ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"},
	func(taskID, convID string) {
		// 在流活跃期间缓存 taskID，以便调用 Stop。
	},
	func(delta string) {
		// 将 delta 转发给游戏客户端。
	},
)
```

### Docker

构建镜像：

```powershell
docker build -t dify-game-ai-gateway .
```

带环境变量运行：

```powershell
docker run --rm `
  -e DIFY_BASE_URL=http://dify-api/v1 `
  -e DIFY_APP_KEYS=default=app-your-key `
  -e REDIS_ADDR=redis:6379 `
  -e AUTH_JWT_PUBKEY="-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----" `
  -p 9000:9000 `
  -p 9001:9001 `
  dify-game-ai-gateway
```

### 开发状态

已完成的里程碑范围：

- M0-T1 项目骨架
- M0-T2 配置加载
- M0-T3 可观测性基座
- M1-T1 Dify 阻塞式对话
- M1-T2 Dify SSE 流式解析
- M1-T3 Stop、文件上传和参数接口
- M1-T4 上游错误映射与重试
- M2-T1 Protobuf 协议与分帧编解码
- M2-T2 TCP 接入层（listener/session/mux）
- M2-T3 JWT 鉴权
- M3-T1 Redis 会话存储与会话创建锁
- M3-T2 上下文装配
- M3-T3 限流/配额/熔断
- M3-T4 内容审核
- M4-T1 对话主链路编排
- M4-T2 中止与会话管理对接
- M5-T1 Mock Dify 与集成测试
- M5-T2 压测与稳定性

下一个计划里程碑：

- M5-T3 安全加固审查

### 安全注意事项

- Dify App API Key 必须只保存在网关服务端，不能发送到游戏客户端。
- 日志应避免记录原始玩家内容和密钥。新增日志时应使用 telemetry 脱敏 helper。
- 玩家输入应只进入 `query`；会影响系统行为的控制类 `inputs` 只能来自网关侧可信上下文。

---

## English

Dify Game AI Gateway is a Go service layer between a game client and Dify. It keeps Dify App API keys on the server side, translates game requests into Dify API calls, streams model output back to the game client, and provides the foundation for auth, rate limiting, moderation, observability, and session management.

This repository is under active milestone development. The implemented surface currently covers the project skeleton, configuration, telemetry, the Dify client package, the Protobuf protocol and frame codec, the TCP access layer, JWT authentication, the Redis session store, context assembly, rate limiting / quota / circuit breaking, moderation, and main chat pipeline orchestration.

### Features

- Environment-based configuration with fail-fast validation.
- JSON logging helpers and Prometheus `/metrics` registration.
- Dify blocking chat client for `POST /chat-messages`.
- Dify streaming SSE parser with `message`, `message_end`, `error`, `message_replace`, and ping handling.
- Early stream event callback for `task_id` and `conversation_id`, so callers can support Stop while a stream is active.
- Dify Stop API wrapper: `POST /chat-messages/{task_id}/stop`.
- Dify file upload wrapper: `POST /files/upload`.
- Dify app parameter wrapper: `GET /parameters`.
- Parameter contract checking for local `inputs` variables.
- Upstream HTTP error mapping with exponential-backoff retry for 429/5xx before any stream delta is emitted.
- Protobuf client protocol with 4-byte length-prefixed frame codec (`api/proto`, `internal/codec`).
- TCP access layer: one goroutine per connection, heartbeat, connection-level write serialization, and `request_id` multiplexing (`internal/listener`, `internal/session`, `internal/mux`).
- JWT session-token verification and player binding, with algorithms pinned to the key family and `alg=none`/HMAC alg-confusion rejected (`internal/auth`).
- Redis session store: `conv:{player}:{npc}` mapping CRUD with TTL plus the distributed lock for first-time conversation creation (`internal/store`).
- Context assembly from trusted providers into Dify `inputs`, admitting only contract keys and degrading to partial context when a source is unavailable (`internal/context`).
- Rate limiting, quota, and circuit breaking: Redis sliding-window per-player limits, daily token budgets, a global upstream in-flight semaphore, and circuit state metrics (`internal/limiter`).
- Moderation for input and sentence-buffered streaming output, including detection of blocked content split across chunks (`internal/moderation`).
- Main chat pipeline orchestration across auth, limiter, session mapping, context assembly, input moderation, Dify streaming, output moderation, client writes, and token accounting (`internal/pipeline`).
- Stop and conversation reset: `StopRequest` aborts the upstream generation via the cached task_id and closes the stream (not counted as an upstream failure); `ResetRequest` clears the conversation mapping so the next message starts fresh (`internal/pipeline`, `internal/listener`).

Not yet implemented: full process assembly (the `cmd/gateway` entrypoint) plus M5 integration tests, load testing, security hardening, and deployment docs.

### Repository Layout

```text
cmd/gateway/          Gateway entrypoint
api/proto/            Protobuf protocol and generated Go code
internal/config/      Environment loading and validation
internal/dify/        Dify REST and SSE client
internal/codec/       Length-prefixed frame and envelope codec
internal/listener/    TCP access layer and connection lifecycle
internal/session/     Connection session state and player binding
internal/mux/         Single-connection write serialization and request multiplexing
internal/auth/        JWT session-token verification
internal/store/       Redis conversation mapping and creation lock
internal/limiter/     Rate limiting, quota, and circuit breaking
internal/context/     Player context fetching and inputs assembly
internal/moderation/  Input and streaming-output moderation
internal/pipeline/    Main chat pipeline orchestration
internal/difymock/    Scriptable SSE Dify mock (integration tests)
internal/loadtest/    Concurrent load test and leak detection (M5-T2)
internal/telemetry/   Metrics, JSON logging, and redaction helpers
deploy/               Deployment workspace
test/                 Verification scripts and future integration tests
```

The detailed product and implementation plan lives in [game-gateway-PDR.md](./game-gateway-PDR.md).

### Requirements

- Go 1.23 or newer
- Dify App API endpoint and app API key
- Redis is used for session and rate-limit storage; the JWT public key is used for session-token authentication

### Configuration

The gateway reads configuration from environment variables.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `GATEWAY_ADDR` | No | `:9000` | Future TCP gateway listen address |
| `GATEWAY_ADMIN_ADDR` | No | `:9001` | Future admin HTTP address for metrics and health endpoints |
| `DIFY_BASE_URL` | Yes | | Dify API base URL, for example `http://dify-api/v1` |
| `DIFY_APP_KEYS` | Yes | | App key mapping, for example `default=app-xxx;npc-blacksmith=app-yyy` |
| `REDIS_ADDR` | Yes | | Redis address for session/rate-limit storage |
| `AUTH_JWT_PUBKEY` | Yes | | JWT public key PEM. Escaped `\n` sequences are normalized |
| `UPSTREAM_TIMEOUT_SEC` | No | `60` | Upstream call timeout |
| `RATE_PER_PLAYER` | No | `1r/2s` | Per-player rate limit |
| `TOKEN_BUDGET_DAILY` | No | `100000` | Daily token budget per player |
| `MAX_INFLIGHT_UPSTREAM` | No | `200` | Global upstream concurrency limit |
| `MODERATION_ENABLED` | No | `true` | Moderation switch |
| `GATEWAY_SERVICE_NAME` | No | `game-ai-gateway` | OpenTelemetry service name |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | No | | Optional OTLP endpoint |

Example local environment:

```powershell
$env:DIFY_BASE_URL = "http://localhost/v1"
$env:DIFY_APP_KEYS = "default=app-your-key"
$env:REDIS_ADDR = "localhost:6379"
$env:AUTH_JWT_PUBKEY = "-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----"
```

### Build And Test

```powershell
go test ./...
go build ./...
```

Or use the Makefile:

```powershell
make test
make build
make lint
```

`make proto` generates Go code from `api/proto/gateway.proto` using `protoc` and `protoc-gen-go`. It fails with an explanatory message if either tool is missing.

### Run

The current `cmd/gateway` entrypoint is a skeleton and only logs startup:

```powershell
go run ./cmd/gateway
```

The Dify client package is usable from Go code today:

```go
client := dify.NewClient("http://dify-api/v1", "app-key", nil)

result, err := client.Chat(ctx, dify.ChatReq{
	Query:  "What gear do you sell?",
	Inputs: map[string]string{"player_level": "12"},
	User:   "player-10086:npc-blacksmith",
})
```

For streaming:

```go
result, err := client.ChatStream(
	ctx,
	dify.ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"},
	func(taskID, convID string) {
		// Cache taskID while the stream is active so Stop can be called.
	},
	func(delta string) {
		// Forward delta to the game client.
	},
)
```

### Docker

Build the image:

```powershell
docker build -t dify-game-ai-gateway .
```

Run with environment variables:

```powershell
docker run --rm `
  -e DIFY_BASE_URL=http://dify-api/v1 `
  -e DIFY_APP_KEYS=default=app-your-key `
  -e REDIS_ADDR=redis:6379 `
  -e AUTH_JWT_PUBKEY="-----BEGIN PUBLIC KEY-----\n...\n-----END PUBLIC KEY-----" `
  -p 9000:9000 `
  -p 9001:9001 `
  dify-game-ai-gateway
```

### Development Status

Completed milestone scope:

- M0-T1 project skeleton
- M0-T2 config loader
- M0-T3 telemetry foundation
- M1-T1 Dify blocking chat
- M1-T2 Dify SSE streaming parser
- M1-T3 Stop, file upload, and parameters APIs
- M1-T4 upstream error mapping and retry behavior
- M2-T1 Protobuf protocol and frame codec
- M2-T2 TCP access layer (listener/session/mux)
- M2-T3 JWT authentication
- M3-T1 Redis session store and conversation creation lock
- M3-T2 context assembler
- M3-T3 rate limiting / quota / circuit breaker
- M3-T4 content moderation
- M4-T1 main chat pipeline orchestration
- M4-T2 stop and conversation management wiring
- M5-T1 mock Dify and integration tests
- M5-T2 load testing and stability

Next planned milestone:

- M5-T3 security hardening review

### Security Notes

- Dify App API keys must stay on the gateway server and must not be sent to the game client.
- Logs must avoid raw player content and secrets. Use the telemetry redaction helpers when adding new logging.
- Player text should remain in `query`; trusted gateway-side context should be the only source for control `inputs`.
