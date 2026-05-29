# Dify Game AI Gateway / Dify 游戏 AI 网关

## 中文说明

Dify 游戏 AI 网关是位于游戏客户端和 Dify 之间的 Go 服务层。它用于把 Dify App API Key 保留在服务端，将游戏请求转换为 Dify API 调用，把模型输出流式返回给游戏客户端，并为鉴权、限流、内容审核、可观测性和会话管理提供基础。

本仓库仍处于里程碑开发阶段。当前已实现的范围主要包括项目骨架、配置加载、可观测性基座和 Dify client 包。

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
- 上游 HTTP 错误映射与重试：在尚未发出任何流式 delta 前，对 429/5xx 做重试。

尚未实现：TCP 接入层、Protobuf 编解码、客户端 session 管理、JWT 鉴权、Redis 会话存储、上下文装配、限流器、内容审核和完整端到端网关编排。

### 目录结构

```text
cmd/gateway/          网关入口
internal/config/      环境变量加载与校验
internal/dify/        Dify REST 与 SSE client
internal/telemetry/   指标、JSON 日志和脱敏 helper
api/                  后续协议/proto 工作区
deploy/               部署工作区
test/                 验证脚本和后续集成测试
```

详细产品和实现计划见 [game-gateway-PDR.md](./game-gateway-PDR.md)。

### 环境要求

- Go 1.23 或更高版本
- Dify App API endpoint 和 app API key
- Redis 地址和 JWT 公钥目前会被配置校验要求提供，实际运行消费这些配置的模块将在后续里程碑实现

### 配置项

网关通过环境变量读取配置。

| 变量 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `GATEWAY_ADDR` | 否 | `:9000` | 后续 TCP 网关监听地址 |
| `GATEWAY_ADMIN_ADDR` | 否 | `:9001` | 后续管理 HTTP 地址，用于 metrics 和健康检查 |
| `DIFY_BASE_URL` | 是 | | Dify API base URL，例如 `http://dify-api/v1` |
| `DIFY_APP_KEYS` | 是 | | App key 映射，例如 `default=app-xxx;npc-blacksmith=app-yyy` |
| `REDIS_ADDR` | 是 | | 后续会话/限流存储使用的 Redis 地址 |
| `AUTH_JWT_PUBKEY` | 是 | | JWT 公钥 PEM；转义的 `\n` 会被规范化 |
| `UPSTREAM_TIMEOUT_SEC` | 否 | `60` | 规划中的上游调用超时 |
| `RATE_PER_PLAYER` | 否 | `1r/2s` | 规划中的单玩家限流 |
| `TOKEN_BUDGET_DAILY` | 否 | `100000` | 规划中的单玩家每日 token 预算 |
| `MAX_INFLIGHT_UPSTREAM` | 否 | `200` | 规划中的全局上游并发上限 |
| `MODERATION_ENABLED` | 否 | `true` | 规划中的内容审核开关 |
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

`make proto` 在 M2 协议里程碑前是占位目标，会输出说明并失败，不会生成代码。

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

下一个计划里程碑：

- M2-T1 Protobuf 协议与分帧编解码

### 安全注意事项

- Dify App API Key 必须只保存在网关服务端，不能发送到游戏客户端。
- 日志应避免记录原始玩家内容和密钥。新增日志时应使用 telemetry 脱敏 helper。
- 玩家输入应只进入 `query`；会影响系统行为的控制类 `inputs` 只能来自网关侧可信上下文。

---

## English

Dify Game AI Gateway is a Go service layer between a game client and Dify. It keeps Dify App API keys on the server side, translates game requests into Dify API calls, streams model output back to the game client, and provides the foundation for auth, rate limiting, moderation, observability, and session management.

This repository is under active milestone development. The implemented surface currently focuses on the project skeleton, configuration, telemetry, and the Dify client package.

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
- Upstream HTTP error mapping and retry handling for 429/5xx before any stream delta is emitted.

Not yet implemented: TCP listener, Protobuf codec, client session handling, JWT auth, Redis session store, context assembler, limiter, moderation, and full end-to-end gateway orchestration.

### Repository Layout

```text
cmd/gateway/          Gateway entrypoint
internal/config/      Environment loading and validation
internal/dify/        Dify REST and SSE client
internal/telemetry/   Metrics, JSON logging, and redaction helpers
api/                  API/proto workspace for later protocol work
deploy/               Deployment workspace
test/                 Verification scripts and future integration tests
```

The detailed product and implementation plan lives in [game-gateway-PDR.md](./game-gateway-PDR.md).

### Requirements

- Go 1.23 or newer
- Dify App API endpoint and app API key
- Redis and JWT public key values are required by configuration validation, although their runtime consumers are planned for later milestones

### Configuration

The gateway reads configuration from environment variables.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `GATEWAY_ADDR` | No | `:9000` | Future TCP gateway listen address |
| `GATEWAY_ADMIN_ADDR` | No | `:9001` | Future admin HTTP address for metrics and health endpoints |
| `DIFY_BASE_URL` | Yes | | Dify API base URL, for example `http://dify-api/v1` |
| `DIFY_APP_KEYS` | Yes | | App key mapping, for example `default=app-xxx;npc-blacksmith=app-yyy` |
| `REDIS_ADDR` | Yes | | Redis address for later session/rate-limit storage |
| `AUTH_JWT_PUBKEY` | Yes | | JWT public key PEM. Escaped `\n` sequences are normalized |
| `UPSTREAM_TIMEOUT_SEC` | No | `60` | Planned upstream call timeout |
| `RATE_PER_PLAYER` | No | `1r/2s` | Planned per-player rate limit |
| `TOKEN_BUDGET_DAILY` | No | `100000` | Planned daily token budget per player |
| `MAX_INFLIGHT_UPSTREAM` | No | `200` | Planned global upstream concurrency limit |
| `MODERATION_ENABLED` | No | `true` | Planned moderation switch |
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

`make proto` is intentionally a placeholder until the M2 protocol milestone. It fails with an explanatory message instead of generating code.

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

Next planned milestone:

- M2-T1 Protobuf protocol and frame codec

### Security Notes

- Dify App API keys must stay on the gateway server and must not be sent to the game client.
- Logs must avoid raw player content and secrets. Use the telemetry redaction helpers when adding new logging.
- Player text should remain in `query`; trusted gateway-side context should be the only source for control `inputs`.
