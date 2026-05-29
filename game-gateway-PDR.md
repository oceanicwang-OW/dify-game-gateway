# 游戏 AI 网关 — 设计与实现规格说明书 (PDR)

> 版本：v1.0　|　状态：待评审　|　适用：将 Dify 作为 LLM 编排后端、为 C++ 游戏客户端提供 AI 能力的网关服务。
>
> **本文档面向人类工程师与 AI 编码代理双重读者。** 第 12 章「任务拆解」中的每个任务都被设计为自包含、可独立执行、有明确验收标准，可直接分配给 AI 代理逐个完成。AI 执行者应先通读第 1–11 章建立全局认知，再按第 12 章顺序领取任务。

---

## 1. 文档概述

### 1.1 目的与范围

构建一个**游戏 AI 网关 (Game AI Gateway)**，作为 C++ 游戏客户端与 Dify 后端之间的中间层，承担协议转换、鉴权、限流、上下文注入、内容审核、流式透传与可观测性职责。

**范围内：** 网关服务本体、客户端↔网关协议、网关↔Dify 接口封装、鉴权与安全、运维接入。

**范围外：** Dify 自身的部署与运维（按官方 Docker Compose / K8s 部署，当作黑盒）、Dify 应用内的工作流/Prompt 编排（由策划/Prompt 工程师在 Dify 控制台完成）、游戏客户端业务逻辑。

### 1.2 术语表

| 术语 | 含义 |
|---|---|
| 网关 / Gateway | 本文档要实现的服务 |
| Dify App API Key | Dify 某个应用的 `app-xxxx` 密钥，仅存于网关服务端 |
| conversation_id | Dify 维护的多轮会话标识 |
| request_id | 客户端为每次请求生成的标识，用于多路复用 |
| session token | 游戏登录服下发给客户端的鉴权令牌 |
| inputs | Dify 工作流的输入变量，用于注入玩家上下文 |
| SSE | Server-Sent Events，Dify 流式响应所用协议 |
| BFF | Backend For Frontend，本网关的定位 |

### 1.3 设计原则

1. **客户端零密钥**：Dify App API Key 绝不下发到客户端，只存在于网关。
2. **协议隔离**：客户端继续使用游戏自有协议；HTTP/SSE 全部封装在网关内部。
3. **异步非阻塞**：单次 LLM 调用耗时可达数秒，任何环节不得阻塞客户端主循环或网关连接的后续请求。
4. **故障可降级**：Dify 不可用、超时、被限流时，网关返回结构化错误或兜底内容，不让客户端崩溃。
5. **可观测、可计费、可止损**：每次调用可追踪 token 消耗与成本，支持按玩家限流与熔断。
6. **Dify 当黑盒**：仅通过其官方 REST API 交互，不依赖、不修改 Dify 内部实现。

---

## 2. 系统架构

### 2.1 总体拓扑

```
┌──────────────┐   游戏协议(TCP/KCP)    ┌─────────────────────────────┐   HTTPS + SSE   ┌──────────────┐
│ C++ 游戏客户端 │ ───────────────────▶ │        游戏 AI 网关          │ ──────────────▶ │  Dify 后端    │
│              │ ◀─────────────────── │ (本文档实现，Go)             │ ◀────────────── │ (App API)    │
└──────────────┘   流式分帧回包         └─────────────────────────────┘   /v1/*         └──────┬───────┘
                                            │         │         │                                │
                                       鉴权校验   限流/配额   上下文注入                      ┌─────▼─────┐
                                            │         │         │                          │ LLM 服务商 │
                                       内容审核   可观测性   会话映射(Redis)               └───────────┘
       ┌──────────────┐
       │  游戏登录服   │ ──签发 session token──▶ (客户端持有，网关校验)
       └──────────────┘
```

### 2.2 组件职责

| 组件 | 职责 |
|---|---|
| **接入层 (Listener)** | 维护客户端长连接，分帧收发，连接生命周期管理 |
| **协议编解码 (Codec)** | 客户端协议(Protobuf) ↔ 内部统一请求对象互转 |
| **鉴权模块 (Auth)** | 校验 session token，解析玩家身份，绑定到连接 |
| **限流/配额 (Limiter)** | 按玩家/全局做 QPS 限流、token 预算、熔断 |
| **上下文装配 (Context Assembler)** | 把玩家状态组装为 Dify `inputs`，管理 conversation_id |
| **Dify 客户端 (Dify Client)** | 封装 Dify REST API，处理流式/阻塞、重试、超时 |
| **内容审核 (Moderator)** | 对 LLM 输出做审核，命中策略时替换/拦截 |
| **多路复用器 (Multiplexer)** | 用 request_id 区分并发请求，串行化单连接写出 |
| **可观测性 (Telemetry)** | 日志、指标、链路追踪、成本统计 |
| **会话存储 (Session Store)** | Redis：玩家↔conversation_id 映射、限流计数、缓存 |

### 2.3 技术选型

| 项 | 选型 | 理由 |
|---|---|---|
| 语言 | **Go 1.22+** | 高并发网络服务原生友好；单二进制部署；SSE/HTTP 客户端库成熟；goroutine 天然适配「一请求一协程」模型 |
| 客户端协议 | **Protobuf over TCP/KCP** | 与现有游戏网络栈一致；强类型；向后兼容 |
| 会话/限流存储 | **Redis** | conversation 映射、滑动窗口限流、结果缓存 |
| 配置 | 环境变量 + YAML | 12-factor；密钥走 Secrets/KMS |
| 可观测 | OpenTelemetry + Prometheus | 指标与追踪标准化 |
| 部署 | Docker + K8s | 与 Dify 同集群，按负载横向扩 |

> 替代方案：若团队栈为 C++/Rust/Node，可替换语言，但模块边界与接口契约（第 4、5 章）保持不变。

### 2.4 部署形态

无状态网关多副本 + 共享 Redis；前置 L4 负载均衡（长连接需开启会话保持或用一致性哈希）。网关与 Dify 建议同 VPC/同集群，降低 RTT。Dify App API Key 通过 K8s Secret 注入，不落盘明文。

---

## 3. 核心数据流

### 3.1 一次流式对话的完整时序

```
客户端          网关                              Dify
  │  连接 + 鉴权帧 │                                │
  │ ─────────────▶│ 校验 session token             │
  │ ◀─────────────│ 鉴权结果                        │
  │  chat 请求    │                                │
  │ ─────────────▶│ ① 限流检查                      │
  │               │ ② 取/建 conversation_id (Redis) │
  │               │ ③ 组装 inputs (玩家上下文)       │
  │               │ ④ POST /v1/chat-messages stream │
  │               │ ──────────────────────────────▶│
  │               │ ◀── SSE: message(answer delta) ─│
  │ ◀── chunk ────│ ⑤ 审核 delta → 推回客户端        │
  │ ◀── chunk ────│ ◀── SSE: message ───────────────│
  │               │ ◀── SSE: message_end(usage) ────│
  │               │ ⑥ 记录 token/成本，更新映射       │
  │ ◀── done ─────│ (附 conversation_id)            │
```

### 3.2 流式 vs 阻塞

- **默认流式 (`streaming`)**：NPC 对话逐字返回，体验好。网关订阅 SSE，逐 delta 转发。
- **阻塞 (`blocking`)**：适用于一次性结构化输出（如生成一段任务描述）。注意 Cloudflare/网关超时上限约 100s。

### 3.3 多路复用

同一连接可并发多个请求。网关为每个请求开独立 goroutine 调用 Dify；所有回包带 `request_id`，客户端据此归位；单连接写出用互斥锁串行化，防止帧交错。

**会话创建并发约束**：同一 `(player_id, npc_id)` 的首次请求（Redis 键不存在或已过期）必须串行化。网关用进程内 singleflight + Redis 分布式锁（SET NX + TTL）保护「读键→新建会话→回填键」的原子窗口，防止多个并发首次请求各自独立创建 Dify 会话并相互覆盖映射键，导致对话历史分叉。锁 TTL 建议 15s；锁释放后后续等待者直接读已有键，不重入创建逻辑。续聊路径（键命中）不受此约束，无需加锁。

---

## 4. 客户端 ↔ 网关 协议规范

### 4.1 传输与分帧

- 传输：TCP（或 KCP）。
- 分帧：`4 字节大端长度前缀 + Protobuf 负载`。长度上限 4 MB，超限断连。
- 心跳：客户端每 15s 发 Ping，网关回 Pong；60s 无活动断连。

### 4.2 消息定义 (Protobuf)

```proto
syntax = "proto3";
package gateway;

// 客户端 -> 网关
message ClientEnvelope {
  oneof body {
    AuthRequest   auth    = 1;
    ChatRequest   chat    = 2;
    StopRequest   stop    = 3;
    Ping          ping    = 4;
  }
}

message AuthRequest {
  string session_token = 1;   // 游戏登录服下发
  string player_id     = 2;
}

message ChatRequest {
  string request_id      = 1; // 客户端生成，回包据此归位
  string conversation_id = 2; // 空=新会话；通常由网关托管，客户端可不传
  string npc_id          = 3; // 对话对象，用于选择 Dify 应用/会话隔离
  string query           = 4; // 玩家输入
  map<string, string> context = 5; // 可选：客户端侧附加上下文
}

message StopRequest { string request_id = 1; } // 中止生成
message Ping {}

// 网关 -> 客户端
message ServerEnvelope {
  oneof body {
    AuthResult    auth_result = 1;
    ChatChunk     chunk       = 2;
    ChatDone      done        = 3;
    ErrorMsg      error       = 4;
    Pong          pong        = 5;
    ChatBlocked   blocked     = 6;
  }
}

message AuthResult  { bool ok = 1; string reason = 2; }
message ChatChunk   { string request_id = 1; string delta = 2; }
message ChatDone    { string request_id = 1; string conversation_id = 2; uint32 total_tokens = 3; }
message ErrorMsg    { string request_id = 1; string code = 2; string message = 3; }
// 审核命中终止帧：客户端收到后须丢弃该 request_id 已收的全部增量，展示 fallback 文案。
message ChatBlocked { string request_id = 1; string fallback = 2; }
message Pong {}
```

### 4.3 客户端协议错误码

| code | 含义 | 客户端处理建议 |
|---|---|---|
| `UNAUTHENTICATED` | 未鉴权或 token 失效 | 重新登录获取 token |
| `RATE_LIMITED` | 触发限流/配额 | 退避后重试，提示玩家稍候 |
| `UPSTREAM_TIMEOUT` | Dify 超时 | 展示兜底话术 |
| `UPSTREAM_ERROR` | Dify 返回错误 | 展示兜底话术，上报 |
| `CONTENT_BLOCKED` | 审核拦截 | 展示替代文案 |
| `BAD_REQUEST` | 请求字段非法 | 记录并修正 |
| `INTERNAL` | 网关内部错误 | 重试或上报 |

---

## 5. 网关 ↔ Dify 接口规范

> 基础地址 `BASE = {DIFY_BASE_URL}`，自托管通常为 `http://dify-api/v1`，云为 `https://api.dify.ai/v1`。
> 所有请求头：`Authorization: Bearer {APP_API_KEY}`，`Content-Type: application/json`（文件上传除外）。
> **App API Key 仅存于网关服务端，禁止下发客户端。**

### 5.1 接口总览

| 用途 | 方法 & 路径 | 网关是否必用 |
|---|---|---|
| 发送对话消息（核心） | `POST /chat-messages` | 必用 |
| 中止生成 | `POST /chat-messages/{task_id}/stop` | 必用（支持 Stop） |
| 拉取会话历史消息 | `GET /messages?conversation_id=&user=` | 可选 |
| 会话列表 | `GET /conversations?user=` | 可选 |
| 删除会话 | `DELETE /conversations/{conversation_id}` | 可选 |
| 重命名会话 | `POST /conversations/{conversation_id}/name` | 可选 |
| 上传文件（多模态） | `POST /files/upload` | 按需 |
| 获取应用参数(变量定义) | `GET /parameters` | 启动时校验用 |
| 反馈(点赞/点踩) | `POST /messages/{message_id}/feedbacks` | 可选 |
| 建议追问 | `GET /messages/{message_id}/suggested?user=` | 可选 |

### 5.2 发送对话消息 `POST /chat-messages`

**请求体字段：**

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `query` | string | 是 | 玩家输入 |
| `inputs` | object | 是 | 工作流变量键值对（玩家上下文注入入口）。无变量时传 `{}` |
| `user` | string | 是 | 用户唯一标识；**数据隔离边界**——会话/消息/文件仅对相同 `user` 可见。网关用稳定的 `player_id`（或 `player_id:npc_id`） |
| `response_mode` | enum | 否 | `streaming`(推荐) / `blocking`，默认 blocking |
| `conversation_id` | string | 否 | 续聊传上一轮返回值；新会话传空 |
| `files` | array | 否 | 多模态文件，先经 `/files/upload` 取 `id` |
| `auto_generate_name` | bool | 否 | 默认 true，自动生成会话标题 |

**请求示例：**

```json
{
  "query": "你这里有什么好装备？",
  "inputs": { "player_level": "12", "current_quest": "find_sword", "affinity": "friendly" },
  "user": "player-10086:npc-blacksmith",
  "response_mode": "streaming",
  "conversation_id": ""
}
```

**流式响应 (`text/event-stream`)：** 每行形如 `data: {JSON}`，按 `event` 字段区分：

| event | 含义 | 网关动作 |
|---|---|---|
| `message` | 文本增量，字段 `answer` 为 delta；含 `task_id`/`message_id`/`conversation_id` | 缓冲至完整语义单元（句/段）后审核；通过则转发为 `ChatChunk`；首个事件**立即**经 onEvent 回调记录 `task_id`（供 Stop）与 `conversation_id` |
| `message_file` | 输出文件（图片等） | 按需转发 URL |
| `message_end` | 结束；`metadata.usage` 含 token 与成本 | 记录用量与成本，触发 `ChatDone` |
| `message_replace` | 命中 Dify 侧审核，整体替换 answer | 向客户端发送 `ChatBlocked`（fallback 填替换内容），通知客户端丢弃已收增量；不依赖前端自行替换 |
| `tts_message` / `tts_message_end` | 语音分片（启用 TTS 时） | 按需透传 |
| `workflow_started` / `node_started` / `node_finished` / `workflow_finished` | Chatflow 生命周期事件 | 一般忽略，可用于调试/进度 |
| `error` | 流中错误，字段 `message`/`code` | 转 `ErrorMsg`，结束该请求 |
| `ping` | 约每 10s 保活 | 忽略 |

**关键提取逻辑：** 仅 `message`/`agent_message` 的 `answer` 是要展示的文本；`conversation_id` 在事件中持续出现，网关取一次即可缓存；`message_end.metadata.usage.total_tokens` 用于计费。

**阻塞响应：** `application/json`，结构同 `message_end`，`answer` 为完整文本。

### 5.3 中止生成 `POST /chat-messages/{task_id}/stop`

仅 streaming 有效。请求体 `{ "user": "<同一 user>" }`。客户端发 `StopRequest` → 网关用缓存的 `task_id` 调用此接口并关闭上游流。

### 5.4 文件上传 `POST /files/upload`

`multipart/form-data`，字段 `file` 与 `user`。返回 `id`，在 `chat-messages.files` 中以 `{ "type":"image", "transfer_method":"local_file", "upload_file_id":"<id>" }` 引用。

### 5.5 应用参数 `GET /parameters`

返回 `user_input_form`（变量名与类型）、文件上传配置、开场白等。**网关启动时调用一次**，校验本地 `inputs` 约定与 Dify 应用定义一致，不一致则告警，防止上线后变量名对不上。

### 5.6 错误处理与重试

| HTTP | 典型 code | 网关策略 |
|---|---|---|
| 400 | `invalid_param` / `app_unavailable` | 不重试，转 `BAD_REQUEST` |
| 400 | `provider_not_initialize` / `provider_quota_exceeded` | 不重试，转 `UPSTREAM_ERROR`，告警 |
| 400 | `completion_request_error` | 视情况一次重试 |
| 404 | 会话不存在 | 清除本地映射，作为新会话重试一次 |
| 429 | 限流 | 指数退避重试 ≤2 次，仍失败转 `RATE_LIMITED` |
| 5xx | — | 指数退避重试 ≤2 次，仍失败转 `UPSTREAM_ERROR` |
| 超时 | — | 总超时建议 60s；转 `UPSTREAM_TIMEOUT`，返回兜底 |

重试需保证幂等：仅在「尚未向客户端发出任何 delta」时才允许整请求重试，否则只能中止并报错。

---

## 6. 鉴权与安全

### 6.1 玩家鉴权

1. 客户端登录游戏登录服，获得短期 **session token**（JWT，含 `player_id`、`exp`、签名）。
2. 客户端连接网关后首帧发 `AuthRequest`，网关验证 token 签名与有效期（用登录服公钥本地校验，或调登录服校验接口）。
3. 验证通过后将 `player_id` 绑定到该连接；后续请求无需重复带 token。
4. token 过期：网关回 `UNAUTHENTICATED`，客户端重登。

> 网关**不**创建账号、不处理密码；账号体系归登录服。

### 6.2 网关与 Dify 的信任边界

- Dify App API Key 通过 K8s Secret / KMS 注入环境变量，进程内持有，**不写日志、不落盘明文、不进客户端**。
- 多 NPC/多场景可用多个 Dify 应用，网关维护 `npc_id → APP_API_KEY` 映射，统一从 Secret 读取。
- 网关→Dify 走内网/同集群；若跨网必须 HTTPS。

### 6.3 限流、配额与熔断

| 维度 | 策略 |
|---|---|
| 单玩家 QPS | Redis 滑动窗口，如 1 req/2s |
| 单玩家 token 预算 | 每日/每小时累计上限，超限拒绝并提示 |
| 全局并发 | 信号量限制同时在途的 Dify 请求数，保护上游 |
| 熔断 | Dify 连续错误率超阈值时短路，直接走兜底，定时半开探测 |

### 6.4 内容审核

- **输入审核**：玩家 `query` 先过敏感词/模型审核，命中则不调用 LLM，直接兜底。
- **输出审核**：流式输出**必须缓冲至完整语义单元（句/段）再审核**，不得逐裸 delta 检查（不当内容可能横跨多个 chunk，逐 token 检查会漏判）。通过审核的缓冲内容方可作为 `ChatChunk` 发出。一旦命中审核，立即停止后续增量，向客户端发送 `ChatBlocked`（含兜底文案）；客户端须丢弃该请求已收全部增量并展示兜底内容。`message_replace` 事件同样触发 `ChatBlocked`。
- 合规留存：审核结果与原文按合规要求脱敏留存（注意隐私边界）。

### 6.5 输入校验与注入防护

- 严格校验 `query` 长度、`context` 键白名单、`npc_id` 合法性。
- 玩家输入只能进入 `query`，**绝不允许玩家内容直接写入会改变系统行为的 `inputs` 控制变量**（防 Prompt 注入提权）。控制类变量只能由网关用可信玩家状态填充。
- 拒绝异常大小/异常频率的请求。

### 6.6 数据隔离与隐私

- Dify `user` 字段严格用玩家可信标识，确保会话与消息按玩家隔离，杜绝串号。
- 日志中对玩家输入/输出做必要脱敏；不在 URL query 中放敏感数据。
- 不收集、不外传与游戏无关的玩家 PII。

---

## 7. 上下文注入设计

### 7.1 inputs 变量约定

在 Dify 应用中预先定义工作流变量，网关与策划共同维护一份**变量契约**（示例）：

| 变量名 | 类型 | 来源 | 说明 |
|---|---|---|---|
| `player_level` | string | 游戏服玩家状态 | 等级 |
| `current_quest` | string | 任务系统 | 当前任务 ID |
| `affinity` | string | 好感度系统 | NPC 对玩家态度 |
| `scene` | string | 客户端/场景服 | 当前场景 |
| `recent_events` | string | 事件系统 | 最近发生的剧情事件摘要 |

网关在 Context Assembler 中拉取这些值（来源：游戏内部服务/缓存），组装进 `inputs`。策划改 Prompt 引用这些变量即可，无需改网关代码。

### 7.2 会话生命周期

- **键**：`conv:{player_id}:{npc_id}` → `conversation_id`，存 Redis，设 TTL（如 24h 不活跃过期）。
- **新建**：键不存在时 `conversation_id` 传空，从 `message_end` 回填并写入 Redis。整个「检查键→发起新会话→写入键」流程必须在 `(player_id, npc_id)` 粒度的分布式锁保护下执行（见 §3.3），防止多副本并发首次请求产生分叉会话。
- **续聊**：命中键则带上历史 `conversation_id`。
- **重置**：玩家主动「重新开始对话」或剧情切换时删除键。
- **清理**：可选定期调用 Dify 删除会话接口回收。

---

## 8. 可观测性与运维

### 8.1 日志

结构化 JSON 日志，每次请求记录：`request_id`、`player_id`(脱敏)、`npc_id`、`conversation_id`、上游耗时、首 token 延迟、`total_tokens`、成本、结果状态、错误码。**禁止记录完整 API Key 与未脱敏玩家内容。**

### 8.2 指标 (Prometheus)

`gateway_requests_total{status}`、`gateway_upstream_latency_seconds`(直方图)、`gateway_first_token_latency_seconds`、`gateway_tokens_total`、`gateway_cost_usd_total`、`gateway_active_connections`、`gateway_inflight_upstream`、`gateway_ratelimited_total`、`gateway_circuit_state`。

### 8.3 追踪

OpenTelemetry：客户端请求 → 鉴权 → 上下文装配 → Dify 调用 → 审核 的全链路 span。

### 8.4 扩缩容

网关无状态，按 `active_connections` 与 `inflight_upstream` 自动扩缩；Redis 用集群/哨兵；Dify 侧 API 与 Celery worker 独立扩容（属 Dify 运维，非本服务职责）。

---

## 9. 模块设计（Go 参考目录结构）

```
game-gateway/
├── cmd/gateway/main.go            # 启动、装配依赖、优雅退出
├── internal/
│   ├── config/                    # 配置与环境变量加载、校验
│   ├── listener/                  # TCP/KCP 接入、分帧、连接管理
│   ├── codec/                     # Protobuf 编解码
│   ├── session/                   # 连接会话状态、player 绑定
│   ├── auth/                      # session token 校验
│   ├── limiter/                   # 限流/配额/熔断 (Redis)
│   ├── context/                   # 玩家上下文拉取与 inputs 装配
│   ├── dify/                      # Dify REST 客户端 (chat-messages/stop/files...)
│   │   ├── client.go
│   │   ├── sse.go                 # SSE 解析
│   │   └── types.go
│   ├── moderation/                # 输入/输出审核
│   ├── mux/                       # 单连接多路复用与写串行化
│   ├── store/                     # Redis: 会话映射/限流/缓存
│   └── telemetry/                 # 日志/指标/追踪
├── api/proto/gateway.proto        # 客户端协议定义
├── deploy/ (Dockerfile, k8s/, helm/)
└── test/ (integration, load)
```

### 9.1 关键内部接口（契约，便于并行开发与替换实现）

```go
type Authenticator interface {
    Verify(ctx context.Context, token string) (playerID string, err error)
}
type Limiter interface {
    Allow(ctx context.Context, playerID string, estTokens int) (bool, error)
    Record(ctx context.Context, playerID string, usedTokens int) error
}
type ContextAssembler interface {
    Build(ctx context.Context, playerID, npcID string) (map[string]string, error)
}
type Moderator interface {
    CheckInput(ctx context.Context, text string) (allowed bool, fallback string)
    CheckOutput(ctx context.Context, chunk string) (allowed bool, replacement string)
}
type DifyClient interface {
    // 流式：首个 message 事件触发 onEvent(taskID, convID)，调用方须在此回调中缓存 taskID
    // 以便在流仍活跃时调用 Stop；随后每个 delta 触发 onDelta；流结束后返回用量。
    ChatStream(ctx context.Context, req ChatReq, onEvent func(taskID, convID string), onDelta func(delta string)) (ChatResult, error)
    Stop(ctx context.Context, taskID, user string) error
}
type SessionStore interface {
    GetConversation(ctx context.Context, playerID, npcID string) (string, error)
    SetConversation(ctx context.Context, playerID, npcID, convID string, ttl time.Duration) error
    DeleteConversation(ctx context.Context, playerID, npcID string) error
    // AcquireConversationLock 获取 (playerID,npcID) 维度的分布式锁，用于首次会话创建串行化（见 §3.3）。
    // 返回的 unlock 必须在新会话写入 Redis 后调用；ctx 取消或 ttl 到期时锁自动释放。
    AcquireConversationLock(ctx context.Context, playerID, npcID string, ttl time.Duration) (unlock func(), err error)
}
```

---

## 10. 配置项（环境变量）

| 变量 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `GATEWAY_ADDR` | 否 | `:9000` | 监听地址 |
| `DIFY_BASE_URL` | 是 | — | 如 `http://dify-api/v1` |
| `DIFY_APP_KEYS` | 是 | — | `npc_id=app-xxx;default=app-yyy` 多应用映射 |
| `REDIS_ADDR` | 是 | — | Redis 地址 |
| `AUTH_JWT_PUBKEY` | 是 | — | 登录服 JWT 验签公钥 |
| `UPSTREAM_TIMEOUT_SEC` | 否 | `60` | Dify 调用总超时 |
| `RATE_PER_PLAYER` | 否 | `1r/2s` | 单玩家限流 |
| `TOKEN_BUDGET_DAILY` | 否 | `100000` | 单玩家日 token 预算 |
| `MAX_INFLIGHT_UPSTREAM` | 否 | `200` | 全局在途上游请求上限 |
| `MODERATION_ENABLED` | 否 | `true` | 是否启用审核 |

---

## 11. 测试与验收

- **单元测试**：codec、sse 解析、limiter 窗口、context 装配、错误映射。
- **集成测试**：用 mock Dify（可复现 SSE 序列）验证流式转发、Stop、重试、降级。
- **端到端**：用 C++ demo 客户端跑通鉴权→多轮对话→中止→限流→兜底。
- **压测**：N 个并发连接、每连接持续对话，观测首 token 延迟 P95、错误率、内存/协程泄漏。
- **安全测试**：无效/过期 token、超长输入、Prompt 注入尝试、密钥是否泄漏到日志/客户端。

**整体验收标准：** 在 Dify 正常时端到端流式可用且首 token 延迟 P95 达标；Dify 异常时全部请求走结构化错误/兜底且不崩；密钥零泄漏；限流与配额生效；指标与日志齐全。

---

## 12. 任务拆解（供 AI 代理执行）

> 执行约定：每个任务自包含，含「目标 / 依赖 / 产出物 / 验收标准」。按里程碑顺序执行；同里程碑内无依赖关系的任务可并行。完成一个任务后运行其验收项再领取下一个。技术栈默认 Go，若变更需在 M0-T1 记录并保持接口契约不变。

### 里程碑 M0 — 工程骨架

**M0-T1 初始化项目骨架**
- 目标：建立第 9 章目录结构、`go.mod`、lint/CI、Dockerfile。
- 依赖：无。
- 产出物：可编译的空骨架、`make build/test/lint` 可用。
- 验收：`go build ./...` 与 `go test ./...` 通过；CI 绿。

**M0-T2 配置加载模块 (`config`)**
- 目标：实现第 10 章全部环境变量的加载、默认值与启动校验（缺必填项即 fail-fast）。
- 依赖：M0-T1。
- 产出物：`config.Load()`，含单测。
- 验收：缺 `DIFY_BASE_URL` 等必填项时启动报错并打印缺失项。

**M0-T3 可观测性基座 (`telemetry`)**
- 目标：结构化日志、Prometheus 指标注册、OTel 初始化；提供脱敏工具函数。
- 依赖：M0-T1。
- 产出物：`telemetry.Init()`、`/metrics` 端点、脱敏 helper。
- 验收：`/metrics` 可抓取；日志为 JSON 且密钥/玩家内容被脱敏。

### 里程碑 M1 — Dify 客户端（可独立交付）

**M1-T1 Dify 类型与请求构造 (`dify/types.go`, `client.go`)**
- 目标：实现 `ChatReq` 构造、`Authorization` 头、`POST /chat-messages`（blocking 先行）。
- 依赖：M0-T2。
- 产出物：`DifyClient.Chat()`(阻塞)。
- 验收：对 mock 服务发出符合第 5.2 节字段的请求；阻塞响应解析出 `answer`/`conversation_id`/`total_tokens`。

**M1-T2 SSE 流式解析 (`dify/sse.go`)**
- 目标：解析 `data: {JSON}` 行，按第 5.2 节事件表分发；正确提取 `answer` 增量、`task_id`、`conversation_id`、`message_end.usage`。
- 依赖：M1-T1。
- 产出物：`DifyClient.ChatStream(..., onDelta)`。
- 验收：喂入录制的 SSE 序列（含 message/message_end/ping/error/message_replace）能正确回调 delta；**首个 message 事件触发 onEvent 回调并传出 task_id/conversation_id**（不等到流结束）；识别结束与错误；message_replace 触发正确的 fallback 路径。

**M1-T3 Stop / 文件 / 参数接口**
- 目标：实现 `POST /chat-messages/{task_id}/stop`、`POST /files/upload`、`GET /parameters`。
- 依赖：M1-T2。
- 产出物：`Stop()`、`UploadFile()`、`GetParameters()`。
- 验收：Stop 能中止 mock 流；启动时 `GetParameters()` 可对比变量契约并告警。

**M1-T4 错误映射与重试**
- 目标：实现第 5.6 节 HTTP/code→内部错误映射、指数退避重试、幂等保护（已发 delta 不重试）。
- 依赖：M1-T2。
- 产出物：统一错误类型与重试包装。
- 验收：对 429/5xx 触发退避重试；对 400 类不重试；已发增量后上游断开只报错不重试。

### 里程碑 M2 — 接入层与协议

**M2-T1 Protobuf 协议与编解码 (`api/proto`, `codec`)**
- 目标：按第 4.2 节定义 proto 并生成代码；实现 4 字节长度前缀分帧编解码。
- 依赖：M0-T1。
- 产出物：生成的 pb 代码、`codec.ReadFrame/WriteFrame`。
- 验收：编解码往返一致；超长帧被拒绝。

**M2-T2 接入层与连接管理 (`listener`, `session`, `mux`)**
- 目标：TCP 监听、每连接 goroutine、心跳、连接级写互斥、按 request_id 多路复用。
- 依赖：M2-T1。
- 产出物：可接收 `ClientEnvelope`、回写 `ServerEnvelope` 的接入层。
- 验收：并发多请求回包不交错且按 request_id 可归位；连接断开资源释放无泄漏。

**M2-T3 鉴权模块 (`auth`)**
- 目标：JWT 验签校验 session token，绑定 player_id 到连接；过期/非法回 `UNAUTHENTICATED`。
- 依赖：M2-T2，M0-T2。
- 产出物：`Authenticator` 实现 + 首帧鉴权流程。
- 验收：有效 token 通过；过期/篡改 token 被拒。

### 里程碑 M3 — 业务编排与防护

**M3-T1 会话存储与映射 (`store`)**
- 目标：Redis 实现 `conv:{player}:{npc}` 映射的增删查与 TTL。
- 依赖：M0-T2。
- 产出物：`SessionStore` 实现。
- 验收：新建/续聊/重置语义正确；TTL 过期生效；**两个并发首次请求（相同 player_id/npc_id，键不存在）只创建一个 Dify 会话，映射键无竞争覆盖**；键过期后的并发首次请求同上。

**M3-T2 上下文装配 (`context`)**
- 目标：按第 7.1 节从来源拉取玩家状态组装 `inputs`；来源不可用时降级为部分上下文。
- 依赖：M3-T1。
- 产出物：`ContextAssembler` 实现（来源可先用接口+mock）。
- 验收：输出的 `inputs` 键符合变量契约；控制类变量不被玩家输入污染。

**M3-T3 限流/配额/熔断 (`limiter`)**
- 目标：Redis 滑动窗口单玩家限流、日 token 预算、全局在途信号量、熔断器。
- 依赖：M3-T1，M0-T3。
- 产出物：`Limiter` 实现 + 熔断状态指标。
- 验收：超频/超预算被拒并计指标；上游高错误率时熔断打开并半开恢复。

**M3-T4 内容审核 (`moderation`)**
- 目标：输入与（流式）输出审核接口；命中时输入直接兜底、输出替换并标记 `CONTENT_BLOCKED`。
- 依赖：M2-T2。
- 产出物：`Moderator` 实现（策略可先用敏感词，预留模型审核接口）。
- 验收：命中输入不触发上游调用；命中输出停止后续增量并向客户端发送 `ChatBlocked`（含兜底文案）；**跨 chunk 拆分的不当内容经句级缓冲可被正确检出并阻断**（单 delta 切分测试）。

### 里程碑 M4 — 主流程编排

**M4-T1 对话主链路编排**
- 目标：串起 鉴权→限流→会话映射→上下文装配→输入审核→Dify 流式→输出审核→回写→记账，覆盖第 3.1 时序与全部错误分支。
- 依赖：M1-T4、M2-T3、M3-T2、M3-T3、M3-T4。
- 产出物：`ChatRequest` 完整处理器。
- 验收：端到端流式可用；各失败分支返回正确客户端错误码；`message_end` 用量被记账与计指标。

**M4-T2 中止与会话管理对接**
- 目标：处理 `StopRequest`（用缓存 task_id 调 Stop 并关流）；处理会话重置。
- 依赖：M4-T1。
- 产出物：Stop/Reset 流程。
- 验收：**在首个 delta 之后、流结束之前**发出 StopRequest，能立即停推且不再产生增量（验证 task_id 在流活跃期间已通过 onEvent 回调可用）；Reset 后下条消息为新会话。

### 里程碑 M5 — 联调、加固、交付

**M5-T1 Mock Dify 与集成测试**
- 目标：实现可编排 SSE 序列的 mock Dify；覆盖正常/超时/429/5xx/错误事件/审核替换。
- 依赖：M4-T2。
- 产出物：mock 服务 + 集成测试集。
- 验收：上述场景测试全绿。

**M5-T2 压测与稳定性**
- 目标：模拟大量并发连接持续对话，定位泄漏与瓶颈。
- 依赖：M5-T1。
- 产出物：压测脚本与报告。
- 验收：达成第 11 章 P95 与零泄漏目标。

**M5-T3 安全加固审查**
- 目标：核对密钥不泄漏、玩家内容脱敏、注入防护、token 校验、错误信息不外泄内部细节。
- 依赖：M4-T2。
- 产出物：安全检查清单结果。
- 验收：第 11 章安全测试全部通过。

**M5-T4 部署与文档**
- 目标：Dockerfile、K8s/Helm、Secret 注入、就绪/存活探针、运维手册与配置说明。
- 依赖：M5-T2。
- 产出物：可部署制品与文档。
- 验收：在测试集群一键部署，探针正常，指标接入监控。

---

## 13. 附录：风险与开放问题

- **首 token 延迟**：受 Dify 工作流复杂度与模型影响，需在 M5 压测后据实调优超时与降级阈值。
- **长连接负载均衡**：L4 需会话保持或一致性哈希，避免重连风暴。
- **多应用密钥管理**：NPC 数量增长后 `npc_id→key` 映射的维护方式需在 M3 前确定（配置中心 vs 环境变量）。
- **审核延迟 vs 流式体验**：句级缓冲审核（§6.4 要求）会引入可感知的分段延迟，牺牲逐字顺滑度；这是正确性/安全性的必要代价。产品侧需确认缓冲粒度（句 vs 段），并在 M3-T4 中对不同粒度做延迟基准测试。
- **Dify 版本兼容**：升级 Dify 前需回归 SSE 事件与字段，防止接口契约变化。
