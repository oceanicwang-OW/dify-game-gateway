# M5-T3 安全加固审查 / Security Hardening Review

对应 PDR M5-T3 与第 11 章「安全测试」。

## 1. 审查范围

- Dify App API Key 不泄漏到客户端、日志或启动错误。
- 玩家输入/输出不直接写日志；客户端错误不包含上游内部错误体。
- `inputs` 注入防护：客户端 `ChatRequest.context` 不能污染 Dify 控制变量。
- JWT session token 校验：无效、过期、篡改、`alg=none`、HMAC alg-confusion 均拒绝。
- 超长输入在进入限流、会话、上下文、Dify 调用前被拒绝。

## 2. 结果

| 项目 | 结果 | 证据 |
| --- | --- | --- |
| API Key 仅服务端持有 | 通过 | Dify client 只在 HTTP `Authorization` 头使用 key；新增 `TestLoadDoesNotLeakDifyAppKeyInParseErrors` 覆盖配置解析错误不泄漏 key。 |
| 客户端错误不泄漏上游内部信息 | 通过 | `clientErrorMessage` 只返回通用文案；新增 `TestHandleChatDoesNotExposeUpstreamErrorBodyToClient`。 |
| 玩家内容不写日志 | 通过 | pipeline/listener 日志不记录 `query`、delta 或 blocked fallback；telemetry 提供 `Redact` 并已有 JSON 日志脱敏测试。 |
| Prompt/inputs 注入防护 | 通过 | `context.Assembler` 只允许变量契约内的可信 provider 输出；新增 `TestHandleChatIgnoresClientContextForDifyInputs` 覆盖客户端 context 被忽略。 |
| JWT/token 校验 | 通过 | `internal/auth` 要求 `exp`，验证签名与 player 绑定，拒绝 none/HMAC 混淆；已有 auth 单测覆盖。 |
| 超长输入 | 已加固 | 新增 `maxQueryRunes=4096`，超限返回 `BAD_REQUEST` 且不触发 limiter/store/context/Dify；新增 `TestHandleChatRejectsOverlongQueryBeforeSideEffects`。 |
| 分帧上限 | 通过 | `codec.MaxFrameSize = 4 MB`，超限帧拒绝。 |

## 3. 本轮修复

- `internal/pipeline`: 增加 `query` 最大 4096 字符限制，超限在任何外部副作用前拒绝。
- `internal/config`: `DIFY_APP_KEYS` 解析错误不再包含原始 mapping，避免误配时泄漏 app key。
- `README.md`: 将开发状态推进到 M5-T3 完成，下一里程碑为 M5-T4。

## 4. 剩余边界

- `cmd/gateway` 尚未完成进程组装；真实运行环境中的 TLS/L4、Redis、Dify、K8s Secret、探针和部署压测属于 M5-T4。
- 当前安全测试覆盖网关代码路径与进程内集成路径；真实部署后的日志采集、Secret 注入和外部压测需要在 M5-T4 复核。
