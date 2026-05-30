# M5-T2 压测与稳定性报告 / Load & Stability Report

对应 PDR M5-T2（压测与稳定性）与第 11 章「压测」「整体验收标准」。

## 1. 目标

- 模拟大量并发连接、每连接持续多轮对话，定位**协程/内存泄漏**与吞吐瓶颈。
- 观测**首 token 延迟 P95**、错误率。
- 验收：达成第 11 章「零泄漏」目标；Dify 正常时错误率为 0。

## 2. 方法 / Methodology

压测以**进程内**方式驱动**完整接入栈**，避免引入外部依赖的噪声：

```
net.Pipe(client↔server) → listener.Server.ServeConn
  → mux（写串行化 + 按 request_id 多路复用）
  → pipeline.Handler（鉴权 → 限流 → 会话 → 上下文 → 审核 → 流式 → 记账）
  → dify.Client（真实 HTTP + SSE 解析 + 重试）
  → difymock（本地 mock Dify，可编排 SSE 序列）
```

- **真实组件**：codec 分帧、listener、mux、pipeline 编排、dify.Client（真实 HTTP/SSE）、difymock。
- **替身（stub）**：auth / limiter / store / context-assembler / moderation=AllowAll。这样泄漏与瓶颈的观测聚焦在**接入层与流式机器**（每连接 + 每请求 goroutine、mux、codec、SSE、HTTP 连接生命周期），而非 Redis 往返延迟。
- **泄漏判定**：预热 1 条连接后记录 `runtime.NumGoroutine()` 基线；跑完全部负载并 `wg.Wait()` 等所有 `ServeConn` 退出后，轮询协程数应回落到基线（容忍 4 个瞬时运行时协程）。dify.Client 的 HTTP transport 关闭 keep-alive，避免空闲连接 goroutine 造成假阳性。

代码：`internal/loadtest/loadtest_test.go`。

## 3. 如何运行 / How to run

```bash
# 负载 + 泄漏测试（-short 下跳过）
go test ./internal/loadtest -run TestLoad -v

# 竞态检测下运行
go test -race ./internal/loadtest -run TestLoad

# 单轮对话基准（延迟 / 分配）
go test ./internal/loadtest -run x -bench BenchmarkChatTurn -benchmem
```

## 4. 结果 / Results（代表性单次运行，Apple M 系列，Go 1.23）

**负载（64 并发连接 × 15 轮 = 960 轮）：**

| 指标 | 值 |
|---|---|
| 总轮次 | 960 |
| 吞吐 | ~19,700 turns/s |
| 首 token 延迟 P50 | ~3.0 ms |
| 首 token 延迟 P95 | ~4.7 ms |
| 首 token 延迟 P99 | ~5.9 ms |
| 错误率 | 0 / 960 = **0.0000** |
| 协程 基线 → 负载后 | 3 → 3（**零泄漏**） |

**基准（单轮对话，端到端经 listener）：**

```
BenchmarkChatTurn-10    10898    108906 ns/op    101985 B/op    315 allocs/op
```

`-race` 下负载测试通过，无数据竞争。

> 数值随机器与运行波动；上表为一次代表性运行。首 token 延迟在此为**回环 HTTP + net.Pipe + mock SSE** 的开销，**不含真实模型生成延迟**。

## 5. 对照验收 / Acceptance

| 第 11 章要求 | 结果 |
|---|---|
| 协程/内存零泄漏 | ✅ 协程负载前后均为基线（3→3）；teardown 后无残留 |
| Dify 正常时端到端流式可用、错误率低 | ✅ 960 轮 0 错误 |
| 首 token 延迟 P95 达标 | ⏱ 已建立基线（接入层开销 P95 ≈ 5ms）。PDR 附录指出**绝对阈值需在接入真实 Dify 后据实设定**；本测量为网关自身开销的下界基线。 |

## 6. 边界与后续 / Caveats & Next

本测试覆盖网关接入/流式机器的并发正确性与泄漏；**不**覆盖以下，需待 `cmd/gateway` 主程序组装（M5-T4）后用真实部署压测补齐：

- 真实 TCP（含 TLS/L4 负载均衡、socket 上限、半开连接）。
- 真实 Redis 限流/会话存储的往返延迟与争用。
- 真实 Dify 模型的首 token 延迟分布（决定生产 P95 阈值与超时/降级参数）。
- 长时间浸泡测试（数小时）下的内存增长曲线（pprof heap 对比）。

建议在 M5-T4 部署后，用外部压测工具（多机客户端）对运行中的网关二进制重跑本场景，据实标定 P95 阈值并接入监控告警。
