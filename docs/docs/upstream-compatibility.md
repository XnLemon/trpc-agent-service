# 上游 server / OpenClaw 兼容性评估

## tRPC-Agent-Go server

锁定依赖为 `trpc.group/trpc-go/trpc-agent-go v1.11.2`。当前验证结果：

| 上游入口 | 平台验证 | 结论 |
| --- | --- | --- |
| `server/trpcagent` `WithRunner` / `Handler()` | `trpcservice/gateway/trpcagent_test.go`，覆盖认证、请求体边界、Runner 事件相关性和取消 | 可复用，但 Runner 必须是平台 Dispatch 适配器；客户端 `RunOptions` 不得覆盖平台身份/策略 |
| `server/a2a` | `trpcservice/gateway/a2a_test.go`，覆盖 A2A JSON-RPC 到 Dispatch 的转换 | 可复用协议转换；认证、租户解析、幂等和 Outbox 仍由平台持有 |
| 上游 OpenAI server | 本仓库的 `gateway/openai.go` 测试覆盖平台 OpenAI-compatible 入口 | 不直接把上游固定 Runner server 切入默认路径；平台入口必须先经过 Dispatch |

上游 server 绑定单个 Runner/handler，不能直接承担平台的动态 tenant/app/revision Runner 选择。因此默认路径保留 `HTTPHandler -> DispatchService -> ExecutionPlan -> Runner`，上游 server 只作为已认证协议适配层。

## OpenClaw

OpenClaw 的公开扩展面是独立 TypeScript Gateway：

- Channel Plugin 通过 `registerHttpRoute` 暴露入站 webhook；
- Gateway 通过 WebSocket JSON-RPC 暴露核心和插件方法；
- Channel account runtime 由 OpenClaw Gateway 启停和重启；
- outbound channel 通常仍由 OpenClaw 自己发送。

当前仓库是 Go module，未发现可锁定、可调用的 OpenClaw Go SDK。公开接口也没有同时证明以下平台合同：

1. 入站请求可在 OpenClaw 外部完成平台 tenant/app/principal 认证；
2. 出站发送可转交平台 Reply Outbox，并保证 provider receipt、lease/fence 和 `unknown` 语义；
3. account runtime 可以由平台按 tenant/binding 独立拥有、关闭和恢复。

因此本版不把 OpenClaw 当作可替换的 Go Channel provider，也不复制其协议结构体建立伪 bridge。现有企业微信、Telegram 和 Outbox 路径继续由平台控制。后续若引入 OpenClaw，必须先提供独立 adapter proof：真实 webhook -> `DispatchService`、真实 outbound -> `ReplyOutbox`、双 tenant account isolation、重启/重复投递测试，再允许切换默认路径。

## 已执行验证

```bash
go test ./trpcservice/gateway -run 'Test(OpenAI|A2A|TRPCAgent)'
go test ./trpcservice/channels ./trpcservice/outbox
```

外部 OpenClaw Gateway 需要独立运行时和账号凭据；本仓库不宣称已经完成该 live integration。
