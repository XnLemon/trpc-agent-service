# Issue #79：生产可观测性、Dashboard 与告警

> 本页是 Issue [#79](https://github.com/XnLemon/trpc-agent-service/issues/79) 的文档先行契约。它承接 Issue #45 的 provider-neutral telemetry 和 Issue #54 的 tenant-scoped usage/audit；本页固定实现边界，再进入代码阶段。

## 目标与非目标

目标是让一次可信请求在 http.request → gateway.dispatch → runner.execution 下继续关联到 model.call、tool.call、storage.operation 和 channel.receive/channel.send，并提供可安全聚合的运行指标、租户授权的查询模型以及可部署的 dashboard/alert 资源。

本 Issue 不改变审计事实源，不把 Prometheus 当作成本或合规事实源，不新增 Session/Memory/Knowledge/Artifact 后端，不实现新的 IM 协议。指标导出失败只能丢弃 telemetry，不能阻塞业务提交、租约或关闭。

## Trace 合同

所有入口接受 W3C-compatible correlation（当前 HTTP 兼容 X-Request-ID/X-Trace-ID），缺失 request ID 时生成受界限 ID。子操作必须继承同一 context.Context；取消和 deadline 分别归类为 canceled/timeout。span 名称稳定且只允许下列受控属性：component、operation、status、error_class、tenant_hash、app_hash、model_family、provider、channel。

每个 operation 只能有一个终态：开始时可记录 `status=started`，但必须在成功、业务错误、取消或超时时记录恰好一个终态并结束 span。Model callback 以一次模型调用为边界：流式 partial response 不能提前结束 span 或重复累计 token；`GenerateContent` 在创建响应流前返回的错误也必须结束同一 span。企业微信 receive 创建的 context 必须作为 Gateway/Runner dispatch 的 parent，不能从独立的 handler 根 context 重新开始链路。

Storage telemetry 覆盖实际 RuntimeStore/Session service 的读写方法（Get/Create/Update/Delete Session、Append/List event、message/reply lifecycle），而不只覆盖 capability factory construction 或 inbound claim；新增 adapter 必须复用同一 hook。

不得写入 token、API key、DSN 密码、Authorization、完整 URL、session/user/message/request 原文或不受界限的外部 ID。tenant/app 关联只使用配置的短 hash；hash 不作为 Prometheus label，避免租户数量直接变成指标基数。

## 指标目录

| 名称 | 类型 | 语义 | 允许标签 |
| --- | --- | --- | --- |
| trpcservice_requests_total | counter | 各组件请求/调用量 | component, operation, provider, channel, status, error_class, model_family |
| trpcservice_operation_duration_ms | histogram | HTTP、Runner、Model、Tool、Storage、Channel 延迟 | 同上 |
| trpcservice_active_executions | up/down counter | 当前执行数 | component |
| trpcservice_runner_leases | up/down counter | Runner lease 数 | component, status |
| trpcservice_operation_retries_total | counter | 重试次数 | component, operation, provider, error_class |
| trpcservice_tokens_total | counter | 输入/输出 token 聚合 | component, provider, model_family |
| trpcservice_cost_minor_total | counter | 授权聚合成本（最小货币单位） | component, provider, model_family |
| trpcservice_backend_operation_duration_ms | histogram | Session/Storage 后端延迟 | component, provider, status, error_class |
| trpcservice_channel_deliveries_total | counter | IM 发送成功/失败/重试/DLQ | channel, provider, status, error_class |

标签白名单在代码中集中校验；禁止 tenant_id、user_id、session_id、message_id、request_id、trace_id、完整 URL 和原文。provider 的 canonical 枚举是 `openai|postgres|inmemory|other`，channel 是 `api|telegram|wecom|outbox|other`，model_family 是 `gpt|claude|gemini|other`；未知或租户自定义值统一映射到 `other`，不能直接把模型名作为 label。Token/cost 的生产来源是单个 AuditEvent 的 `Event.Cost` 增量：只有 `Append` 返回 `err == nil && Duplicate == false` 时上报一次，不能把周期性的 `UsageTotal` 快照当作 counter 增量；重复事件不能重复计数，写入失败不能伪造成本。未授权调用者只能看到进程级或受控聚合桶。

## Dashboard 与访问控制

发布包提供 deploy/observability/ 下的 Prometheus recording/alert rules 和 Grafana dashboard JSON。Prometheus telemetry 本身是进程级低基数聚合，不含 tenant label，因此 query adapter 固定区分两种视图：`platform` 视图只对平台运维管理员开放，显示请求/延迟/交付等进程聚合；`tenant` 视图只返回经授权的 AuditEvent usage aggregate（token/cost）和该租户的 scope 元数据，不返回跨租户的 process telemetry。普通租户只能访问自身 usage 聚合，跨租户管理员只能访问其授权租户集合；匿名或超出基数预算的维度归入 aggregate 桶。Adapter 先通过平台的 tenant authorization，再构造固定查询模板；客户端不能提交任意 PromQL 或把原始 tenant label 拼进查询。Grafana JSON 只引用 adapter 暴露的受控视图，不把 dashboard 当作授权边界。

Dashboard 展示四组面板：请求与错误率、端到端/分阶段延迟、Runner/队列/IM 交付、token/cost 与后端延迟。审计详情、原始消息和 provider 错误必须跳转到受权限保护的审计查询，不从 telemetry 反推。

## 告警初始规则

初始阈值是可调的起始值，不是容量承诺：5 分钟终态错误率 > 5%；P95 gateway/runner 延迟 > 2s；IM delivery retryable/dead-letter failure > 1%；backend P95 > 500ms。终态集合固定为 `complete|success|error|failure|canceled|timeout`，错误分子为 `error|failure|canceled|timeout`，分母只取上述终态（`started` 不进入分母）。Delivery 比率按投递尝试计数：分母为 `success|retry|failure|dead_letter`，分子为 `retry|failure|dead_letter`；dead-letter 是最终结果，retry 是当前尝试结果。token/cost 预算由同一个授权 query adapter 从 AuditEvent aggregate 计算 80%/100% 阈值，不把 tenant_id 放进 Prometheus label；如果部署方需要 Prometheus 告警，使用 adapter 发布的受控 aggregate gauge。告警 payload 只带 component、operation、channel、provider、error_class 和受控聚合标识。

## 验收台账

- [ ] HTTP/IM callback 到 Gateway、Runner、Model、Tool、Storage、IM reply 的 trace context 连续且取消安全；每个模型调用只有一个终态（含流式和创建流失败）。
- [ ] 目录指标覆盖 volume、latency、终态 errors、IM success/retry/dead-letter、tokens、cost 和 backend latency。
- [ ] 标签白名单、上述固定低基数映射、脱敏和 tenant-authorized aggregate query adapter 有负向测试。
- [ ] Prometheus/Grafana dashboard 与 alert rules 可加载，并不包含 secret、原始租户标识或无界 ID。
- [ ] no-op provider 保持默认行为；trace/metric exporter 的 shutdown 故障不阻塞业务路径。

代码阶段必须把本页所有 `[ ]` 变为有代码和测试证据的 `[x]`，并在 PR 描述中列出验证命令与 dashboard/query adapter 的授权边界。
