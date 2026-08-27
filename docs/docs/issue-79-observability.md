# Issue #79：生产可观测性、Dashboard 与告警

> 本页是 Issue [#79](https://github.com/XnLemon/trpc-agent-service/issues/79) 的文档先行契约。它承接 Issue #45 的 provider-neutral telemetry 和 Issue #54 的 tenant-scoped usage/audit；本页固定实现边界，再进入代码阶段。

## 目标与非目标

目标是让一次可信请求在 http.request → gateway.dispatch → runner.execution 下继续关联到 model.call、tool.call、storage.operation 和 channel.receive/channel.send，并提供可安全聚合的运行指标、租户授权的查询模型以及可部署的 dashboard/alert 资源。

本 Issue 不改变审计事实源，不把 Prometheus 当作成本或合规事实源，不新增 Session/Memory/Knowledge/Artifact 后端，不实现新的 IM 协议。指标导出失败只能丢弃 telemetry，不能阻塞业务提交、租约或关闭。

## Trace 合同

所有入口接受 W3C-compatible correlation（当前 HTTP 兼容 X-Request-ID/X-Trace-ID），缺失 request ID 时生成受界限 ID。子操作必须继承同一 context.Context；取消和 deadline 分别归类为 canceled/timeout。span 名称稳定且只允许下列受控属性：component、operation、status、error_class、tenant_hash、app_hash、model_family、provider、channel。

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

标签白名单在代码中集中校验；禁止 tenant_id、user_id、session_id、message_id、request_id、trace_id、完整 URL 和原文。Token/cost 来源仍是 tenant-scoped audit usage aggregate；未授权调用者只能看到进程级或受控聚合桶。

## Dashboard 与访问控制

发布包提供 deploy/observability/ 下的 Prometheus recording/alert rules 和 Grafana dashboard JSON。Dashboard 查询必须先通过平台的 tenant authorization，再把 tenant 过滤器映射到受控 datasource/query；不得让客户端把任意 tenant label 拼进 PromQL。跨租户管理员只能访问其授权租户集合，普通租户只能访问自身聚合，匿名或超出基数预算的维度归入 aggregate 桶。

Dashboard 展示四组面板：请求与错误率、端到端/分阶段延迟、Runner/队列/IM 交付、token/cost 与后端延迟。审计详情、原始消息和 provider 错误必须跳转到受权限保护的审计查询，不从 telemetry 反推。

## 告警初始规则

初始阈值是可调的起始值，不是容量承诺：5 分钟错误率 > 5%；P95 gateway/runner 延迟 > 2s；IM delivery failure 或 DLQ 增长 > 1%；backend P95 > 500ms；单租户 token/cost 超预算 80%/100%。告警 payload 只带 component、operation、channel、provider、error_class 和受控聚合标识。

## 验收台账

- [ ] HTTP/IM callback 到 Gateway、Runner、Model、Tool、Storage、IM reply 的 trace context 连续且取消安全。
- [ ] 目录指标覆盖 volume、latency、errors、IM success、tokens、cost 和 backend latency。
- [ ] 标签白名单、脱敏和 tenant-authorized aggregate 查询有负向测试。
- [ ] Prometheus/Grafana dashboard 与 alert rules 可加载，并不包含 secret 或无界 ID。
- [ ] no-op provider 保持默认行为；exporter/shutdown 故障不阻塞业务路径。

