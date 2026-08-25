# 租户审计与用量成本契约

本页是 Issue #54 的实现契约。它在运行时 telemetry 之上定义不可采样的业务审计、用量和
成本事实，并先于代码落地。`trpcservice/observability` 的 trace、metric 和 log 可以采样或在
exporter 故障时丢弃；本页定义的 mandatory audit 事件必须写入 append-only writer，不能因
trace sampling、指标基数限制或 exporter 状态而省略。

业务包只依赖平台拥有的 audit 接口，不依赖 PostgreSQL、OpenTelemetry、Prometheus 或日志
实现。审计记录不是请求/响应 payload 的副本，也不是 provider 错误日志。

## 完成边界

Issue #54 包含：

- 版本化 `AuditEvent`、`Usage`、append-only `Writer`、租户查询和聚合契约；
- 并发安全的 InMemory 实现及共享 conformance suite；
- 有序 PostgreSQL migration、Repository、租户约束、重复写和并发测试；
- Admin/control-plane、Gateway/Runner、Tool 决策、IM ingress、reply delivery、重试和
  dead-letter 事件生产者；
- 按 tenant/app/channel/model 的受控 usage/cost 聚合；
- failure、retention、masking、access control 和 repair 运维说明。

不包含 Dashboard/告警 UI、新 IM 协议、真实 KMS/Vault、替换现有 runtime outbox，或对外部
provider 的 exactly-once 承诺。

## 事件模型

### 公共信封

所有事件使用 `schema_version = 1`，并包含以下 secret-free 字段。空值表示该事件不适用，
不能用其他租户或平台默认值补齐。

| 字段 | 约束 | 说明 |
| --- | --- | --- |
| `schema_version` | 固定为 `1` | 不兼容变更必须递增版本 |
| `event_id` | 租户内稳定、非空 | 重试和并发去重键的一部分 |
| `event_type` | 受控枚举 | 稳定的事实名称，不使用自由文本 |
| `tenant_id` | 必填 | 写入、读取、聚合和授权边界 |
| `channel` | 受控协议名，可空 | 例如 `api`、`telegram` |
| `user_id` / `session_id` | 可空引用 | 只进入受权审计查询，不进入 metric label |
| `agent_app_id` / `revision` | 可空 | 执行使用固定 plan 中的版本 |
| `model_profile_id` | 可空 | 不保存 endpoint、DSN 或 secret reference |
| `tool_name` | 可空、规范化 | 不保存参数、响应或外部 URL |
| `decision` | 受控枚举 | `allow`、`deny`、`approval_required` 等 |
| `latency_ms` | 非负，可空 | 事件对应操作的墙钟耗时 |
| `error_type` | 受控稳定类别，可空 | 禁止 provider 原始错误文本 |
| `request_id` / `trace_id` | 可空关联 ID | trace 被采样掉时仍保留关联值 |
| `correlation_id` | 可空；控制面必填 | 一次管理变更/repair 的业务关联 |
| `actor_type` / `actor_id` | 成对出现；控制面必填 | 可信主体，不从消息正文推断 |
| `reason` | 控制面和人工决策必填 | trim 后 1..1000 rune，禁止控制字符 |
| `previous_version` / `next_version` | 成对出现 | 控制面变更要求 `next = previous + 1` |
| `occurred_at` | UTC、非零 | 事实发生时间，不以写入时间替代 |

`event_type` 至少覆盖：

- `control_plane.changed`；
- `execution.started`、`execution.completed`、`execution.failed`、
  `execution.canceled`、`execution.timed_out`、`execution.fallback`；
- `tool.allowed`、`tool.denied`、`tool.approval_required`；
- `im.authorization_allowed`、`im.authorization_denied`、`im.ingress_accepted`、
  `im.ingress_duplicate`；
- `im.delivery_sent`、`im.delivery_retry_scheduled`、
  `im.delivery_dead_lettered`、`im.delivery_reconciled`；
- `budget.rejected`、`content.redacted`。

### 用量与成本

一次模型、工具或完整执行可以携带 `usage`。没有供应商事实时字段保持 `nil`，不能猜测或用
零表示“已知没有消耗”。

| 字段 | 约束 | 说明 |
| --- | --- | --- |
| `input_tokens` / `output_tokens` | 非负，可空 | provider 返回的 token 事实 |
| `model_cost_minor` / `tool_cost_minor` | 非负，可空 | `currency` 的最小货币单位 |
| `currency` | ISO 4217 大写三字符 | 任一金额存在时必填 |
| `budget_used_tokens` / `budget_used_minor` | 非负，可空 | 此事件造成的预算增量，不是可变累计值 |
| `execution_result` | 受控枚举 | `success`、`failure`、`canceled`、`timeout`、`rejected` |
| `provider` / `model` | 规范化、secret-free | 供应商和模型身份，不保存 endpoint |

金额和 token 只追加为事实。月度累计值由事件聚合产生，不能通过并发更新一个 tenant counter
作为唯一真相。不同 `currency` 不相加；调用方必须分别查询或在审计系统外使用明确汇率。

### 兼容性与规范化

- v1 读取器忽略未知的可选字段，但拒绝未知 `schema_version`、`event_type`、`decision`、
  `execution_result` 和 `error_type`；
- event ID、租户 ID 和关联 ID trim 后必须非空且不得包含控制字符；
- writer 在持久化前验证完整事件并生成 canonical digest；
- 已存在的 `(tenant_id, event_id)` 若 digest 相同，返回原记录和 `duplicate=true`；digest 不同
  返回 conflict，绝不能覆盖旧事实；
- 事件按 `occurred_at,event_id` 稳定排序；跨进程到达顺序不是因果顺序，因果关系使用
  `correlation_id`、request/trace ID 和版本字段表达。

## Writer、读取与聚合接口

目标 Go 边界保持窄接口：

```go
type Writer interface {
    Append(context.Context, Event) (AppendResult, error)
}

type Reader interface {
    Get(context.Context, eventID string) (Event, error)
    List(context.Context, Query) ([]Event, error)
}

type Aggregator interface {
    AggregateUsage(context.Context, UsageQuery) ([]UsageTotal, error)
}
```

这些接口实例在构造时绑定一个独立可信的 tenant scope；scope 来自已经验证的
`gateway.Principal`、`admin.Principal` 或 trusted bootstrap configuration，不能从 `Event`、
`Query` 或 HTTP/message payload 推导。`Reader`、`Query` 和 `UsageQuery` 不接受 tenant ID；
Repository 始终注入绑定 scope。`Append` 保留 `Event.TenantID` 作为持久化事实，但它必须与绑定
scope 相同，否则在访问存储前返回 tenant-scope error。业务代码不能获得可在调用时切换 tenant
的通用 Store。

`UsageQuery` 允许选择 app/channel/provider/model 作为 group-by，但不接受
user/session/message/request/trace、任意 URL 或正文维度。Admin 跨租户报表必须在此接口之外
先逐租户授权，再分别构造 tenant-bound Reader 并分页；不能给普通 Worker 一个全表读取方法。

InMemory 与 PostgreSQL 必须运行同一 conformance suite，覆盖：

- 缺 tenant 和不合法枚举被拒绝；两个 Store 共享同一 backing storage 时，通过 tenant A
  Store append 一个 `TenantID=B` 的事件必须在访问 backing storage 前失败，tenant A Store
  也不能 get/list/aggregate tenant B 的记录；
- defensive copy，调用方不能修改已保存事实；
- 相同重复写幂等，不同内容使用相同 ID 冲突；
- 多 goroutine/连接同时写不同事件不丢失，同时写同一事件只有一个新记录；
- context cancel 在提交前不产生记录；提交结果不因随后取消而被报告为未提交；
- 聚合按时间窗及 tenant/app/channel/provider/model 隔离。

## PostgreSQL 持久化边界

有序 migration 新增 `audit_event`，主键为 `(tenant_id, event_id)`，所有索引以
`tenant_id` 开头。事件字段使用类型/检查约束，`usage` 数值列使用 nullable non-negative
整数；不保存自由 JSON payload。migration 另建 `execution_audit_handoff` 作为可恢复的投影
outbox：它不是最终 AuditEvent，只有受 fence/状态约束的 reserve/finalize/repair 入口可以修改；
一旦 projected 就不能改变 terminal payload。索引至少支持：

- `(tenant_id, occurred_at, event_id)` 审计时间线；
- `(tenant_id, agent_app_id, occurred_at)`；
- `(tenant_id, channel, occurred_at)`；
- `(tenant_id, model_profile_id, occurred_at)`。

运行时角色没有 audit 表的直接 DML 权限。migration 提供 tenant-bound 写入/读取入口，并以 RLS
或等价数据库策略将当前 trusted scope 与行 `tenant_id` 比较；Repository 仍在调用 SQL 前比较
`Event.TenantID` 与自己的绑定 scope。写入入口只允许 `INSERT`，读取入口只返回绑定 tenant，
没有运行时 `UPDATE`、`DELETE`、`TRUNCATE` 路径。Repository 使用参数化 SQL，并将重复键后的
digest 比较放在同一事务/连接中。数据库集成测试必须用 tenant A scope 尝试写入和读取 tenant B，
证明即使绕过 Go 的 event mismatch 检查也会被数据库拒绝。migration owner 负责保留清理：线上
writer 不获得删除权限。未来 WORM/hash-chain 归档是可选增强，不能被描述为当前数据库已提供
外部不可篡改证明。

## 提交与失败策略

强制审计失败不能被吞掉或仅写 telemetry。策略按事实来源区分：

1. 当前 PostgreSQL 的 model/backend/app/binding 变更和 tenant status 变更已在同一事务写入
   metadata-complete `*_change_outbox`，可作为 durable compliance handoff。当前 tenant create
   没有 outbox，tenant configuration outbox 也缺少 actor/reason/correlation；它们尚不满足本
   契约。PostgreSQL 实现阶段必须先扩展 Admin 输入、Repository 函数和 migration，使这两类
   mutation 在同一事务写入 metadata-complete handoff，事务任一部分失败则整体回滚。不得用
   mutation 提交后的 best-effort Audit append 填补该缺口。完成后 projector 才能按稳定 source
   identity 幂等追加 `control_plane.changed`；Audit writer 暂时不可用时，API/worker 暴露
   backlog/repair 状态，不能宣称审计已投影。
2. InMemory 控制面没有 durable handoff。配置了 mandatory Audit writer 后，append 失败必须
   返回稳定 `audit_write_failed`；调用方不能收到成功。响应可能是“变更已提交但未确认”，
   因此重试依赖领域 expected-version/correlation ID，运维必须 repair 而不能盲目重放。
3. Gateway admission、Tool allow/deny/approval、IM authorization 和 budget rejection 等执行前
   决策必须在产生被决策的外部副作用前 append。writer 失败时取消尚未发生的副作用并返回
   稳定 `audit_write_failed`。
4. Runner terminal outcome 和 usage/cost 只能在执行后确定，使用独立于 Channel
   `message_event` 的 durable `execution_audit_handoff`；普通 API 和 SSE 也必须覆盖。Dispatcher
   在调用 Runner 前以 request/event ID、tenant、channel、user/session、app/revision、model
   profile 和 correlation 字段创建 `pending` handoff。Runner 终止后先把实际 result、稳定错误
   类别、latency 和可获得的 usage/cost finalize 到该 handoff，再投影 append-only AuditEvent。
   普通响应不得在 finalize 前返回成功；SSE 可以继续实时发送非终态 chunk，不能为了审计缓冲
   整个 stream，但只有 finalize 成功后才能发送成功 terminal/done。finalize 经有界重试仍失败时，
   stream 发送稳定 `audit_write_failed` terminal，handoff 保持 pending/repairable，不能把已经
   发生的模型调用伪装成成功或重新执行。Channel execution commit、回复物化和 provider delivery
   同样在 terminal handoff durable 后继续；已有 `message_event`、`reply_outbox` 或 receipt 仅作
   关联事实，不能代替缺失的 usage 字段。修复器按稳定 ID finalize/project，不重跑模型、Tool 或
   结果不明的发送；无法从 provider reconciliation 恢复的 usage 保持未知并追加
   `audit_incomplete` repair 事实，不能猜测为零。
5. telemetry exporter 故障不影响上述判断；Audit writer 故障也不能把成功变成一个伪造的
   deny/allow 事件。只记录实际发生的结果。

执行 handoff 测试必须覆盖普通 API 与 SSE：reserve 失败时 Runner 不启动；terminal finalize
失败时不出现成功 terminal/done 且 pending 可见；SSE 已发送的非终态 chunk 不被撤回或重复；
取消/timeout 使用各自结果；相同稳定 ID 重试不重复；进程在 reserve 后、finalize 前重启时 repair
能发现 incomplete execution，且任何 repair 都不会再次调用 Runner。

每个生产者使用确定性 event ID，例如来源表/状态转换的稳定 ID 与目标状态的组合。retry、
lease recovery 和 projector restart 因而不会创建重复事实。不存在外部 exactly-once 保证：
数据库只保证自己的 append/idempotency，provider side effect 仍遵循 reply outbox 的
reconcile 规则。

## 生产者映射

| 路径 | 事件与稳定来源 | 关键规则 |
| --- | --- | --- |
| Admin/control plane | `control_plane.changed`，领域 change outbox | actor/reason/correlation/前后版本必填 |
| Gateway admission | budget reject、IM auth allow/deny、ingress accepted/duplicate | 身份来自可信 Principal/Binding，不从正文取值 |
| Runner execution | durable execution handoff -> started + 一个 terminal outcome | 覆盖 API/SSE/IM；固定 app revision/model profile；成功 terminal 在 finalize 后发送 |
| Tool policy | allow/deny/approval required | tool 名称和 decision；不保存参数/结果 |
| redaction/fallback | redacted/fallback | 只保存策略类别，不保存被删内容或 provider error |
| reply outbox | sent/retry/dead-letter/reconciled | event/reply/segment 派生确定性 ID；保持 fence 语义 |

现有代码没有完整 Tool policy/approval、模型 fallback 或按 provider 返回 token/cost 的生产路径。
Issue #54 为已存在的稳定路径接入 producer，并提供这些决策可调用的审计 hook；不得为了制造
事件而伪造尚未发生的 Tool、fallback 或 usage 事实。对应路径未来调用 hook 时沿用本契约。

## 脱敏与访问控制

validator 拒绝任何包含以下内容的字段值：Authorization/Bearer、API key/token/secret、DSN、
`secret_ref`、完整 HTTP(S) URL、换行控制字符，以及 provider 原始错误。`user_id` 和
`session_id` 是允许的受控引用，但消息正文、prompt、Tool 参数/结果、provider response、
webhook body 和完整 URL 在 schema 中没有字段，不能塞入 `reason` 或 `error_type`。

读取要求调用方先有明确 tenant scope。普通 runtime 只能 append；tenant auditor 可读取自己的
租户；跨租户运维使用独立管理身份并产生自身的 control-plane audit。应用层返回 defensive
copy，HTTP 错误只暴露稳定类别。

## 保留、归档和运维

`tenant.audit_retention_days` 决定在线查询保留窗，不决定事件是否写入。清理任务由受控 owner
执行，逐租户记录 cutoff、删除/归档数量、operator 和 correlation ID；运行时 writer 无权清理。
有合规冻结时暂停该租户清理。需要超过在线窗口的 WORM/object archive 由独立导出任务完成，
并记录 manifest digest；本 Issue 不宣称实现真实 WORM。

运维至少监控：writer append failure、projector backlog age/count、conflict count、repair count、
retention lag 和聚合查询失败。指标只使用 component/operation/status/error_class/channel/provider
等低基数标签；tenant/app/model 的详细成本通过受权聚合查询获取，不直接进入 Prometheus label。

故障恢复顺序：停止产生不可审计的新副作用；确认 source fact 是否 durable；按稳定 event ID
重放 projector/append；核对数量和 digest；恢复流量。不得通过删除冲突行、覆盖旧事件或重跑
结果不明的外部副作用来“修复”审计。

## Issue #54 ledger

- [x] 文档：schema、版本、事件目录、失败策略、幂等、保留、脱敏、访问控制和运维边界。
- [x] 契约：AuditEvent/Usage/Writer/Reader/Aggregator 与兼容性、redaction 测试。
- [x] InMemory：append-only writer、租户隔离、defensive copy、并发/重复 conformance。
- [ ] PostgreSQL：有序 migration、Repository、权限、租户索引、并发/重启 conformance。
- [ ] Admin/control-plane producer 与 durable change-outbox projector。
- [ ] Gateway/Runner durable execution handoff、terminal outcome、budget、redaction/fallback 和 Tool policy hook。
- [ ] IM authorization/ingress 与 reply delivery/retry/dead-letter producer。
- [ ] tenant/app/channel/provider/model usage/cost 聚合和低基数指标边界。
- [ ] writer failure、retry、cancel、duplicate、secret/provider-error 负向测试。
- [ ] 全仓 test/race/vet/build、MkDocs strict、GitHub CI 与最终最新 HEAD LGTM。
