# Gateway、Execution Plan 与 HTTP/SSE

> 本页把已合并的生产架构设计（PR #25，对应 Issue #24）和已完成的 Channel
> Binding 可信边界（Issue #26）收敛为 Issue #28 的可执行验收契约。文档阶段只
> 定义边界；代码阶段必须以测试证明每一项已勾选能力。

## 1. 交付边界

Issue #28 实现第一条可离线运行的网络执行链：

```text
HTTP/API principal 或 Verified Channel principal
  -> InboundMessage
  -> ExecutionPlanResolver
  -> RunnerRegistry
  -> Dispatch
  -> tRPC-Agent-Go Runner Event
  -> JSON 或 SSE
```

PR #25 的架构验收继续约束组件职责：Channel Adapter 负责协议适配和验签，Gateway
负责可信主体、快照和执行分发，Worker/Runner 只消费固定的 `ExecutionPlan`。Issue
#26 的 `VerifiedBinding` / `RoutingTarget` 是 Channel principal 的唯一可信来源；本
Issue 不重新解释请求 body/header，也不从其中拼出租户。

本 Issue 明确不实现真实 WeCom/Telegram webhook、OAuth/OIDC、KMS/Vault、Redis/SQL
持久化、生产队列、Admin API、Graph/Chain/Parallel/Cycle 全量运行时或多节点一致性。
InMemory 限流、幂等、Registry 和 Session 只证明单进程契约，不能宣称跨节点生产语义。

## 2. 可信主体与统一入站消息

### 2.1 两种 principal 不可互换

| 主体 | 来源 | 固定字段 | 禁止行为 |
| --- | --- | --- | --- |
| `ChannelPrincipal` | Issue #26 验签后的 `channels.RoutingTarget` | Tenant、Binding、App、渠道和可信 external identity | 反序列化客户端提交的 `VerifiedBinding`；接受 body/header 覆盖 |
| `APIPrincipal` | `Authenticator` 根据 API credential 返回 | Tenant、App（可选固定 revision/profile 约束）和主体 ID | 把明文 `tenant_id` 当作凭证；与 Channel principal 互转 |

主体对象必须是不可变/防御性复制的值。Handler 只能把已认证主体交给 Dispatch；
`tenant_id`、`binding_id`、`app_id`、model/backend/profile ID 出现在请求 body 或 header
时，一律视为不可信业务字段，忽略或按严格 schema 拒绝，但不能改变路由结果。

### 2.2 `InboundMessage`

统一消息至少包含：

- `content` 与显式 `content_type`；本阶段只执行 `text`。
- `external_message_id`；API 请求没有外部消息 ID 时由服务端生成独立 message ID。
- external user、conversation/chat 和可选 thread/topic 标识。
- `channel`、provider account、Binding 和其他通道元信息只能来自可信 principal。
- `request_id`、`trace_id` 和执行 Context；external message ID 不替代 request ID。

消息必须在进入 Runner 前规范化并限制 body 大小、文本长度、未知 JSON 字段和空白
身份字段。Channel 的 Runner identity 使用 Binding-aware 规则；API 使用明确的
API principal scope 和请求 conversation/user 标识，不能将两个入口混为一个字段协议。

## 3. ExecutionPlanResolver

Resolver 接口只依赖控制面 Repository 和 catalog 接口，不依赖任何 InMemory 实现：

1. 从 principal 固定的 Tenant ID 读取 active Tenant，并创建不可变
   `tenant.ConfigurationSnapshot`。
2. 读取 principal 固定的 App；要求同一 Tenant、active 且存在 current published
   Revision。
3. 读取 Revision 引用的 active Model Profile，并使用受信 catalog 校验配置。
4. 读取 Tenant 的默认 active Backend Profile，并使用受信 catalog 校验 session 能力。
5. 再次校验所有对象的 Tenant、ID、version、revision 和 content digest 关系，构造
   现有 `runtime.ExecutionPlan`。

Resolver 返回的错误只表达稳定类别，例如 `unauthenticated`、`not_found`、
`not_executable`、`configuration_unavailable`、`context_canceled`；不得泄露 Secret
ref、provider endpoint、内部堆栈或其他租户对象是否存在。每次 Repository/catalog 读取
都必须传递调用方 Context。配置更新只影响后续 Resolver 调用，已返回的 Plan 不再读
控制面“当前值”。

`ExecutionPlan.CacheKey()` 是 Registry 的唯一完整键，必须包含 Tenant、App、Revision、
Model Profile、Backend Profile 的版本和摘要；Plan、factory input 和 snapshot 不携带
Secret value 或 live client。

## 4. RunnerRegistry

Registry 持有由它创建的 Runner，借用调用方提供的 Session service、Secret Resolver、
Model Factory 等共享依赖，不在关闭时关闭借用资源。

### 生命周期契约

- `Acquire(ctx, plan)` 按完整 Plan Cache Key 查找或构造 Runner。
- 相同 key 的并发构造合并为一次；构造失败不留下半初始化条目。
- 不同 Tenant 或任一 version/digest 不同的 Plan 绝不共享 Runner。
- 返回带引用的 lease；`Release` 只减少引用，不能让 eviction 关闭仍被借用的 Runner。
- `Invalidate(key)` 只阻止新请求使用旧 Runner；等引用归零后再关闭。
- 空闲过期和容量淘汰只选择引用为零的条目；`Close` 停止新 Acquire，取消/等待有界，
  每个 Runner 最多关闭一次。
- 关闭错误不能泄露 provider endpoint 或 Secret；重复 `Close` 安全。

Registry 失效接口保留未来接入分布式配置事件的边界，但本阶段只提供进程内实现。

## 5. Dispatch

Dispatch 是与 HTTP/IM 协议无关的执行边界：

1. 校验可信 principal、规范化消息和执行 Context。
2. 生成 Binding-aware 或 API-aware Runner user/session identity。
3. Resolve 固定 `ExecutionPlan`，Acquire Registry lease。
4. 调用 `runner.Run`，以 Revision runtime policy 和请求 deadline 约束执行。
5. 将 Event 转为受控文本/状态/错误事件；不把 Repository、Secret、Plan 可变对象暴露给
   Handler。
6. 在正常完成、错误、调用方取消或 server shutdown 时，停止消费新事件、以有界时间排空
   Event channel、Release lease，并让 Registry 负责旧 Runner 的最终关闭。

请求取消必须传入 Runner。Handler 断开不能遗留 event consumer、Registry 引用或后台
goroutine；排空超时只产生脱敏的取消/关闭结果。

## 6. HTTP API

本阶段提供两个最小对话 endpoint，以及一组独立的存活/就绪 endpoint：

| Endpoint | 成功响应 | 失败/取消 |
| --- | --- | --- |
| `POST /v1/chat` | `application/json`，返回 `request_id`、最终文本和受控执行状态 | 在发送响应前写一次 HTTP status；脱敏 JSON error |
| `POST /v1/chat/stream` | `text/event-stream`，输出规范化 `message`、`status`、`error`、`done` 事件 | partial stream 后只写 SSE error/terminal，不再写第二个 HTTP status |
| `GET /healthz` | `200` 表示进程存活 | 进程无法提供存活检查时返回失败 |
| `GET /readyz` | `200` 表示 Resolver、Registry 和 Runner 构造依赖已就绪 | 依赖未加载或服务正在摘流时返回失败 |

请求使用严格 JSON decoder：未知字段、空/过大 body、非 text 内容、超长文本和缺失
可信主体/消息身份都失败关闭。服务端生成或校验 `request_id`，只接受有效 tracing
header 作为 trace 关联；任意业务字段不得伪造 `trace_id`。response 始终返回
`request_id`，但不返回 Secret、完整 provider endpoint、内部堆栈或跨租户存在性。

SSE 每个事件使用稳定的 `event:` 类型和 JSON `data:`，以明确 `done` 或 `error` 终止。
写失败立即停止发送并释放 Dispatch 资源；客户端断开、handler timeout 和 shutdown
都会取消执行 Context。

## 7. 服务生命周期与保护措施

- `cmd/trpc-service` 启动持续运行的 HTTP Server，安全默认监听地址、请求超时和关闭
  超时可由配置覆盖。
- `/healthz` 只表示进程存活；`/readyz` 检查 Resolver、Registry、Runner 构造依赖是否
  可用。依赖未加载时 readiness 返回失败，不能假装可接收流量。
- 收到 SIGINT/SIGTERM 后先摘除 readiness、停止接收新请求，再有界等待在途请求；到期
  取消剩余 Context、排空 Event、关闭 Registry/Runner，避免 goroutine 泄漏。
- 按 Tenant 固定配额实现进程内限流；`nil`/零配额、并发和窗口边界有明确测试。
- 按可信 principal + external message ID 定义 InMemory 幂等接口；重复请求返回已有
  结果或稳定冲突，不再次启动 Runner。该实现不承诺跨节点或重启后的持久化保证。

## 8. PR #25 / Issue #24 验收对齐

PR #25 已在合并 head `75d857bc5ad07ebc162c26817064532afd15a46e` 完成 Issue #24
的架构设计验收。下表把该已验收基线映射到 Issue #28 的实现边界；它不把 PR #25
的设计交付重新声称为运行时代码，也不把 Issue #28 的 InMemory 证明扩大为生产能力。

| PR #25 验收组 | 已验收的基线证据 | Issue #28 的对齐边界 |
| --- | --- | --- |
| 架构职责、控制面/数据面和部署拓扑 | `architecture.md`、架构图和部署章节 | Gateway 只编排可信主体、固定 Plan 与执行；真实部署仍不在本 Issue |
| WeCom 核心时序与 IM 协议 | `architecture.md`、`channel-binding.md` 和 WeCom/Telegram 对比 | #26 提供可信 Channel 来源；#28 不实现真实 webhook 或 IM Adapter |
| 数据模型、同步、顺序与幂等 | `data-model.md`、`ops.md` 的状态机和迁移约束 | #28 只证明单进程 InMemory 幂等/限流；不宣称持久化或跨节点语义 |
| 多后端矩阵与迁移回滚 | `backend-profile.md`、架构文档中的一致性/迁移矩阵 | ExecutionPlan 固定 Backend 版本与 digest；Redis/SQL/向量迁移仍是后续能力 |
| 治理、观测、故障恢复 | `ops.md` 的策略链、审计、trace、重试和恢复 runbook | #28 先落实错误脱敏、Context 取消、Event 排空和资源关闭；不声称生产 telemetry |
| 生产风险清单 | `ops.md` 的 11 项风险及缓解措施 | 每个代码阶段只勾选有测试证明的局部风险控制，不回填设计之外的生产承诺 |
| 核心安全与版本约束 | PR #25 checklist、#26 trusted routing、secret-free snapshot 设计 | #28 保持 principal provenance、租户隔离、完整 CacheKey 与 Secret 不出边界 |
| README、导航、渲染和 CI 验收 | 已合并 PR #25 的 README/MkDocs/CI 验证记录 | README 只跟随 #28 实际代码阶段更新，不把设计项提前标为完成 |

## 9. 下一代码阶段 ledger：Runner Registry 与 Dispatch

文档先行的 Stage 2 只覆盖进程内 Runner Registry 和协议无关 Dispatch；完成后才将
下面项目从 `[ ]` 改为 `[x]`，并把测试命令与 exact head 写入 PR ledger：

- [ ] 使用完整 `ExecutionPlan.CacheKey()` 做 Runner 查找，不能按 Tenant/App 的部分字段共享。
- [ ] 合并同 key 的并发构造，构造失败不缓存半成品，并区分借用依赖与 Registry 自有 Runner。
- [ ] 提供引用计数 lease、Invalidate、空闲/容量淘汰和有界 Close；在途请求释放前不得关闭 Runner。
- [ ] Dispatch 只接收已验证 Principal 与规范化 `InboundMessage`，生成 Binding/API-aware identity，传递 request ID 和取消 Context。
- [ ] 以脱敏的文本、状态、错误、done 事件消费 Runner Event；取消或关闭时有界排空并释放 lease。
- [ ] 用并发、跨租户、版本失效、构造失败、取消、淘汰和关闭回归测试证明上述边界。

## 10. 离线验收矩阵

使用 InMemory Tenant/Agent/Model/Backend Repository、fake Authenticator、Issue #26
fake verified binding、fake Secret Resolver、fake Model Factory、InMemory Session 和
fake Runner/Model 覆盖：

- API principal → Resolver → Registry → Runner → JSON final response。
- Verified Channel principal → Dispatch → Runner Event → SSE response。
- 两个 Tenant 使用相同 App/Profile key 时 Runner、Session 和 identity 严格隔离。
- body/header 伪造 Tenant/App/Profile/Binding 不改变可信路由。
- 相同 Plan 并发只构造一个 Runner；构造失败不缓存半成品。
- 版本更新后新请求得到新 Runner，在途旧请求正常完成后再关闭。
- 普通 timeout、SSE disconnect、Context cancel、server shutdown、Registry eviction/close。
- 限流拒绝、重复 message ID、无效/未知/过大 JSON、脱敏错误和跨租户读取失败。

代码阶段完成后，README 只能勾选实际实现并有测试支撑的持续服务、健康检查、Registry、
Gateway、普通/流式 API、限流和 InMemory 幂等能力；真实 IM、持久化幂等、生产 Secret
Manager 与多节点语义继续保持未勾选。
