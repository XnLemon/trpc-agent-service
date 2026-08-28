# Issue #76：无状态 Worker、共享队列与迁移切换

本页是 Issue #76 的 docs-first 合约和实现 ledger。目标是让 Gateway 只负责鉴权、
限流和投递，Worker 只消费不可变执行任务；所有跨节点状态都落在共享的耐久后端。
本 Issue 不改变已有 `RuntimeStore`/Reply Outbox 状态机，也不把 InMemory 声称为生产
耐久存储。

## 边界与角色

```mermaid
flowchart LR
  IM[IM / HTTP] --> G[Stateless Gateway]
  G --> Q[(Durable execution queue)]
  Q --> W1[Worker A]
  Q --> W2[Worker B]
  W1 --> S[(Shared Session / Memory / SQL)]
  W2 --> S
  W1 --> O[(Reply Outbox)]
  W2 --> O
  A[Migration Tool] --> S
  A --> D[(Destination backend)]
```

Gateway 不保存 session 粘性，也不能由请求体选择租户；它把已经验证的
`tenant_id`、`agent revision`、`backend profile` 和 `traceparent` 编入任务。Worker
从队列领取任务后在 `context.Context` 中运行 Runner，租约和 fencing token 保护执行
结果。租约过期时只能由新 fence 的 Worker 进入重试或对账，旧 Worker 的提交必须被
拒绝。Reply Outbox 继续负责异步 IM 投递，避免执行队列和供应商重试耦合。

## 执行队列契约

`trpcservice/runtime/queue` 提供协议中立的 `Store` 和 `Worker`：

- `Enqueue` 以 `(tenant_id, task_id)` 幂等；相同 payload 返回已有任务，冲突返回
  `ErrConflict`。
- `Claim` 原子地选择 `queued`、到期 `leased` 或 `retryable` 任务，递增
  `fencing_token`，设置 `lease_owner` 和 `lease_expires_at`。
- `Complete`、`Retry`、`Fail` 必须携带 owner + fence；过期或旧 fence 返回
  `ErrConflict`，不能覆盖新 Worker 的结果。
- 重试由 `NextAttemptAt` 和有限的指数退避驱动；不可重试错误或超过上限进入
  `failed`（死信），保留脱敏的错误类别。
- `Worker.Run` 只由创建者拥有；`Close` 可重复调用，会取消派生 context、停止领取
  新任务并等待正在运行的 handler，保证不会向已关闭的 channel 发送。

状态转换：

```text
queued -> leased -> completed
queued -> leased -> retryable -> leased
leased -> failed
leased (lease expired) -> leased  (new fencing token)
```

队列记录必须显式带租户键。PostgreSQL 使用复合主键、`FOR UPDATE SKIP LOCKED` 和
条件更新实现跨节点竞争；InMemory 实现只用于测试/本地，借助共享 Backend 模拟多节点
可见性。生产 Worker 不需要 sticky session，只要 Session、Memory、Queue 和 Outbox
使用同一共享后端。

## 迁移、双写与切换

`trpcservice/runtime/migration` 将迁移拆成可重放阶段，每一步都按租户隔离并产生
`Report`：

1. **copy**：读取源快照，按 `(tenant_id, kind, key)` 排序写入目标；重复执行安全。
2. **dual-write**：在切换前由应用同时写 source/destination，记录单调 watermark。
3. **catch-up**：从 watermark 继续增量，直到 source/destination 的 watermark 相等。
4. **validate**：按规范化 JSON 计算 SHA-256 checksum，比较记录数、字节数和 digest；
   任一租户失败都阻止切换。
5. **cutover**：原子地把租户路由标记为 destination，并保留旧 source 的只读窗口。
6. **rollback**：只允许在 cutover 后且未发生 destination-only 写入时回退；回退前
   再次校验 checksum，防止静默丢写。

Redis → SQL 迁移适合 Session/Event/Queue：先全量 copy，再以 Redis stream/watermark
追平，最后按租户切读。向量库迁移不复制 provider 内部 id，而是以
`tenant_id + source + document_id + version` 重建并校验；对象存储只迁移对象字节和
ETag，元数据事务仍由 SQL 负责。迁移工具不会把 secret、原始消息或 DSN 写入报告。

## 容量与故障验收

容量模型使用可观测的队列深度、claim 延迟、每租户并发和 Session/IM QPS。上线前至少
验证：两个 Worker 并发领取同一租户任务只有一个有效 fence；持有旧 lease 的 Worker
提交被拒；取消和 `Close` 不泄漏 goroutine；IM 重复投递只产生一个 task；迁移 checksum
失败不切换，切换后 rollback 恢复原路由。压测应覆盖峰值 callback、慢模型和 SQL 短暂
不可用，记录 p95/p99 延迟与恢复时间。

## Issue ledger

| 项目 | 阶段 | 证据 | 状态 |
| --- | --- | --- | --- |
| 无状态 Gateway/Worker 角色和共享后端边界 | 文档 | 本页角色与拓扑 | ✅ |
| Durable queue lease/fencing/retry/shutdown | 代码 | `runtime/queue` 契约与测试 | ⏳ |
| Redis/SQL 与向量迁移、校验、切换、回滚 | 代码 | `runtime/migration` 契约与测试 | ⏳ |
| copy、dual-write、catch-up、checksum 工具 | 代码 | 迁移报告和阶段测试 | ⏳ |
| Session/IM 容量与故障测试 | 代码 | 队列/迁移并发、取消测试 | ⏳ |
| migration DDL 与权限 | 代码 | `0013_execution_queue.up.sql` 和 migration 测试 | ⏳ |

完成代码阶段后，本表与 PR 描述同步；未实现的生产 provider、分布式锁和压测环境不
会被标记为已交付。
