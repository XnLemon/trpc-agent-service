# Issue #80：可靠性、备份与容量门禁

Issue #80 的父级验收把运行时韧性、灾备操作和容量证据放在同一个门禁中。本页记录当前仓库可验证的实现边界，不把设计建议当作已经完成的生产能力。

## 缺口 ledger

| 验收项 | 状态 | 证据 |
| --- | --- | --- |
| Model、Tool、Backend capability、IM Provider 的 timeout/retry/circuit/fallback 组合策略 | 已实现 | `trpcservice/resilience` 以及四类显式 wrapper；默认不改变旧调用方 |
| 取消、每次尝试 deadline、不可重试错误、半开探测和 bounded retry | 已实现 | `go test ./trpcservice/resilience` 与 race 测试 |
| PostgreSQL backup/restore 与灾备 rehearsal | 已实现 | `scripts/backup-restore-smoke.sh --rehearse` 和 Reliability Gates workflow |
| 发布回滚流程 | 已实现 | 下文的固定快照、灰度、回滚和恢复步骤 |
| 可重复负载测试与发布容量上限 | 已实现 | `examples/load-test`，默认租户 admission 上限为 8 并发、60 次/分钟 |
| 真实模型/IM、跨区域数据库故障演练 | 非本 Issue 的本地门禁 | 需要受控外部环境和供应商配额，不能伪造成 CI 证据 |

## 运行时韧性

`resilience.Policy` 是一个显式、可共享的依赖级策略。每次尝试都会获得一个新的 `context.WithTimeout`；退避是有界指数退避；连续失败达到阈值后进入 `open`，经过 `OpenTimeout` 只放行一个 `half_open` 探测。父 context 取消不会重试，也不会调用 fallback。

```go
policy, err := resilience.New(resilience.Config{
    Timeout: 5 * time.Second, MaxAttempts: 3,
    Backoff: 50 * time.Millisecond, MaxBackoff: time.Second,
    FailureThreshold: 3, OpenTimeout: 5 * time.Second,
    Retryable: func(err error) bool { return errors.Is(err, ErrUnavailable) },
    Fallback: func(context.Context, error) error { return nil },
})
```

策略只在显式包装时生效：

- `model.ResolveAndBuildWithPolicy` 包住 Secret Resolver 和 Model Factory。解析或构造失败会继续使用既有脱敏错误。
- `backend.NewResilientStorageFactory` 包住 capability materialization；失败尝试返回的 capability 会立即关闭，避免泄漏资源。
- `tool.NewResilientTool` 与 `Registry.ResolveWithPolicy` 只包装显式列入 `retrySafeToolIDs` 的 `CallableTool`，保留工具声明和参数边界；没有外部幂等证明的副作用工具不会自动重试。
- `outbox.Config.Resilience` 包住 IM Provider 的 `Deliver`/`Reconcile`。只有 Provider 支持 `ReplyID + SegmentIndex` 外部幂等键时，才允许对 `Deliver` 开启重试。

`bootstrap.NewFromEnvironment` 会为 model、backend、tool 和 outbox 分别创建默认策略，避免一个依赖的熔断状态阻断其他依赖；手动组装的 `bootstrap.Config` 保持原有行为，只有显式提供 `Resilience` 时才启用。`RetrySafeToolIDs` 仍默认为空，必须由调用方证明幂等后再逐项启用 Tool 重试。

Fallback 必须返回一个安全的成功结果或明确错误，不能把原始 provider 错误、凭据、用户内容或 DSN 写入响应、日志、审计或持久化状态。副作用 Tool 不应配置自动重试；应使用外部幂等键、人工审批或补偿流程。

## 备份、恢复与灾备演练

脚本依赖 PostgreSQL client 工具，不打印 DSN 或密码：

```bash
./scripts/backup-restore-smoke.sh --check
TRPC_BACKUP_DSN="$SOURCE_DSN" \
TRPC_BACKUP_FILE=/secure/backup/trpc-agent.dump \
  ./scripts/backup-restore-smoke.sh --backup
TRPC_BACKUP_FILE=/secure/backup/trpc-agent.dump \
TRPC_RESTORE_DSN="$DISPOSABLE_RESTORE_DSN" \
  ./scripts/backup-restore-smoke.sh --restore
TRPC_BACKUP_DSN="$SOURCE_DSN" \
TRPC_RESTORE_DSN="$DISPOSABLE_RESTORE_DSN" \
  ./scripts/backup-restore-smoke.sh --rehearse
```

`--rehearse` 使用 custom-format dump、`pg_restore --list` 校验和真实恢复后的 `SELECT 1` 连通性检查；脚本不会删除数据库。只有明确确认目标是 disposable database 时才设置 `TRPC_RESTORE_ALLOW_CLEAN=1`。

生产值班流程：

1. 先冻结配置发布和新租户灰度，记录当前 revision、migration digest、backup 时间和 correlation ID。
2. 从最近一次成功备份恢复到隔离数据库，校验 `schema_migrations`、租户行数、event 序号、outbox 状态、审计 digest 和随机样本。
3. 先以只读或 shadow plan 验证恢复数据，再切换新的 Backend/Profile 指针；不要把未校验的恢复库直接作为生产真相源。
4. 恢复后从 event/outbox cursor 追平增量，确认幂等键、fencing token 和未完成回复没有重复投递。
5. 记录恢复耗时、丢失窗口和校验结果；任何 checksum、租户过滤或审计失败都保持服务 not-ready 并转人工处理。

## 发布回滚

每次执行使用固定的 Tenant/App/Revision/Model/Backend snapshot。发布采用小租户灰度：先检查 readiness、migration digest、Secret capability、队列和 storage conformance，再观察 callback、execution、state commit、reply delivery 四组指标。

触发错误预算、超时、成本或 DLQ 阈值时：停止扩大灰度，把 App/Binding 指针切回最近的 published revision，主动失效未来 Runner cache，保留进行中的旧 snapshot 直到完成或取消。回滚不会撤销已发送消息、删除审计事实或重跑有副作用的 Tool。切换后重新运行 focused fault-injection、`go test -race ./...` 和健康/ready 检查。

## 容量与负载证据

没有真实供应商和硬件基准前，不发布虚假的单节点 QPS。当前可发布的保护上限来自服务 admission contract：

| 保护项 | 默认值 | 语义 |
| --- | ---: | --- |
| 每租户 in-flight execution | 8 | 进程内并发上限，超出立即返回 rate limited |
| 每租户 admission window | 60 / minute | 固定窗口上限，窗口不是跨节点配额 |
| Agent 单次 LLM calls | 16 | Revision RuntimePolicy 上限 |
| Agent 单次 Tool calls | 64 | Revision RuntimePolicy 上限 |

运行确定性容量契约和 benchmark：

```bash
go test ./examples/load-test -count=1 -v
go test ./examples/load-test -run '^$' -bench . -benchmem -count=1
go test -race ./examples/load-test -count=1
```

正式压测仍需覆盖单租户/多租户突发、同 Session 并发、模型长尾、Tool timeout、IM 429、数据库 failover、向量索引落后和滚动重启。发布容量取以下最小值：tenant concurrency/budget、Worker CPU/memory、provider quota、Session write QPS、queue throughput、IM quota 和 telemetry backpressure。

## CI 门禁

`.github/workflows/reliability.yml` 在 Pull Request、`main` push 和手动触发时运行：

- resilience wrapper 的普通和 race 契约测试；
- deterministic tenant load/capacity test 与 benchmark；
- 临时 PostgreSQL 的真实 dump/restore rehearsal 和恢复数据校验。

故障注入和提交密钥扫描分别由现有 `Fault-injection E2E` 与 `CI / Commit Secret Scan` workflow 继续执行；本 Issue 不要求连接真实模型、Telegram、企业微信或 Secret Manager。
