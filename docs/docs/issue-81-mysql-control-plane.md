# Issue #81：MySQL 控制面 Repository 与迁移契约

本页是 Issue #81 的先行设计和实现边界。它把 MySQL 适配器与现有
PostgreSQL 控制面之间的可观察行为固定下来：租户、Agent App/Revision、Model
Profile、Backend Profile 和 Channel Binding 的领域接口不变，租户隔离、乐观锁、
生命周期、Outbox 事件和错误分类也不因数据库驱动切换而改变。

## 目标与非目标

目标是让同一套控制面 API 可以选择 PostgreSQL 或 MySQL，并满足：

- 五类控制面 Repository 都实现各自领域接口；
- 所有读写都带显式 `tenant_id`，跨租户引用由复合主键/外键和 Repository 双重校验；
- 发布、回滚、状态迁移和配置替换在一个事务内提交，事件 Outbox 与主表原子可见；
- 行锁加乐观版本检查，重复写入、版本冲突、唯一冲突和非法状态迁移映射到稳定领域错误；
- Bootstrap 根据受信配置选择驱动，重启后能从同一数据库重新发现控制面对象；
- SQL、DSN、密码和 Secret 值不进入领域错误、日志、trace、执行快照或缓存键。

本 Issue 不实现运行时 Session/Memory/Knowledge/Artifact 适配、Redis 迁移、无状态
Worker、Dashboard、KMS/Vault 或新的 Admin API；这些能力只消费本页定义的控制面契约。

## 驱动与配置边界

Repository 继续使用 `database/sql` 的 `*sql.DB`，因此调用方可以复用连接池、Context
取消和现有的所有权约定。MySQL 适配器由独立的 `tenant/mysql`、`agent/mysql`、
`model/mysql`、`backend/mysql` 和 `channels/mysql` 包提供；实现细节不泄漏到领域接口。

生产 Bootstrap 使用显式驱动选择：

```text
TRPC_CONTROL_PLANE_DRIVER=postgres | mysql
TRPC_POSTGRES_DSN=postgres://...       # driver=postgres
TRPC_MYSQL_DSN=user:password@tcp(host:3306)/db?parseTime=true&charset=utf8mb4
```

默认仍为 PostgreSQL，已有 `TRPC_POSTGRES_DSN` 配置和 API 保持兼容。`mysql` 模式只
读取 `TRPC_MYSQL_DSN`；缺失驱动、DSN、迁移版本或连接 Ping 失败时在绑定 HTTP 端口前
fail-closed。DSN 只存在于 Bootstrap 的短生命周期配置中，不能写入运行时快照、Repository
错误或 telemetry。测试可以传入已经打开的 `*sql.DB`，不要求 Repository 自己关闭借用的
连接池；Bootstrap 只有在 `OwnDB=true` 时负责关闭它。

## SQL 语义映射

| PostgreSQL 契约 | MySQL 8.0 实现 | 兼容约束 |
| --- | --- | --- |
| `$1` 参数 | `?` 参数 | 只由 MySQL 包生成，禁止拼接用户输入 |
| `public.table` | 当前数据库中的 `table` | DSN 选定的数据库是唯一控制面 schema |
| `JSONB` | `JSON` | 写入前用领域校验，读取后做完整 JSON 解码 |
| `TIMESTAMPTZ` | `DATETIME(6)` | 应用层统一使用 UTC；DSN 启用 `parseTime=true` |
| `BIGINT GENERATED ...` | `BIGINT AUTO_INCREMENT` | 事件 ID 在事务内由 `LastInsertId` 读取 |
| `RETURNING` | `INSERT/UPDATE` 后按同一事务重新 `SELECT` | 不暴露中间状态 |
| `FOR UPDATE` | `SELECT ... FOR UPDATE` | 仅在事务内使用；Context 取消释放锁 |
| PostgreSQL advisory lock | `GET_LOCK`/`RELEASE_LOCK` | 仅迁移历史使用，连接固定在同一 `sql.Conn` |
| `SECURITY DEFINER` 函数 | 最小数据库账号 + 事务内受控 DML | 领域校验和显式租户谓词不可省略 |

MySQL 连接必须使用 InnoDB、`REPEATABLE READ`（或更严格级别）和 UTC session time
zone。迁移脚本不依赖 `public` schema、PostgreSQL 函数、正则运算符或 `RETURNING`。
JSON 字段仍只接受受信 Repository 产生的无密钥对象；`secret_ref` 是唯一可以持久化的
凭据引用。

## Repository 事务与隔离契约

五个适配器的公开方法与 PostgreSQL 版本完全相同。每个方法都先检查 `ctx.Err()`，再
把 Context 传给 `BeginTx`、`QueryContext`、`QueryRowContext` 和 `ExecContext`。

### Tenant

- `Create` 在同一事务插入根行并重新读取规范化快照；MySQL `CreateFirst` 使用同一
  `sql.Conn` 上的 `GET_LOCK('trpc-agent-service:first-tenant', timeout)`，提交或回滚后
  必须释放锁。
- `UpdateConfiguration`、`TransitionStatus` 先按 `tenant_id ... FOR UPDATE` 读取，比较
  `version`，再以 `version = expected` 更新；状态 Outbox 与根行同一事务。
- `Count` 只用于首次 Admin 授权边界，错误映射为稳定 `ErrStorage`。

### Agent App/Revision

- App 根行先锁定；创建 Revision 的 `MAX(revision)+1` 在该锁内计算，避免同一 App 并发
  分配相同 revision。
- Draft 更新只允许 `state=draft`，发布前同时锁 App、Draft 和 Tenant；发布事务冻结
  Revision、移动 current pointer 并写入 Change Outbox。
- 回滚只能指向同租户已有的 published Revision；已发布内容不可更新或删除。

### Model 与 Backend Profile

- 完整配置替换先执行领域 `Prepare*Change`，再锁定 Profile 行并比较版本。
- Backend binding 使用单独表，替换时在同一事务删除旧集合、插入新集合并写 Outbox；读取
  后重新通过受信 Provider Catalog 校验，不能把未知 provider 带入执行快照。

### Channel Binding

- Binding 的稳定身份、`secret_ref` 和非密协议配置按租户保存；更新和状态迁移使用
  `tenant_id + binding_id + version`。
- active 的 `(channel, public_route_key_digest, provider_account_id)` 唯一约束保持与
  PostgreSQL 一致；候选索引只返回不含 Tenant/Secret 的短期上下文。
- Candidate capability 继续是进程内一次性消费；重启后由持久化 version/digest 校验
  fail-closed。候选缓存不是跨节点状态，也不能替代数据库租户隔离。

所有数据库错误先在 MySQL storage helper 中按 `1062 duplicate`、`1213/1205
conflict/deadlock`、`1451/1452 foreign-key`、`1264/1406 invalid` 等类别归一化；错误
文本、连接地址和驱动诊断不会向 Gateway 暴露。`context.Canceled` 与
`context.DeadlineExceeded` 原样保留。

## MySQL Migration 与权限

MySQL 维护独立的有序 migration 集合和 `schema_migrations` 历史表，版本号与 PostgreSQL
保持一致但摘要分别计算。每次迁移：

1. 在固定连接上执行 `GET_LOCK`，检查历史摘要和连续版本；
2. 在事务中执行单个 migration，成功后写入版本、文件名、SHA-256 和 UTC 时间；
3. 提交后再执行下一版本；失败回滚并保留可诊断的稳定 `ErrMigration`；
4. `Verify` 只读校验，不创建或修改任何对象。

迁移至少创建 `tenant`、`agent_app`、`agent_app_revision`、`agent_app_revision_tool`、
`model_profile`、`backend_profile`、`backend_profile_binding`、`channel_binding` 及各自
Change Outbox 表，并保留 `runtime_*`/audit 表所需的同租户复合键形状。表和索引使用
`utf8mb4`、InnoDB；外键显式包含 `tenant_id`。迁移账号、控制面写账号和运行时读账号
分离，应用连接不拥有任意 schema DDL 权限。

MySQL 没有 PostgreSQL 的 `SECURITY DEFINER` 函数边界，因此安全性由三层共同保证：

1. 应用 Repository 的领域校验、显式租户谓词和事务锁；
2. 数据库账号只授予需要的表/列权限；
3. migration/Bootstrap 在启动时验证版本和必需索引，不满足即 readiness=false。

## 验证矩阵

| 层级 | 证据 |
| --- | --- |
| 契约 | 五类 MySQL Repository 编译实现领域接口，零值、nil DB、取消和错误分类测试 |
| SQL 单元 | `sqlmock` 覆盖 `?` 参数、事务提交/回滚、版本冲突、重复键、跨租户谓词和防御性副本 |
| Migration | `MYSQL_CONTROL_PLANE_MIGRATION_TEST_DSN` 指向干净 MySQL 8 服务，执行全量 migration、Verify、摘要不匹配和重启读取 |
| Repository 集成 | `MYSQL_CONTROL_PLANE_TEST_DSN` 执行五类 Repository 的创建、更新、发布/回滚、生命周期、候选消费和双租户隔离 |
| 并发/race | MySQL 服务上的 optimistic-lock、同 App revision、候选消费和 Context 取消测试；本地继续运行 `go test -race ./...` |
| Bootstrap | `TRPC_CONTROL_PLANE_DRIVER=mysql` 选择 MySQL；未知驱动、缺 DSN、迁移失败和重启 rediscovery 均 fail-closed |

未设置 MySQL DSN 时，live 测试必须显式 `Skip`，不能把 skip 记为 MySQL 证据。CI 提供
独立 MySQL 8 服务运行 migration、Repository 和 race smoke；PostgreSQL 现有 job 不变。

## Issue ledger

| 条目 | 状态 |
| --- | --- |
| Tenant、Agent、Model、Backend、Channel MySQL Repository | 待代码阶段 |
| 事务、乐观锁、生命周期、Outbox 和租户隔离语义 | 待代码阶段 |
| MySQL migration、摘要校验、权限和重启恢复 | 待代码阶段 |
| Bootstrap 驱动选择与错误脱敏 | 待代码阶段 |
| MySQL unit/integration/race 测试及 CI 服务 | 待代码阶段 |

