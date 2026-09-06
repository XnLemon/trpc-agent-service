# 包级数据库 Schema

本仓库的表定义按“领域包 + 数据库后端”归属，不再把所有表都看成
migrations/ 的无主资源。这个组织方式参考了上游
[tRPC-Agent-Go 的 Session 后端目录](https://github.com/trpc-group/trpc-agent-go/tree/main/session)、
[PostgreSQL 实现](https://github.com/trpc-group/trpc-agent-go/tree/main/session/postgres)和
[MySQL 实现](https://github.com/trpc-group/trpc-agent-go/tree/main/session/mysql)：
具体后端包携带自己的 schema 资源，迁移层负责编排。

## 目录归属

~~~text
trpcservice/
├── tenant/
│   ├── postgres/schema.sql
│   └── mysql/schema.sql
├── model/
│   ├── postgres/schema.sql
│   └── mysql/schema.sql
├── app/
│   ├── postgres/schema.sql
│   └── mysql/schema.sql
├── backend/
│   ├── postgres/schema.sql
│   └── mysql/schema.sql
├── channels/
│   ├── postgres/schema.sql
│   └── mysql/schema.sql
├── runtime/
│   ├── storage/postgres/schema.sql
│   └── queue/postgres/schema.sql
└── audit/postgres/schema.sql
~~~

每个 `schema.sql` 只拥有本包的基础表和索引，旁边的 `init.go` 暴露
`SchemaModule`、`InitDB` 和 `VerifySchema`。`schema.go` 用 `go:embed`
暴露资源。`trpcservice/schema` 提供通用的 SQL 执行、依赖排序和表存在性校验；
它不拥有任何领域表。

当前模块边界如下：

| 模块 | 表 |
| --- | --- |
| tenant | tenant、Tenant Status Change Outbox、Tenant Configuration Outbox |
| model | model_profile、Model Profile Change Outbox |
| app | agent_app、Revision、Revision Tool、App Change Outbox |
| backend | backend_profile、Binding、Backend Change Outbox |
| channels | channel_binding、Channel Change Outbox |
| runtime/storage | Session、Message、Reply、Event History、Correlation、Memory、Summary、Knowledge、Artifact、Audit Log、Vector、Object、Attachment |
| runtime/queue | Execution Queue |
| audit | Audit Event、Execution Audit Handoff |

依赖顺序是 tenant → model/app/backend → channels → runtime/audit。
App 依赖 Tenant 和 Model；Channel 依赖 Tenant 和 App；运行时表依赖 Tenant
及 Channel 的复合外键。

## Migration 边界

`migrations/0001~0016` 和 MySQL migration 现在只保留跨包行为：函数、触发器、
权限、跨表约束和后续 `ALTER TABLE`。它们不再包含 `CREATE TABLE` 或
`CREATE INDEX`。根 migration 只做历史记录和执行编排，不会在运行时重写 SQL。

启动时的顺序是：先按依赖执行各后端包的 `SchemaModule`，再执行行为 migration，
最后校验 migration history 和包级表清单。PostgreSQL 还会先初始化
`trpcservice/schema/postgres` 提供的公共角色与校验函数。

这是一次有意的 breaking change：根 migration 文件的 digest 已改变，已有环境
不能把旧的 `schema_migrations` 当作新版本历史直接复用；升级前需要按发布流程
建立新的基线或在隔离数据库重建。代码不会自动删除表或数据。未来表字段、索引
或约束变化应由所属后端包更新 schema，并配套新增 migration/验证测试。

包 schema 不由 Repository 构造函数隐式执行。Bootstrap 会显式传入 schema module；
这样保留了本地 MySQL 的权限边界：migration 账号执行 DDL/触发器，应用账号只做
DML。需要独立初始化时，也可以直接调用具体后端包的 `InitDB`。

## 增加新的后端

新增一个 SQL 后端时按以下顺序扩展：

1. 在领域后端包下增加 `schema.sql`、`schema.go` 和 `init.go`；
2. 在 `SchemaModule` 中声明表名、驱动和依赖；
3. 把跨包约束、函数、触发器和演进 SQL 放入行为 migration；
4. 增加后端 schema 的表清单、索引、`InitDB`/`VerifySchema` 和真实数据库测试；
5. 只有在后端 Repository、Bootstrap 和权限边界都实现后，才在配置中开放该后端。

当前 MySQL 只实现控制面 Repository，因此没有伪造
runtime/storage/mysql 或 MySQL runtime schema。
