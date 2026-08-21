# 数据模型

> 设计分阶段推进：本阶段完成 `tenant` 根模型；其余核心表（`agent_app`、`backend_profile`、`channel_binding`、`session`、`message_event`、`memory`、`summary`、`audit_log`）只保留边界和顺序占位。

## Tenant 根模型

### 建模决策

`tenant` 是配置、数据、权限、密钥、审计和成本的顶层隔离键，但保持为窄根表。它只保存身份、生命周期、租户级限额、审计控制、默认引用和并发版本；应用、模型、工具权限、IM 通道、后端连接参数和密钥分别由关联表或 Secret Manager 管理。

这样拆分有三个原因：

- 一个租户可以拥有多个 Agent 应用、通道绑定和后端档案，它们需要独立发布、灰度、回滚和停用。
- 根表字段会在 Gateway 鉴权和调度路径高频读取，窄表便于缓存和版本失效；不把低频配置 JSONB 载入每次请求。
- 关联表可以通过外键、租户复合唯一键和独立状态表达完整性，避免 JSONB 或仅靠 key prefix 造成跨租户引用。

`tenant_id` 是不可变的内部隔离键；`tenant_key` 是规范化且不可变的机器键；`display_name` 只用于展示，可以修改，不能用于路由或会话命名空间。

### PostgreSQL DDL

下面的 DDL 是根表最小实现。默认引用的复合外键需要在后续关联表创建后追加，见下方完整性约束。

```sql
CREATE TABLE tenant (
    -- 身份：ULID 可排序，但不应暴露为可枚举的业务编号
    tenant_id       TEXT PRIMARY KEY
                    CHECK (tenant_id ~ '^t_[0-9A-HJKMNP-TV-Z]{26}$'),
    tenant_key      TEXT NOT NULL UNIQUE
                    CHECK (tenant_key = lower(tenant_key) AND length(tenant_key) BETWEEN 2 AND 64),
    display_name    TEXT NOT NULL
                    CHECK (length(btrim(display_name)) BETWEEN 1 AND 200),

    -- active 接收新请求；suspended 暂停新执行；disabled 为终态
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'suspended', 'disabled')),

    -- NULL = 不设置上限；0 = 有效的零额度/零速率
    rate_limit_rpm             BIGINT,
    max_concurrent_executions  BIGINT,
    monthly_token_budget       BIGINT,
    monthly_spend_limit_minor  BIGINT,
    billing_currency            CHAR(3),
    CHECK (rate_limit_rpm IS NULL OR rate_limit_rpm >= 0),
    CHECK (max_concurrent_executions IS NULL OR max_concurrent_executions > 0),
    CHECK (monthly_token_budget IS NULL OR monthly_token_budget >= 0),
    CHECK (monthly_spend_limit_minor IS NULL OR monthly_spend_limit_minor >= 0),
    CHECK (monthly_spend_limit_minor IS NULL OR billing_currency IS NOT NULL),
    CHECK (billing_currency IS NULL OR billing_currency ~ '^[A-Z]{3}$'),

    -- 合规审计和可观测性采样是两套策略
    audit_retention_days  INT NOT NULL DEFAULT 90
                          CHECK (audit_retention_days > 0),
    log_masking_level     TEXT NOT NULL DEFAULT 'basic'
                          CHECK (log_masking_level IN ('none', 'basic', 'strict')),
    trace_sampling_rate   REAL NOT NULL DEFAULT 1.0
                          CHECK (trace_sampling_rate BETWEEN 0 AND 1),

    -- 关联表创建后以 (tenant_id, id) 复合外键约束归属
    default_agent_app_id       TEXT,
    default_backend_profile_id TEXT,

    -- Admin API 用乐观锁；从 1 开始，避免 0 同时表示“未初始化”
    version         BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 由数据库触发器或每次 UPDATE 语句维护 updated_at。
-- UPDATE ... WHERE tenant_id = $1 AND version = $2
--   SET ..., version = version + 1, updated_at = now();
-- 受影响行数为 0 时返回 optimistic_conflict，不覆盖其他管理员的更新。
```

金额使用平台统一的最小货币单位（例如分），`billing_currency` 使用 ISO 4217 三字母代码；如果平台只支持单一币种，也可以在应用层固定并从 DDL 中移除该列，但不能让金额含义不明确。

### 默认引用的跨租户完整性

后续表必须以租户复合键建模，例如：

```sql
CREATE TABLE agent_app (
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id),
    app_id    TEXT NOT NULL,
    -- 其他发布版本、模型和工具授权字段后续设计
    PRIMARY KEY (tenant_id, app_id),
    UNIQUE (tenant_id, app_id)
);

CREATE TABLE backend_profile (
    tenant_id  TEXT NOT NULL REFERENCES tenant(tenant_id),
    profile_id TEXT NOT NULL,
    PRIMARY KEY (tenant_id, profile_id),
    UNIQUE (tenant_id, profile_id)
);

ALTER TABLE tenant
    ADD CONSTRAINT fk_tenant_default_agent
    FOREIGN KEY (tenant_id, default_agent_app_id)
    REFERENCES agent_app (tenant_id, app_id);

ALTER TABLE tenant
    ADD CONSTRAINT fk_tenant_default_backend
    FOREIGN KEY (tenant_id, default_backend_profile_id)
    REFERENCES backend_profile (tenant_id, profile_id);
```

这两个引用允许为 `NULL`。为 `NULL` 时 Gateway 不得静默回退到平台级或其他租户的配置：只有显式路由到已发布应用的请求才可执行，否则返回可审计的配置错误。删除或停用默认对象前，Admin API 必须先切换默认引用或拒绝操作。

### 字段用途

| 字段 | 语义 | 消费组件 |
| --- | --- | --- |
| `tenant_id` | 全链路不可变隔离键；所有租户归属表显式携带 | Gateway、Worker、Storage、Audit、Telemetry |
| `tenant_key` | 规范化机器键；用于管理 API、配置和指标维度，不承载展示语义 | Admin API、配置缓存 |
| `display_name` | 可修改的运营展示名，不参与鉴权、路由或 session key | Admin API、控制台 |
| `status` | 入口闸门和生命周期状态 | Gateway、Worker、Admin API |
| `rate_limit_rpm` | 租户入口每分钟请求上限；`NULL` 不限，`0` 拒绝新请求 | Gateway |
| `max_concurrent_executions` | 租户并发 Agent 执行上限；`NULL` 不限 | Scheduler、Worker |
| `monthly_token_budget` | 月度 token 上限；`NULL` 不限，`0` 不允许模型消耗 | Quota、Worker、Billing |
| `monthly_spend_limit_minor` / `billing_currency` | 月度金额上限及币种，金额为最小货币单位 | Billing、Quota、Telemetry |
| `audit_retention_days` | 审计事件保留期限；不控制安全事件是否写入 | Audit retention job |
| `log_masking_level` | 日志、trace 和审计载荷脱敏级别；密钥始终禁止写入 | Logging、Audit、Telemetry |
| `trace_sampling_rate` | 可观测性 trace 采样率 `[0,1]`；强制审计不受影响 | OTel Collector |
| `default_*` | 租户默认路由对象；必须通过同租户复合 FK | Gateway、Storage Router |
| `version` | 配置并发更新和缓存失效版本 | Admin API、Config Cache |
| `created_at` / `updated_at` | 数据库生成的生命周期时间 | Admin API、Audit、Ops |

实际 token、金额和调用次数不能只回写到 tenant 计数器；应从不可变的 usage/audit 事件聚合，避免并发丢失和无法追溯。

### 生命周期与状态变更

```text
                         结清 / 整改
                    ┌──────────────────┐
                    │                  ▼
                 active ──欠费/违规──> suspended
                    │                  │
                    │ 管理员停用       │ 长期暂停/管理员停用
                    ▼                  ▼
                 disabled <────────────┘
                    (终态)
```

- `active`：Gateway 可接收新消息，Worker 可调度新执行。
- `suspended`：拒绝新执行并返回固定兜底文案；已接受的执行按取消/收尾策略完成；数据保留。
- `disabled`：终态，不再路由流量；只允许受控的管理和审计读取，数据按保留策略归档/清理。
- 每次迁移必须在同一事务中写入状态变更审计或 Outbox 事件，至少包含 actor、reason、旧/新状态、发生时间、变更前后的 `version` 及 correlation/trace ID。
- 状态检查不能只依赖长 TTL 缓存；应按 `version` 主动失效，确保暂停/停用及时生效。

### 与 tRPC-Agent-Go 的映射

| 平台边界 | 具体约定 |
| --- | --- |
| 可信租户解析 | Gateway 验证 IM 回调和 `channel_binding` 后得到 `tenant_id`；不接受未验证的请求头或用户输入作为租户 ID，并将其写入受控 `context.Context`。 |
| Runner 身份 | 以结构化、无歧义的编码将 `tenant_id` 与外部用户/会话标识组合到 Runner 的 `userID` / `sessionID` 命名空间；持久化查询仍显式带 `tenant_id`，不把 key prefix 当作唯一隔离。 |
| Agent 装配 | Agent Factory 根据已发布的 tenant/app 配置快照创建或复用 Agent；一次执行固定配置版本，避免热更新造成半个请求使用两套配置。模型和工具密钥从 Secret Manager 按租户授权注入。 |
| 可观测性 | Span attributes 写入 `tenant.id`、`tenant.version`、`agent_app.id` 和 `trace_id`；指标的租户维度须评估高基数与访问控制，成本归集从 usage/audit 事件完成。 |
| 取消与状态 | Worker 用 `context.Context` 传递取消；suspended/disabled 只阻断新执行，不绕过已接受请求的收尾和事件排空策略。 |

## 其余核心表（占位）

```text
agent_app         Agent 应用（租户级，发布版本、模型与工具授权） ← 下一步
backend_profile   数据后端档案（Session / Memory / Knowledge 路由）
channel_binding   IM 通道绑定（tenant + channel + 账号 → agent_app）
session           会话（tenant_id、session_id、状态、TTL）
message_event     消息与会话事件（session 内有序、幂等）
memory            长期记忆（租户 + 用户维度，可检索）
summary           会话摘要（滚动压缩）
audit_log         审计日志（tenant、channel、user、tool、decision、cost、trace_id）
```

后续 issue 必须延续 `tenant_id` 显式列和同租户复合约束，不能通过共享的全局 ID 或隐含前缀绕过租户边界。
