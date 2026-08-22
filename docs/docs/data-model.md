# 数据模型

> 设计分阶段推进：`tenant` 根模型已经完成，`agent_app` 的稳定身份、发布版本和运行时边界见
> [Agent App 模型与发布边界](agent-app-model.md)，数据后端选择见
> [Backend Profile 控制面与运行时边界](backend-profile.md)。其余核心表继续按依赖顺序保留占位。

## Tenant 根模型

### 建模决策

`tenant` 是配置、数据、权限、密钥、审计和成本的顶层隔离键，但保持为窄根表。它只保存身份、生命周期、租户级限额、审计控制、默认引用和并发版本；应用、模型、工具权限、IM 通道、后端连接参数和密钥分别由关联表或 Secret Manager 管理。

这样拆分有三个原因：

- 一个租户可以拥有多个 Agent 应用、通道绑定和后端档案，它们需要独立发布、灰度、回滚和停用。
- 根表字段会在 Gateway 鉴权和调度路径高频读取，窄表便于缓存和版本失效；不把低频配置 JSONB 载入每次请求。
- 关联表可以通过外键、租户复合唯一键和独立状态表达完整性，避免 JSONB 或仅靠 key prefix 造成跨租户引用。

`tenant_id` 是不可变的内部隔离键；`tenant_key` 是规范化且不可变的机器键；`display_name` 只用于展示，可以修改，不能用于路由或会话命名空间。`tenant_key` 的规范形式为 2 至 64 个小写 ASCII 字符，首字符为字母，其余字符只能是字母、数字或连字符。Admin API 应先执行 trim 和小写化，再校验该 grammar；数据库只接受已经规范化的值，因而不会悄悄把不同输入折叠成同一个唯一键。

### PostgreSQL DDL

下面的 DDL 是根表最小实现。默认引用的复合外键需要在后续关联表创建后追加，见下方完整性约束。

```sql
CREATE TABLE tenant (
    -- 身份：ULID 可排序，但不应暴露为可枚举的业务编号
    tenant_id       TEXT PRIMARY KEY
                    -- ULID's first payload character is 0-7 (the remaining
                    -- 25 characters use the Crockford alphabet).
                    CHECK (tenant_id ~ '^t_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    tenant_key      TEXT NOT NULL UNIQUE
                    CHECK (tenant_key ~ '^[a-z][a-z0-9-]{1,63}$'),
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

-- tenant_id 和 tenant_key 为不可变标识。
CREATE OR REPLACE FUNCTION tenant_reject_identity_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.tenant_key IS DISTINCT FROM OLD.tenant_key THEN
        RAISE EXCEPTION 'tenant identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tenant_identity_immutable
BEFORE UPDATE ON tenant
FOR EACH ROW EXECUTE FUNCTION tenant_reject_identity_change();

-- 配置更新函数使用以下乐观锁谓词，并负责版本递增和 updated_at。
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

-- 完整字段、binding 子表、约束和 Outbox 见 Backend Profile 设计；
-- 这里保留默认引用所依赖的复合键形状。
CREATE TABLE backend_profile (
    tenant_id  TEXT NOT NULL REFERENCES tenant(tenant_id),
    profile_id TEXT NOT NULL
               CHECK (profile_id ~ '^bp_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    profile_key TEXT NOT NULL
                CHECK (profile_key ~ '^[a-z][a-z0-9-]{1,63}$'),
    status      TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'suspended', 'disabled')),
    PRIMARY KEY (tenant_id, profile_id),
    UNIQUE (tenant_id, profile_key)
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
| `tenant_key` | 不可变的规范化 ASCII slug；用于管理 API、配置和指标维度，不承载展示语义 | Admin API、配置缓存 |
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

状态不是普通配置字段。运行时数据库角色不得直接更新 `tenant.status`，而是只能执行受限的状态迁移函数；该函数锁定 tenant 行、校验期望版本和允许的迁移，在同一事务中更新状态并写入 Outbox。以下 DDL 给出最小边界；完整 `audit_log` schema 仍由后续 issue 定义。

```sql
BEGIN;

CREATE TABLE tenant_status_change_outbox (
    event_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id         TEXT NOT NULL REFERENCES tenant(tenant_id),
    previous_status   TEXT NOT NULL CHECK (previous_status IN ('active', 'suspended')),
    next_status       TEXT NOT NULL CHECK (next_status IN ('active', 'suspended', 'disabled')),
    actor_type        TEXT NOT NULL CHECK (length(btrim(actor_type)) > 0),
    actor_id          TEXT NOT NULL CHECK (length(btrim(actor_id)) > 0),
    reason            TEXT NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 1000),
    previous_version  BIGINT NOT NULL,
    next_version      BIGINT NOT NULL,
    correlation_id    TEXT NOT NULL CHECK (length(btrim(correlation_id)) > 0),
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((previous_status, next_status) IN (
        ('active', 'suspended'),
        ('active', 'disabled'),
        ('suspended', 'active'),
        ('suspended', 'disabled')
    )),
    CHECK (next_version = previous_version + 1)
);

CREATE OR REPLACE FUNCTION transition_tenant_status(
    p_tenant_id TEXT,
    p_expected_version BIGINT,
    p_next_status TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_previous_status TEXT;
    v_next_version BIGINT;
BEGIN
    IF p_actor_type IS NULL OR length(btrim(p_actor_type)) = 0
       OR p_actor_id IS NULL OR length(btrim(p_actor_id)) = 0 THEN
        RAISE EXCEPTION 'tenant status transition requires a non-blank actor';
    END IF;
    IF p_correlation_id IS NULL OR length(btrim(p_correlation_id)) = 0 THEN
        RAISE EXCEPTION 'tenant status transition requires a non-blank correlation ID';
    END IF;

    SELECT status INTO v_previous_status
    FROM public.tenant
    WHERE tenant_id = p_tenant_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'tenant % does not exist', p_tenant_id;
    END IF;
    IF v_previous_status = 'disabled' THEN
        RAISE EXCEPTION 'disabled tenant cannot be re-enabled';
    END IF;
    IF (v_previous_status, p_next_status) NOT IN (
        ('active', 'suspended'),
        ('active', 'disabled'),
        ('suspended', 'active'),
        ('suspended', 'disabled')
    ) THEN
        RAISE EXCEPTION 'invalid tenant status transition: % -> %',
            v_previous_status, p_next_status;
    END IF;

    UPDATE public.tenant
    SET status = p_next_status, version = version + 1, updated_at = now()
    WHERE tenant_id = p_tenant_id AND version = p_expected_version
    RETURNING version INTO v_next_version;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'tenant version conflict';
    END IF;

    INSERT INTO public.tenant_status_change_outbox (
        tenant_id, previous_status, next_status, actor_type, actor_id, reason,
        previous_version, next_version, correlation_id
    ) VALUES (
        p_tenant_id, v_previous_status, p_next_status, p_actor_type, p_actor_id,
        p_reason, p_expected_version, v_next_version, p_correlation_id
    );
END;
$$;

CREATE OR REPLACE FUNCTION update_tenant_configuration(
    p_tenant_id TEXT,
    p_expected_version BIGINT,
    p_display_name TEXT,
    p_rate_limit_rpm BIGINT,
    p_max_concurrent_executions BIGINT,
    p_monthly_token_budget BIGINT,
    p_monthly_spend_limit_minor BIGINT,
    p_billing_currency CHAR(3),
    p_audit_retention_days INT,
    p_log_masking_level TEXT,
    p_trace_sampling_rate REAL,
    p_default_agent_app_id TEXT,
    p_default_backend_profile_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_next_version BIGINT;
BEGIN
    -- Keep the global lock order Tenant -> referenced Profile. Status
    -- transitions use the same order, avoiding a default-assignment race.
    PERFORM 1
    FROM public.tenant
    WHERE tenant_id = p_tenant_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'tenant does not exist';
    END IF;

    IF p_default_backend_profile_id IS NOT NULL THEN
        PERFORM 1
        FROM public.backend_profile
        WHERE tenant_id = p_tenant_id
          AND profile_id = p_default_backend_profile_id
          AND status = 'active'
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'default backend profile must exist in the tenant and be active';
        END IF;
    END IF;

    -- A full, validated configuration snapshot avoids ambiguous patch/null semantics.
    UPDATE public.tenant
    SET display_name = p_display_name,
        rate_limit_rpm = p_rate_limit_rpm,
        max_concurrent_executions = p_max_concurrent_executions,
        monthly_token_budget = p_monthly_token_budget,
        monthly_spend_limit_minor = p_monthly_spend_limit_minor,
        billing_currency = p_billing_currency,
        audit_retention_days = p_audit_retention_days,
        log_masking_level = p_log_masking_level,
        trace_sampling_rate = p_trace_sampling_rate,
        default_agent_app_id = p_default_agent_app_id,
        default_backend_profile_id = p_default_backend_profile_id,
        version = version + 1,
        updated_at = now()
    -- disabled is terminal: archival work uses a separately designed path.
    WHERE tenant_id = p_tenant_id
      AND version = p_expected_version
      AND status <> 'disabled'
    RETURNING version INTO v_next_version;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'tenant is disabled, has a version conflict, or does not exist';
    END IF;
    RETURN v_next_version;
END;
$$;

-- Runtime roles are provisioned without table ownership, inherited broad DML,
-- or direct tenant reads. All configuration and lifecycle mutations are
-- function-only, so version and updated_at cannot be independently changed.
REVOKE ALL PRIVILEGES ON tenant FROM PUBLIC;
-- Workers receive tenant-scoped configuration snapshots from the control plane;
-- they must not enumerate tenant configuration through a shared database role.
REVOKE SELECT, UPDATE ON tenant FROM tenant_app_writer;
-- Cross-tenant root-table reads are an explicit Admin control-plane capability.
GRANT SELECT ON tenant TO tenant_admin_writer;
-- PostgreSQL grants EXECUTE on new functions to PUBLIC by default. Revoke it
-- before the role-specific grants while this migration transaction is open.
REVOKE ALL ON FUNCTION transition_tenant_status(
    TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT
) FROM PUBLIC;
REVOKE ALL ON FUNCTION update_tenant_configuration(
    TEXT, BIGINT, TEXT, BIGINT, BIGINT, BIGINT, BIGINT, CHAR(3), INT,
    TEXT, REAL, TEXT, TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION transition_tenant_status(
    TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT
) TO tenant_admin_writer;
GRANT EXECUTE ON FUNCTION update_tenant_configuration(
    TEXT, BIGINT, TEXT, BIGINT, BIGINT, BIGINT, BIGINT, CHAR(3), INT,
    TEXT, REAL, TEXT, TEXT
) TO tenant_admin_writer;

COMMIT;
```

`SECURITY DEFINER` 函数由不属于运行时连接池的专用 owner 持有；其 `search_path` 固定，且函数 owner 不得是可登录的应用账号。Admin API 只能以 `tenant_admin_writer` 调用状态迁移和配置更新函数；该角色也是唯一拥有跨租户根表读取权限的运行时控制平面角色。普通 Worker 使用 `tenant_app_writer`，没有 `tenant` 的 `SELECT` 或 `UPDATE` 权限。Gateway 验证 IM 回调并解析可信 `tenant_id` 后，由控制平面生成只包含该租户、固定 `version` 的配置快照并随任务下发；Worker 只能消费快照，不能通过用户输入指定租户或查询根表。配置更新函数接收完整配置快照与 `expected_version`，只会将版本递增一次并维护 `updated_at`，且拒绝 `disabled` 租户；停用后的归档或清理若需要写入，必须由后续 issue 定义独立的受控流程。数据库 owner 和 migration role 是受控管理身份，不属于生产流量路径。

### 与 tRPC-Agent-Go 的映射

| 平台边界 | 具体约定 |
| --- | --- |
| 可信租户解析 | Gateway 验证 IM 回调和 `channel_binding` 后得到 `tenant_id`；不接受未验证的请求头或用户输入作为租户 ID，并将其写入受控 `context.Context`。 |
| 租户配置读取 | 控制平面只在可信 `tenant_id` 已确定后读取根表，并将该租户的固定版本配置快照下发给 Worker；`tenant_app_writer` 无根表读取权限，不能枚举或跨租户查询。 |
| Runner 身份 | 以结构化、无歧义的编码将 `tenant_id` 与外部用户/会话标识组合到 Runner 的 `userID` / `sessionID` 命名空间；持久化查询仍显式带 `tenant_id`，不把 key prefix 当作唯一隔离。 |
| Agent 装配 | Agent Factory 根据已发布的 tenant/app 配置快照创建或复用 Agent；一次执行固定配置版本，避免热更新造成半个请求使用两套配置。模型和工具密钥从 Secret Manager 按租户授权注入。 |
| 可观测性 | Span attributes 写入 `tenant.id`、`tenant.version`、`agent_app.id` 和 `trace_id`；指标的租户维度须评估高基数与访问控制，成本归集从 usage/audit 事件完成。 |
| 取消与状态 | Worker 用 `context.Context` 传递取消；suspended/disabled 只阻断新执行，不绕过已接受请求的收尾和事件排空策略。 |

## Channel Binding 与消息数据模型

本节是 Issue #24 的逻辑模型设计，不是已经执行的 PostgreSQL migration。**现有实现**包含
Tenant、Agent App/Revision、Model Profile、Backend Profile 和无密钥执行快照；下面的
Channel/Session/Event/Memory/Summary/Audit 表属于**平台新增**。所有生产 Repository 都必须
把 `tenant_id` 作为显式参数和列，字符串 namespace 只能防碰撞，不能替代授权或复合约束。

### 稳定身份与约束

```text
channel_binding(
  tenant_id, binding_id, channel, provider_account_id,
  public_route_key_digest, app_id, secret_ref,
  status, version, config_digest, created_at, updated_at
)

session(
  tenant_id, session_id, binding_id, app_id,
  external_user_id, external_chat_id, external_thread_id,
  status, state_version, last_event_seq, expires_at, created_at, updated_at
)

message_event(
  tenant_id, event_id, session_id, binding_id,
  external_message_id, idempotency_key, event_seq, kind,
  payload_ref, payload_digest, status, request_id, trace_id,
  created_at, committed_at
)

memory_entry(
  tenant_id, memory_id, user_id, session_id, source_event_id,
  content_ref, content_digest, visibility, index_status, created_at
)

session_summary(
  tenant_id, session_id, summary_version, base_event_seq,
  content_ref, content_digest, status, created_at, updated_at
)

audit_log(
  tenant_id, audit_id, binding_id, channel, user_id, session_id,
  agent_app_id, revision, model_profile_id, tool_name, decision,
  event_seq, latency_ms, error_type, cost_minor, request_id, trace_id,
  actor_type, actor_id, reason, occurred_at, previous_digest, digest
)
```

建议约束如下：

- `channel_binding` 的主键为 `(tenant_id, binding_id)`，`app_id` 通过同租户复合外键引用
  `agent_app`；`secret_ref` 只引用 Secret Manager，不允许保存 secret 值。
- `public_route_key_digest` 仅用于候选发现。相同 `channel + provider_account_id` 的
  active Binding 只能归属一个 Tenant；若未来支持共享账号，必须新增明确的共享模型，不能
  删除这条唯一性约束。
- `session` 的主键为 `(tenant_id, session_id)`，并以 `(tenant_id, binding_id, 外部身份
  元组)` 建唯一约束。外部身份元组由 Adapter 按通道定义，不使用昵称或可变展示名。
- `message_event` 同时有 `(tenant_id, session_id, event_seq)` 唯一约束和
  `(tenant_id, binding_id, external_message_id)` 唯一约束；`idempotency_key` 由验签后的
  `tenant_id + binding_id + channel + external_message_id` 计算。
- `memory_entry` 和 `session_summary` 的正文可以放对象存储，但 SQL 仍保存租户、digest、
  权限、版本和来源 event；向量库只保存可重建的索引，不是权限或审计真相。
- `audit_log` 使用 append-only 写入；`digest`/`previous_digest` 可形成租户内 hash chain，
  长期归档再使用 WORM 或等价不可篡改策略。

`tenant_id` 不能从 `external_user_id`、URL 前缀或 Runner 的字符串 key 反推。Gateway 在
验签成功后才创建可信租户上下文；Storage Adapter 每一次读写都接收该上下文中的显式租户
边界，并拒绝调用者传入不一致的租户值。

### Session identity 规则

```text
direct:  tenant + binding + external_user_id
group:   tenant + binding + external_chat_id
thread:  tenant + binding + external_chat_id + external_thread_id
```

实现时使用长度前缀或结构化编码，避免简单字符串拼接碰撞；现有
`tenant.NewRunnerIdentity` 已提供 Tenant 与外部用户/Session 的无歧义命名空间，但未来
Channel Binding 仍必须在持久化列中保留 binding、channel 和外部身份。相同外部用户在不同
租户、不同 Binding 或不同群聊中不得共享 `session_id`。

## 消息状态与提交顺序

`message_event.status` 与入站幂等记录共同表达以下状态机：

```text
received → running → completed → reply_pending → replied
     │          │          │              │
     └──────────┴──────────┴──────────────┴→ failed / DLQ
```

在 `running` 收到重复请求时只返回确认，不再创建 Runner；`completed` 或 `reply_pending`
时使用缓存回复引用重试出站；`replied` 直接返回已完成；`failed` 只有在错误可重试且没有
不可逆副作用时才能重新排队。模型输出不是天然幂等，扣费、发送、工单和外部写操作必须有
单独的幂等键或人工确认。

推荐顺序是：唯一写入入站事实 → 以 Session version/CAS 或事务分配 `event_seq` → 提交 event
和 state → 写 reply outbox → 异步生成 summary → 写 durable Memory 并异步索引。SQL Adapter
可以把 event/state/outbox 放在同一事务；Redis Adapter 必须使用经过验证的 Lua/Stream/事务
边界；无法原子提交时必须声明最终一致并提供补偿和 repair cursor。向量索引延迟不能影响
Tenant 权限、Session 顺序或 Audit。

## 多后端职责矩阵

| 后端 | 适合存储 | 一致性/延迟 | 成本与运维取舍 |
| --- | --- | --- | --- |
| InMemory | 单进程测试、开发期 Session/Memory | 进程内 mutex；重启丢失、无跨节点可见性 | 最低成本；不能用于生产或迁移源 |
| Redis | 幂等、Queue、低延迟 Session/Event、短期 cache | 单 key 原子能力强；跨 key、故障转移和持久化需看配置 | 运维简单、低延迟；内存成本和数据耐久性要评估 |
| PostgreSQL/MySQL | 控制面、Session/Event/State、Summary、Audit、Memory 正文 | 事务/CAS/复合约束适合作为权威源；副本读取可能延迟 | 查询和审计能力强；连接、锁、分片和容量需运营 |
| Vector DB | Knowledge/Memory embedding 与检索索引 | 最终一致；不能与 SQL event 假设跨库事务 | 检索性能好；重建、维度和 provider 迁移成本高 |
| S3/对象存储 | Artifact、原始文档、较大的 Memory/审计归档 | 以具体 provider 合同为准；metadata 仍需 SQL | 单位成本低；权限、生命周期和清理必须额外管理 |

一次执行选择的 Backend Profile version 必须写入 `ExecutionPlan` 和审计。Redis → SQL 或本地
向量库 → 远端向量库都通过新的 Profile version 迁移：全量复制、双写、增量追平、digest/序号
校验、shadow read、只切新执行、保留旧版本和可回滚窗口。不能在同一次执行中跨两套后端，
也不能因为两个 provider 都叫“Session”就声称它们有相同的事务语义。

## tRPC-Agent-Go 映射与当前边界

| 数据/运行时责任 | 可直接复用 | 平台新增 |
| --- | --- | --- |
| Session Event 生命周期 | `session.Service`、Runner event 和 context | Tenant-scoped Adapter、CAS/序号、SQL/Redis conformance test |
| Memory/Knowledge | `memory.Service`、Knowledge/VectorStore 接口 | Tenant 分区、异步索引、权限过滤和迁移 |
| Artifact | `artifact.Service` | Tenant bucket/prefix、digest、生命周期和审计引用 |
| Audit | OpenTelemetry 可复用为 trace | 独立 append-only audit adapter；sampling 不能代替审计 |
| Agent 执行 | Runner、LLMAgent、Tool/MCP、Plugin/Guardrail | Gateway、Binding、幂等、策略和回复 Outbox |

当前 Go 代码没有实现上述 Channel/Session/Memory/Audit 生产表、客户端或 migration；文档的
逻辑模型用于约束后续 issue，不能作为已完成的数据库交付物。
