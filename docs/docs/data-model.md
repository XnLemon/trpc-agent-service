# 数据模型

> 设计分阶段推进:当前完成 **租户模型(tenant)**,其余核心表(agent_app、channel_binding、session、message/event、memory、summary、audit_log)为占位,后续逐个展开。

## 租户模型

### 建模决策

租户是整个平台隔离体系的根。题目要求的「应用配置、模型配置、工具权限、IM 通道配置、数据后端配置」**不内嵌**在 tenant 主表,而是拆分为关联表(一个租户可拥有多个 Agent 应用、多条通道绑定、多套后端配置),tenant 主表只承载:身份、状态、配额预算、审计策略与默认引用。

这样做的理由:

- 租户下的 Agent 应用、通道绑定是**多对一**关系,内嵌 JSONB 会失去外键约束与独立生命周期管理(灰度、回滚、单独禁用某个通道)
- 配额与审计策略是**租户全局**属性,跟随租户而非单个应用,放主表可直接在 Gateway 入口做限流与审计决策,无需联查
- 主表保持窄表,读写频繁的租户鉴权路径(每条消息都要查)便于缓存

### 表结构(PostgreSQL DDL)

```sql
CREATE TABLE tenant (
    -- 身份
    tenant_id            TEXT PRIMARY KEY,              -- 't_' + 10 位 ULID,如 t_01J8ZQ3W9K
    name                 TEXT NOT NULL,                 -- 租户显示名,平台内唯一
    status               TEXT NOT NULL DEFAULT 'active'
                         CHECK (status IN ('active', 'suspended', 'disabled')),

    -- 配额与预算(超限策略见 ops: 降级 -> 仅缓存回复 -> 停用)
    monthly_token_budget BIGINT NOT NULL DEFAULT 0,     -- 0 = 不限
    rate_limit_rpm       INT    NOT NULL DEFAULT 60,    -- 网关层每分钟消息上限
    max_concurrent_sessions INT NOT NULL DEFAULT 100,   -- 单租户并发会话上限

    -- 审计策略
    audit_retention_days INT    NOT NULL DEFAULT 90,    -- 审计日志保留期
    audit_sampling_rate  REAL   NOT NULL DEFAULT 1.0,   -- 采样率,1.0 = 全量
    log_masking_level    TEXT   NOT NULL DEFAULT 'basic'
                         CHECK (log_masking_level IN ('none', 'basic', 'strict')),

    -- 默认引用(详细配置在关联表,此处仅默认值,允许为空走平台级默认)
    default_agent_app_id TEXT,                          -- FK -> agent_app(占位)
    default_backend_id   TEXT,                          -- FK -> backend_profile(占位)

    -- 乐观锁与时间戳
    version              INT    NOT NULL DEFAULT 0,     -- 配置并发更新控制
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_tenant_name UNIQUE (name)
);
```

### 字段说明

| 字段 | 用途 | 使用方 |
| --- | --- | --- |
| `tenant_id` | 全链路隔离键;session/memory/audit 等所有数据表均携带此列;`t_` 前缀 + ULID 避免枚举攻击,且时间有序便于排序 | 全部组件 |
| `name` | 管理后台展示名,唯一约束防止运营混淆 | Admin API |
| `status` | 生命周期:`active` 正常;`suspended` 欠费/违规暂停(拒绝新消息,存量数据保留);`disabled` 软删除(仅审计可查) | Gateway 鉴权 |
| `monthly_token_budget` | 月度 token 预算,审计表实时累加比对;超预算触发降级而非硬停,保护进行中会话 | Worker / Telemetry |
| `rate_limit_rpm` | 租户级入口限流,防单租户 IM 消息风暴拖垮平台(尤其群聊场景) | Gateway |
| `max_concurrent_sessions` | 并发会话上限,与节点容量评估联动 | Worker 调度 |
| `audit_retention_days` | 审计日志 TTL,到期由清理任务删除,满足合规又控制存储成本 | 审计清理任务 |
| `audit_sampling_rate` | 高流量租户可降采样(如 0.1),配合全量错误日志保证审计完整性 | 审计写入 |
| `log_masking_level` | `none` 不脱敏(内测);`basic` 脱敏 token/key/手机号;`strict` 额外脱敏用户内容片段 | 日志/Trace 中间件 |
| `default_agent_app_id` | 未指定应用时的路由目标;`NULL` 时拒绝,强制显式绑定 | Gateway 路由 |
| `default_backend_id` | 租户默认存储组合(session/memory/knowledge 的后端档案) | Storage Router |
| `version` | 乐观锁:Admin API 并发更新配置时 `WHERE version = ?`,失败重试;同时作为租户配置缓存失效依据 | Admin API / 缓存 |

### 生命周期状态机

```text
        创建          欠费/违规            结清/整改
 ──────► active ◄────────────► suspended ─────────► active
           │                      │
           │ 管理员删除            │ 长期暂停(>N天)
           ▼                      ▼
        disabled ◄──────────── disabled(仅审计可读,数据到期清理)
```

- 所有状态变更写审计日志,记录操作者与原因
- `suspended` 时 Gateway 对新消息返回「服务暂停」兜底文案,进行中会话允许自然结束

### 与 tRPC-Agent-Go 的映射

| 平台概念 | 框架能力 |
| --- | --- |
| `tenant_id` 隔离键 | `runner.Run(ctx, userID, sessionID, ...)` 的 `userID` 组装为 `{tenant_id}:{user_id}`,框架层零改造获得租户维度 |
| 租户级 Agent 装配 | `runner.NewRunnerWithAgentFactory` 工厂函数按请求从租户配置构建 Agent(指令、模型、工具集) |
| 租户标签观测 | OpenTelemetry span attributes 注入 `tenant.id` / `tenant.name`,Langfuse 属性 `langfuse.user.id` 同步携带 |

## 其余核心表(占位,待设计)

```text
agent_app         Agent 应用(租户级, 模型与工具配置)    ← 下一步
backend_profile   数据后端档案(session/memory/knowledge 路由)
channel_binding   IM 通道绑定(tenant + channel + 账号 → agent_app)
session           会话(tenant_id, session_id, 状态, TTL)
message / event   消息与会话事件(session 内有序)
memory            长期记忆(租户 + 用户维度, 可检索)
summary           会话摘要(滚动压缩)
audit_log         审计日志(tenant, channel, user, tool, cost, trace_id)
```
