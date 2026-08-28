# tRPC-Agent-Service

基于 [tRPC-Agent-Go](https://github.com/trpc-group/trpc-agent-go) 的多租户节点化 Agent 部署平台。

## 平台能力

- **多租户隔离**：租户级配置、数据、工具权限、审计与密钥隔离
- **节点化部署**：Agent Worker 无状态水平扩展,共享 Session / Memory 后端
- **多后端支持**：InMemory / Redis / SQL / 向量库 / 对象存储按租户选择与路由
- **IM 接入**：企业微信、微信客服、公众号等 Channel Adapter
- **治理与可观测**：Plugin / Guardrail、OpenTelemetry、租户级审计与成本统计
- **故障恢复**：降级策略、灰度发布、租户级配置回滚

## 当前运行时设计

- [Model Profile、Secret Resolver 与最小 Runner 链路](model-profile.md)：Issue #22 的文档先行设计，

- [Issue #71：Tenant-scoped Provider Registries](issue-71-provider-registries.md)：Secret、Model、Backend、Channel 的租户隔离注册表契约。

- [Issue #69：Multi-tenant Bootstrap](issue-69-multi-tenant-bootstrap.md)：多租户 API identity、动态 Session capability 和按租户审计路由。
- [Issue #72：Precise Cache Invalidation](issue-72-cache-invalidation.md)：按 tenant/app/profile 精确失效，并保持 in-flight Runner 快照。
  固定模型解析、密钥边界、Execution Plan 和 tRPC-Agent-Go Runner 的纵向契约。
- [Issue #82：Agent App Registry 与租户灰度](issue-82-agent-app-registry.md)：实例内按租户选择不可变 candidate revision、授权变更、审计事实和 lease-safe 回滚。
- [生产架构设计](architecture.md)：Issue #24 的控制面/数据面拓扑、可信 Channel Binding、
  企业微信全链路、幂等、迁移和能力边界设计。文档交付不等于对应生产代码已实现。
- [Gateway、Execution Plan 与 HTTP/SSE](gateway.md)：对齐 PR #25 架构验收与 Issue #26
  可信主体，定义 Issue #28 的 Resolver、Runner Registry、Dispatch、普通/SSE API、
  限流、幂等和服务生命周期契约。
- [Telegram 长轮询 Adapter](telegram.md)：Issue #31 的文档先行契约，固定单 Binding、Bot
  身份校验、普通文本映射、Dispatch 聚合回复和生命周期边界。
- [企业微信自建应用 Text Webhook](wecom.md)：Issue #60 的文档先行契约，固定 callback
  验签/AES 解密、可信 Binding 路由、文本入站和可靠回复边界。
- [Telegram live E2E 示例](https://github.com/XnLemon/trpc-agent-service/tree/main/examples/telegram-e2e)：
  Issue #33 的真实 Bot API 传输冒烟测试和手动 CI 运行说明。
- [PostgreSQL 控制面与启动装配](postgresql-control-plane.md)：Issue #37 的实现契约，
  复用既有表设计并统一 migration、Repository 事务边界、bootstrap、readiness 和 shutdown。
- [Issue #81：MySQL 控制面 Repository](issue-81-mysql-control-plane.md)：MySQL 与 PostgreSQL
  的 SQL 语义映射、事务/锁、迁移、Bootstrap 选择和验证矩阵。
- [Issue #41：可重启控制面与 Admin API](issue-41-runtime-bootstrap-admin-api.md)：已合并的实现契约，
  固定真实 Bootstrap、readiness、最小 Admin API、重启恢复和验收矩阵。
- [Issue #67：首次运行初始化](issue-67-first-run-init.md)：显式 `trpc-service init`、数据库状态判定、
  并发幂等和 local/staging/production 首次运行流程。
- [Issue #50：Reliable Reply Delivery](issue-50-reliable-delivery.md)：Outbox worker、Provider
  交付、重试/DLQ、lease recovery、telemetry 与验收 ledger。
- [租户审计与用量成本](audit-usage.md)：Issue #54/PR #55 的实现契约，固定 mandatory audit
  与 telemetry 边界、事件 schema、append-only/重复语义、失败策略、聚合和运维 ledger。
- [Issue #79：生产可观测性、Dashboard 与告警](issue-79-observability.md)：trace/metrics 链路、
  低基数标签、租户授权查询以及 Prometheus/Grafana 资源契约。

## 快速开始

```bash
git clone https://github.com/XnLemon/trpc-agent-service.git
cd trpc-agent-service

./scripts/build.sh
./scripts/start.sh
```

停止服务:

```bash
./scripts/stop.sh
```

## 文档导航

- [架构设计](architecture.md) — 组件拓扑、可信路由、消息链路、数据同步和多后端迁移
- [数据模型](data-model.md) — 核心表结构、Session/Event/Memory/Summary/Audit 和租户约束
- [Channel Binding](channel-binding.md) — 租户级通道绑定、候选发现与可信入站路由
- [Telegram 长轮询 Adapter](telegram.md) — 单 Binding Telegram long polling、文本映射与安全边界
- [企业微信自建应用 Text Webhook](wecom.md) — 自建应用 callback、文本入站与回复 Outbox
- [Gateway、Execution Plan 与 HTTP/SSE](gateway.md) — 可信主体、固定执行计划、Runner Registry、
  Dispatch、健康检查、优雅停机和普通/流式 API
- [PostgreSQL 控制面与启动装配](postgresql-control-plane.md) — 六类控制面表的 migration 顺序、
  SQL Repository 事务和真实运行时启动装配
- [Issue #81：MySQL 控制面 Repository](issue-81-mysql-control-plane.md) — MySQL 适配器、迁移、
  Bootstrap 驱动选择和租户隔离验证
- [Issue #41：可重启控制面与 Admin API](issue-41-runtime-bootstrap-admin-api.md) — Bootstrap/readiness、
  管理 API、重启恢复和测试矩阵
- [Issue #67：首次运行初始化](issue-67-first-run-init.md) — 显式首租户/App 初始化命令、幂等并发和部署流程
- [Issue #50：Reliable Reply Delivery](issue-50-reliable-delivery.md) — 回复 Outbox 交付、Provider、
  重试/DLQ、恢复与运维证据
- [租户审计与用量成本](audit-usage.md) — 版本化事件、append-only writer、用量成本聚合、
  保留/脱敏/访问控制和 failure/repair 规则
- [Issue #79：生产可观测性、Dashboard 与告警](issue-79-observability.md) — trace、metrics、
  dashboard、告警和租户查询边界
- [运维方案](ops.md) — 发布灰度、监控审计、故障恢复、容量模型和生产风险清单

## CI

仓库在 push 到 `main` 和 PR 时自动运行格式、静态检查、构建、测试覆盖率与文档构建,详见仓库根目录的 `.github/workflows/`。
