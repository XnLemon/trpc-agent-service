# 基于 tRPC-Agent-Go 设计多租户节点化 Agent 部署平台

[![CI](https://github.com/XnLemon/trpc-agent-service/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/XnLemon/trpc-agent-service/actions/workflows/ci.yml)
[![Docs](https://github.com/XnLemon/trpc-agent-service/actions/workflows/docs.yml/badge.svg?branch=main)](https://github.com/XnLemon/trpc-agent-service/actions/workflows/docs.yml)
[![codecov](https://codecov.io/gh/XnLemon/trpc-agent-service/branch/main/graph/badge.svg)](https://codecov.io/gh/XnLemon/trpc-agent-service)

## 背景和价值

企业在落地 Agent 应用时，通常不会只部署一个单体机器人，而是希望面向多个部门、多个业务线、多个 IM 入口和多个数据后端，构建一套可统一管理的 Agent 平台。例如：客服团队希望把 Agent 接入企业微信，研发团队希望接入内部群机器人，运营团队希望接入微信公众号或微信客服，不同租户又需要隔离会话、记忆、知识库、工具权限和审计日志。

[tRPC-Agent-Go](https://github.com/trpc-group/trpc-agent-go) 已经具备 Agent 编排（LLMAgent / GraphAgent / Chain / Parallel / Cycle）、Tool / MCP、Session、Memory、Knowledge、Artifact、Plugin / Guardrail、Telemetry、HTTP 服务化（OpenAI-compatible / AG-UI / A2A）、OpenClaw / IM 通道等能力。该题要求基于这些能力设计一个“多租户、可节点化部署、支持多后端数据同步、可接入微信 / 企业微信等 IM 软件”的生产级方案。

这个题目解决的业务痛点是：企业希望把 Agent 能力从单点 demo 扩展成平台化服务，同时满足租户隔离、弹性部署、数据一致性、IM 触达、审计合规和后端可替换等要求。它的价值在于把框架能力真正映射到企业级 Agent 平台架构，而不是只停留在单个 Agent 进程。

本题以 **tRPC-Agent-Go** 为实现框架，对称于基于 tRPC-Agent-Python 的同名题目。

### 任务描述

请设计一个基于 tRPC-Agent-Go 的多租户节点化 Agent 部署平台。平台需要支持多个租户创建和部署自己的 Agent，每个租户可以绑定不同 IM 通道、选择不同数据后端、配置不同工具权限和知识库，并允许多个 Agent 节点水平扩展。系统需要考虑跨节点会话路由、数据同步、后端适配、IM 消息接入、监控审计和故障恢复。

本题以架构设计为主，可以包含少量关键 Go 伪代码、接口定义或数据模型示例。不要求实现完整系统，但方案必须足够具体，能指导后续工程落地。

## 具体要求

### 多租户与节点部署

- 设计租户模型，至少包含 `tenant_id`、应用配置、模型配置、工具权限、IM 通道配置、数据后端配置、审计策略。
- 设计节点部署拓扑，说明 Agent Gateway、Agent Worker、Channel Adapter、Storage Adapter、Admin API、Telemetry Collector 等组件如何协作。可对照 tRPC-Agent-Go 中的 `runner.Runner`、`server/*`、`openclaw` Gateway 与 Channel 的职责划分。
- 支持多节点水平扩展，说明用户消息如何路由到正确租户和正确 session。
- 说明是否需要 sticky session；如果不需要，说明如何依赖共享 Session / Memory 后端（例如 `session/redis`、`session/mysql`、`session/postgres`）实现无状态 Worker。
- 设计租户隔离机制，包括配置隔离、数据隔离、工具权限隔离、日志脱敏和密钥管理。

### 数据同步与多后端支持

- 支持不同租户选择不同数据后端，例如 InMemory、Redis、SQL、向量库、对象存储或外部 Memory 服务。tRPC-Agent-Go 已提供 Session（inmemory / redis / mysql / postgres / sqlite / mongodb 等）、Memory、Knowledge、Artifact 以及 `storage`（redis / mysql / postgres / s3 / qdrant / milvus 等）适配，方案需说明如何在平台层做租户级选择与路由。
- 设计统一的数据访问抽象，说明 Session、Memory、Summary、Artifact、Knowledge、Audit Log 分别如何存储。
- 设计数据同步策略，至少覆盖：
  - 多节点并发写入同一 session 的一致性。
  - Session event、state、summary 的更新顺序。
  - Memory 写入后的跨节点可见性。
  - 后端从 Redis 迁移到 SQL 或从本地向量库迁移到远端向量库时的数据迁移方案。
  - IM 消息重复投递时的幂等处理。
- 说明不同后端的一致性取舍，例如强一致、最终一致、读写延迟、成本和运维复杂度。
- 给出一个最小数据模型或表结构示例，至少包含 tenant、agent app、session、message/event、memory、summary、channel binding、audit log。

### IM 软件接入

- 设计 IM Channel Adapter，支持企业微信、微信客服、微信公众号、Telegram 或其他 IM 通道中的至少两类。可复用并扩展 tRPC-Agent-Go 的 OpenClaw Channel 模型。
- 说明外部 IM 消息如何转换为 tRPC-Agent-Go 的用户输入（`model.Message` / `runner.Runner.Run`），Agent Event 如何转换为 IM 回复、流式消息或卡片消息。
- 设计 IM 账号和租户绑定方式，包括 webhook URL、token、secret、回调验签、消息去重、用户身份映射。
- 说明群聊和单聊的 `session_id` 生成规则，以及用户跨群、跨租户时的隔离策略。
- 考虑 IM 平台限制，例如消息长度、频率限制、异步回复、图片 / 文件消息、撤回或失败重试。

### 治理、监控和安全

- 使用 Plugin / Guardrail / Callbacks 设计租户级治理策略，例如工具白名单、敏感信息脱敏、预算限制、危险工具二次确认、IM 用户权限校验。
- 设计监控指标，例如请求量、模型调用耗时、工具调用耗时、IM 投递成功率、错误率、token 消耗、每租户成本、Session 后端延迟。
- 说明如何接入 OpenTelemetry 或等价 tracing，要求 trace 能串起 IM callback、Runner 执行、Tool 调用、Session / Memory 读写和 IM 回复。
- 设计审计日志字段，至少包含 `tenant_id`、`channel`、`user_id`、`session_id`、`agent_name`、`tool_name`、`decision`、`latency`、`error_type`、`cost`、`trace_id`。
- 说明密钥管理和脱敏策略，IM token、模型 API key、数据库密码不能明文出现在日志、trace 或错误报告中。

### 故障恢复与运维

- 设计节点故障、IM 重试、数据库短暂不可用、模型超时、工具执行失败时的降级策略。Go 侧需同时说明 `context.Context` 取消、goroutine 生命周期和 Runner 事件通道排空，避免泄漏。
- 说明如何做灰度发布和租户级配置回滚。
- 说明如何做容量评估，例如每节点并发 session 数、平均 token 消耗、Redis / SQL QPS、IM 回调峰值。
- 设计最小可运行部署方案和生产推荐部署方案，可以使用 Docker Compose、Kubernetes 或等价部署方式描述。

### 交付物

- 一份架构设计文档，建议 2000 – 4000 字。
- 一张系统架构图，展示 Gateway、Worker、Channel Adapter、Storage Adapter、Plugin / Guardrail、Telemetry、数据库和 IM 平台之间的关系。
- 一张核心时序图，展示“企业微信用户发消息 → Agent 执行 → Tool 调用 → Session / Memory 写入 → IM 回复”的完整链路。
- 一份数据模型设计，包含核心表结构或 JSON schema。
- 一份数据同步和幂等策略说明。
- 一份多后端适配方案，说明 Redis / SQL / 向量库 / 对象存储分别适合存什么。
- 一份风险清单，列出至少 8 个生产风险及对应缓解措施。
- 一份基于该设计的 GitHub 实现代码。

## 题目难点

- 多租户隔离不是只加一个 `tenant_id` 字段，还涉及配置、权限、密钥、数据、日志、工具和成本隔离。
- 节点化部署要求 Agent Worker 尽量无状态，但 Agent 又天然依赖 Session、Memory、Summary 和工具上下文，需要设计可靠的共享状态层。
- IM 通道存在消息乱序、重复投递、响应超时、长度限制和身份映射问题，不能简单等同于 HTTP chat API。
- 不同后端的数据一致性能力不同，Redis、SQL、向量库、对象存储无法用同一种同步策略处理。
- Agent 执行链路包含模型、工具、MCP、知识库、沙箱和外部系统，监控和审计必须跨组件串联。
- 企业级平台必须考虑灰度、回滚、租户级限流、成本控制和合规审计。

## 验收标准

1. 架构方案必须覆盖多租户、节点化部署、数据同步、多后端支持、IM 接入、治理监控和故障恢复。
2. 数据模型必须能表达 tenant、agent、channel binding、session、event、memory、summary、audit log 的关系。
3. 必须说明至少两种 IM 通道的接入差异，其中至少包含微信或企业微信。
4. 必须说明至少三类后端的数据存储和同步策略，例如 Redis、SQL、向量库或对象存储。
5. 必须给出一条完整消息链路的时序说明，包含 `trace_id` 或 `request_id` 如何贯穿链路。
6. 必须列出至少 8 个生产风险和缓解措施。
7. 方案需要明确哪些能力可直接复用 tRPC-Agent-Go，哪些需要新增平台层模块。

## 可直接复用的 tRPC-Agent-Go 能力对照

| 平台需求 | 可复用的框架能力 | 需要新增的平台层 |
| --- | --- | --- |
| Agent 编排 | `agent/llmagent`、`agent/graph`、Chain / Parallel / Cycle | 租户级 Agent 注册、发布与路由 |
| 执行入口 | `runner.Runner`（流式 Event、context 取消） | 多租户 Worker 调度、无状态水平扩展 |
| Session / Memory / Artifact / Knowledge | `session`、`memory`、`artifact`、`knowledge` 及多后端实现 | 租户级后端选择、数据隔离与迁移 |
| Tool / MCP / Skill | `tool`、MCP Tool、`skill` | 租户工具白名单与密钥注入 |
| 治理 | Plugin / Guardrail / Callbacks | 租户策略下发、预算与审批 |
| 服务化 | `server/openai`、`server/agui`、`server/a2a`、`server/trpcagent` | 统一 Gateway、Admin API |
| IM 接入 | OpenClaw Gateway + Channel | 微信 / 企业微信等通道与租户绑定 |
| 可观测性 | OpenTelemetry tracing / metrics | 租户维度审计、成本与合规 |

## 实现进度清单

> 本清单用于跟踪 README 所述平台能力的工程落地情况。`[x]` 表示已经实现并有代码或测试支撑，`[ ]` 表示尚待完成；设计文档完成不等同于对应生产代码已经实现。

### 工程基础

- [x] 建立 Go module、命令行入口和基础目录结构
- [x] 提供构建、启动、停止、格式化、静态检查和覆盖率脚本
- [x] 配置 Go CI、Codecov 和 MkDocs 文档 CI
- [x] 将命令行入口改造成持续运行的服务，并支持优雅停机（Issue #41）
- [x] 增加显式、幂等且并发安全的首次 Tenant/App 初始化命令（Issue #67）
- [x] 增加 Dockerfile、Docker Compose 最小部署和 Kubernetes 生产部署清单（Issue #74）
- [x] 增加配置示例、环境变量说明和可验证的部署快速开始（Issue #74）
- [x] 增加开发专用、可重复的本地 golden path，从空库验证第一条 deterministic `/v1/chat`（Issue #101）

### 多租户控制面

- [x] 实现租户根模型、配额、审计保留策略、脱敏级别和生命周期状态
- [x] 实现租户配置校验、乐观锁版本和状态转换规则
- [x] 定义租户 Repository 接口并提供并发安全的 InMemory 实现
- [x] 实现不可变运行时配置快照和带租户命名空间的 Runner 用户/会话身份
- [x] 覆盖租户隔离、并发更新、Context 取消和运行时边界测试
- [x] 实现 PostgreSQL Tenant、Agent、Model、Backend、Channel Binding Repository（Issue #37）
- [x] 实现 MySQL 租户及控制面 Repository（Issue #81；契约、验收对照与验证见 `docs/docs/issue-81-mysql-control-plane.md`）
- [x] 实现租户级 Agent App 根模型和生命周期
- [x] 实现 LLMAgent Revision 版本模型、草稿、不可变发布和内容摘要
- [x] 定义 Agent Repository 并提供租户隔离、并发安全的 InMemory 实现
- [x] 实现租户级 Channel Binding 领域模型、候选索引与可信入站路由边界
- [x] 实现 Backend Profile 领域模型
- [x] 完成 Model Profile、Secret Resolver 与最小 Runner 链路设计
- [x] 实现 Model Profile 控制面、Secret Resolver 契约和最小 Runner 链路
- [x] 实现租户、Agent、通道和后端配置的 Admin API（Issue #41）
- [x] 实现 Agent App 草稿更新、原子发布和版本回滚
- [x] 实现配置缓存失效、租户级灰度和租户配置版本回滚（Issue #82；当前灰度为租户/App 全量候选 revision 选择，不含百分比分流）
- [x] 接入 KMS/Secret Manager，禁止密钥进入运行时快照、日志和 trace（SecretManager 契约与 Vault KV v2 adapter）

### Gateway 与 Agent Worker

- [x] 引入并初始化 tRPC-Agent-Go，建立可执行的 `runner.Runner`
- [x] 将 Tenant、Agent Revision、Model Profile 和 Backend Profile 组合成 Execution Plan
- [x] 实现 Agent Registry，按租户/App 的 stable 或 canary revision 加载 Agent（Issue #82；Runner 仍按完整 ExecutionPlan key 复用）
- [x] 实现 Tenant + App + Revision 不可变执行快照、Factory Cache Key 和无密钥 Factory 输入契约
- [x] 实现 Gateway 的鉴权、租户解析、Agent 路由、限流和请求去重
- [x] 实现普通及流式对话 API，并贯穿 `request_id` / `trace_id`
- [x] 实现无状态 Worker 和基于共享 Session/Memory 后端的水平扩展（Issue #76；队列 Worker + 共享 RuntimeStore 契约）
- [x] 实现 Durable execution queue 的 lease、fencing、retry 和 shutdown（Issue #76）
- [x] 实现 `context.Context` 取消、Runner Event 通道排空和 goroutine 生命周期管理
- [x] 实现健康检查、readiness、优雅摘流和服务关闭（生产入口缺少 bootstrap 配置时快速失败）

### 数据模型、多后端与同步

- [x] 完成 Tenant 根模型的 PostgreSQL DDL、约束、生命周期和框架映射设计
- [x] 完成 Agent App/Revision 的 PostgreSQL DDL、发布事务、回滚和框架映射设计
- [x] 完成 Backend Profile 的 PostgreSQL DDL、生命周期、运行时快照和框架映射设计
- [x] 完成 Model Profile 的无密钥 PostgreSQL 目标形状、Repository 事务和启动装配设计（Issue #37）
- [x] 将 Tenant、Agent App/Revision、Model、Backend、Channel Binding DDL 和受控写入口落地为 PostgreSQL migration（Issue #37）
- [x] 补齐 channel binding、session、event、memory、summary、artifact 和 audit log 的可执行 DDL
- [x] 定义 Session、Memory、Summary、Knowledge、Artifact 和 Audit 的统一访问接口
- [x] 实现租户级 Backend Registry/Factory 和后端路由
- [x] 接入 InMemory 与 PostgreSQL Session/Event/Reply Outbox 运行时后端（Issue #49/#50）
- [x] 接入 PostgreSQL runtime vector index/object storage 与 InMemory adapter
- [x] 实现同一 Session 并发写入的版本/CAS/事务控制和事件序号（Issue #49）
- [x] 明确并实现 event、state、summary 的更新顺序及冲突重放
- [x] 实现 Memory 写入后的跨节点可见性和异步向量索引
- [x] 实现 Redis 到 SQL、以及本地到远端向量库的租户级迁移流程（Issue #76；provider-neutral adapter）
- [x] 实现全量复制、双写、增量追平、校验、切读和回滚工具（Issue #76）
- [x] 实现 Issue #76 Redis/SQL、向量迁移与租户级 cutover/rollback 工具

### IM Channel Adapter

- [x] 定义统一 Channel Adapter 生命周期、共享入站消息和持久化出站回复契约（Issue #60）
- [x] 实现外部消息到 `model.Message` / `runner.Runner.Run` 的转换
- [x] 将 Runner Event 聚合为文本并通过 Reply Outbox 投递到 Telegram/企业微信
- [ ] 实现 IM 流式消息和卡片消息的转换
- [x] 接入企业微信自建应用文本 webhook（Issue #60；仅直聊文本）
- [x] 接入 Telegram long polling 文本通道（Issue #31；单 Binding、Gateway Dispatch、进程内幂等）
- [x] 增加真实 Telegram live E2E 示例与 CI workflow（Issue #33；根目录 `examples/telegram-e2e`）
- [ ] 接入 Telegram webhook、媒体/rich update 或其他 IM 通道
- [x] 实现企业微信 webhook 验签、账号与租户 Binding、用户身份映射（Issue #60）
- [x] 使用 `tenant + channel + message_id` 实现幂等去重和缓存回复（Issue #49）
- [x] 实现单聊/群聊 Session ID 规则及跨群、跨租户隔离
- [x] Telegram 文本回复分段、重复投递和论坛线程路由
- [x] 处理文本回复的异步 Outbox、重试、死信和失败恢复（Issue #50/#52；媒体/撤回仍未实现）
- [x] 增加企业微信重复投递、乱序、验签失败和跨租户访问测试（Issue #60）

### 治理、安全与可观测性

- [ ] 使用 Plugin / Guardrail / Callbacks 实现租户级治理链
- [ ] 实现工具白名单、调用前鉴权、密钥注入和危险操作二次确认
- [ ] 实现 IM 用户权限校验、敏感信息脱敏和租户级预算控制
- [x] 实现包含 README 指定字段的不可篡改审计日志（Issue #54/#55）
- [x] 接入运行时 OpenTelemetry tracing、metrics 和结构化日志（Issue #45 阶段 A）
- [x] 串联 HTTP/IM callback、Gateway、Runner、Model、Tool、Storage 和 IM reply 的 trace（Issue #79 / PR #85；含流式模型调用、创建流失败和 WeCom context 继承）
- [x] 采集请求量、延迟、终态错误率、IM 成功/重试/死信、token、成本和实际 Session/Storage 后端延迟指标（Issue #79 / PR #85）
- [x] 提供通过授权 query adapter 的租户 usage dashboard、平台运维 process dashboard、告警规则并控制 provider/channel/model label 基数（Issue #79 / PR #85）
- [x] 提供 Prometheus 可抓取的运行时指标路径，并保持默认 no-op 配置兼容（Issue #88 / PR #89；通过 OTLP Collector → Prometheus）
- [x] 持久化并恢复 Reply Outbox 的 W3C trace parent（Issue #91 / PR #92）

### 可靠性、运维与测试

- [x] 覆盖 Agent App/Revision 边界、租户隔离、发布回滚、并发冲突、Context 取消和执行快照测试
- [x] 对 Agent App 和 InMemory Repository 运行 race 与重复稳定性测试
- [ ] 实现模型、工具、数据库和 IM 故障的超时、重试、熔断及降级策略
- [x] 实现 IM 异步重试队列、指数退避和死信处理（Issue #50）
- [ ] 完成容量模型，并对并发 Session、Redis/SQL QPS 和 IM 峰值进行压测
- [ ] 完成备份恢复、故障演练和租户级发布回滚流程
- [x] 增加 Storage Adapter 契约测试和端到端消息链路测试（Issue #49/#50/#52）
- [x] 增加多租户越权、密钥泄漏、并发一致性和故障注入测试（Issue #99；确定性示例覆盖 Gateway 边界、Outbox 与 Runner Registry）
- [x] 在 CI 中运行 `go test -race ./...`（Issue #39）；Codecov project/patch 的 85% 目标仍待分支保护或 ruleset 设为合并门禁
- [ ] 增加依赖漏洞、镜像和提交密钥扫描

### 设计交付物

- [x] 将架构设计扩充为 2000–4000 字的完整方案（Issue #24，文档设计已完成）
- [x] 补全包含 Gateway、Worker、Channel、Storage、Guardrail、Telemetry 和外部系统的架构图
- [x] 补全“企业微信消息 → Agent → Tool → Session/Memory → IM 回复”核心时序图
- [x] 完成数据同步、消息幂等和多后端一致性取舍专项说明
- [x] 对比至少两种 IM 通道的协议、限制和回复机制
- [x] 列出至少 8 个生产风险及对应缓解措施
- [x] 持续标注可直接复用的 tRPC-Agent-Go 能力与平台新增模块边界
- [x] 完成 Issue #37 PostgreSQL 控制面 migration、Repository、bootstrap 和 readiness 的文档契约
- [x] 完成 Issue #41 Bootstrap、readiness、Admin API 和重启恢复的文档先行契约
- [x] 完成 Issue #45 运行时 observability、指标、脱敏日志和 OTLP 配置契约
- [x] 完成 Issue #54 租户审计、用量成本、失败处理、保留与访问控制的文档先行契约
- [x] 实现 Issue #54 租户审计、用量成本、失败处理、保留与访问控制（PR #55）
- [x] 完成 Issue #75 Runtime Capabilities 的统一接口、PostgreSQL/InMemory 实现和租户边界契约
- [x] 完成 Issue #79/#88 运行时可观测性、Prometheus 导出、Dashboard 与告警契约
- [x] 完成 Issue #81 MySQL 控制面 Repository、迁移和最小权限契约
- [x] 完成 Issue #82 Agent App Registry、租户级 candidate revision 与 lease-safe 回滚契约
- [x] 完成 Issue #91 Reply Outbox 跨进程 Trace Context 契约

> Issue #24 只完成架构、数据模型和运维文档；当前仓库另外交付了 Issue #28 的 Gateway/API、
> Issue #31 的 Telegram long polling 文本 Adapter、Issue #37/#41 的 PostgreSQL 控制面与 Admin/
> Bootstrap、Issue #45 的运行时 observability 阶段 A，以及 Issue #49/#50 的 Session/Event/
> Reply Outbox 持久化和可靠投递。Issue #75 已补齐租户级 Memory、Summary、Knowledge、Artifact、
> Audit、Vector 和 Object runtime capabilities；Issue #79/#88 已补齐运行时 trace、metrics、
> dashboard/alert 及 Prometheus 导出路径；Issue #91 已补齐跨 Outbox trace-parent 传递；Issue
> #81/#82 分别交付了 MySQL 控制面和实例内 Agent App Registry。Telegram webhook、多账号/HA
> Channel 调度、媒体/rich update、Redis/向量库/对象存储迁移工具、无状态 Worker 水平扩展和治理
> 策略链仍未实现。
> 业务审计与用量成本已由 Issue #54/PR #55 交付。

## 当前实现记录（不改变原验收要求）

> 以下内容仅索引当前实现的代码、文档和测试范围，不替代、收窄或修改上方原验收要求；
> 上方勾选仅表示该原条目已有代码或测试证据。当前实现已合并到主线，但仍不等同于全部原验收项已完成。

- `trpcservice/gateway/auth.go` 的 proof-bearing API 身份校验对应
  `trpcservice/gateway/auth_test.go`；`resolver.go` 对应 `resolver_test.go`。
- 当前实现包含 Gateway、HTTP/SSE、进程内限流/幂等和 Channel trusted-principal 的阶段性代码，
  具体边界以 `docs/docs/gateway.md` 为准。
- Issue #31 的 `trpcservice/channels/telegram` 提供单 Binding、`getMe` 身份校验、普通文本
  long polling、Gateway Dispatch、进程内幂等和脱敏分段回复；具体边界以
  `docs/docs/telegram.md` 为准。
- Issue #33 的 `examples/telegram-e2e` 使用真实 Telegram Bot API 和确定性 Dispatcher 验证
  `getMe -> getUpdates -> sendMessage`；live workflow 只手动触发并使用受保护 Environment，
  不替代完整模型供应商或生产控制面 E2E。
- Issue #37/#41 的 PostgreSQL 控制面、可重启 Bootstrap、readiness 和最小 Admin API 已合并；
  Issue #75 的 Memory/Knowledge/Artifact 等 runtime capabilities 通过独立的
  `trpcservice/runtime/storage` 契约接入 InMemory 与 PostgreSQL，审计事实仍由 Issue #54/PR #55
  单独持久化。
- Issue #45 的运行时 observability 阶段 A 已合并，提供 no-op/OTLP provider、低基数指标、
  trace context 和脱敏结构化日志；Issue #54/PR #55 已补齐 Stage B 的 AuditEvent、usage/cost、
  审计持久化、执行/IM 生产者和失败处理。
- Issue #52 的 examples/telegram-e2e 增加 Runner -> reply outbox -> Telegram Provider ->
  provider_message_id -> sent -> message_event=replied 的受保护 live E2E；它不替代无凭证的
  确定性单元测试，也不承诺外部 Telegram exactly-once 投递。
- Issue #75 的 runtime capabilities 使用显式 `tenant_id` 和复合键；PostgreSQL 版本提供
  Memory/Summary/Knowledge/Artifact/Audit/Vector/Object 存储，InMemory 版本提供共享 backend
  视图、异步向量索引和相同的租户边界。它不等同于 Redis、独立向量数据库或对象存储服务的适配器。
- Issue #79/#88 提供低基数 OpenTelemetry telemetry、OTLP Collector 到 Prometheus 的导出路径、
  Grafana Dashboard/Tempo 查询资源和 tenant-authorized usage query adapter；tenant ID 等高基数
  关联不会进入 Prometheus label。
- Issue #91 将经过 W3C 校验的 `traceparent` 写入 ReplyCorrelation，并由 Outbox Worker 在
  `channel.send` 前恢复；旧记录或非法 parent 会从新的 root span 投递，不阻塞回复。
- Issue #99 的 `examples/fault-injection-e2e` 使用受控 fake 验证伪造租户路由、机密错误脱敏、
  Reply Outbox 重试/租约竞争/原子 materialization，以及租户隔离的并发 Runner 构建；不依赖网络或生产凭据。
- Issue #26 的 fake candidate resolver/verifier 与 proof-bearing routing 边界有独立测试，
  但这不代表 Telegram webhook、多账号/HA Channel 调度、媒体能力、治理链或完整平台验收
  已满足 README 原验收要求。

> Issue #75（运行时 Memory/Knowledge/Artifact 等能力）已由 PR #90 合并，Issue #91
>（Outbox trace parent）已由 PR #92 合并；它们已计入上方完成度。后续开放 PR 不计入
>完成度，直到合并并有对应验证证据。
> 当前基线仍不包含完整的 Redis/外部向量库/对象存储迁移工具、百分比分流、无状态 Worker
> 水平扩展、IM webhook/rich update 和 Plugin/Guardrail 治理链；这些能力继续保留在上方未完成清单。

## 代码目录

下面是当前仓库的主要目录，按职责列出；测试文件和各后端的 codec/storage 辅助文件未逐一展开。

```txt
|-- README.md              # 说明文档，包含设计、安装、使用
|-- go.mod                 # Go module 定义
|-- .github
|   `-- workflows          # CI 流程（PR / push 触发）
|-- scripts
|   |-- build.sh           # 构建项目
|   |-- clean.sh           # 清理中间产物
|   |-- coverage.sh        # 运行单测覆盖率
|   |-- race.sh            # 运行完整模块的 race 检测
|   |-- format.sh          # 格式化 Go 代码（--check 为 CI 校验模式）
|   |-- lint.sh            # 静态检查
|   |-- quickstart.sh      # Docker Compose 可验证快速开始
|   |-- start.sh           # 启动服务
|   |-- stop.sh            # 停止服务
|   `-- validate-deployment.sh # 部署清单和 build context 预检
|-- Dockerfile             # 非 root distroless 服务镜像
|-- deploy                 # Compose、配置示例和 Kubernetes 清单
|   `-- observability       # OTel Collector、Prometheus、Tempo、Grafana 配置
|-- data                   # 服务运行时数据
|-- migrations             # PostgreSQL/MySQL schema 与迁移校验
|-- examples               # 可运行的外部集成示例
|   |-- fault-injection-e2e # 确定性 Gateway/Outbox/Registry 故障注入 E2E
|   |-- telegram-e2e        # Telegram live long-polling/outbox E2E
|   `-- wecom-e2e           # 企业微信回调示例测试
|-- docs                   # 各模块说明与架构设计文档
|-- cmd
|   `-- trpc-service       # 命令行入口，可直接启动服务
`-- trpcservice            # 源码
    |-- admin              # Admin API 与认证
    |-- agent              # Agent App/Revision 模型与 Repository
    |-- audit              # 审计事件与持久化
    |-- backend             # Backend Profile、Capability Registry/Factory
    |-- bootstrap           # 数据库、Provider、Runtime 和 HTTP 装配
    |-- channels            # Telegram/企业微信 Channel Adapter
    |-- config              # 服务配置模型
    |-- controlplane        # PostgreSQL 控制面测试 facade
    |-- gateway             # 鉴权、路由、Runner Registry、HTTP/SSE
    |-- log                 # 结构化日志与脱敏
    |-- metrics              # 运行时指标与 Audit telemetry wrapper
    |-- model               # Model Profile、Secret Resolver/Registry
    |-- observability       # Trace、metrics、OTLP provider 与访问控制
    |-- runtime              # Execution、Session、Outbox、Runtime Storage
    |-- storage              # PostgreSQL/MySQL 通用存储与错误映射
    |-- skill                # Skill 接口
    |-- tenant               # 多租户模型、Repository 与运行时边界
    |-- tool                 # 平台 Tool
    |-- version.go           # 版本信息
    |-- web                  # 管理 / 对话页面入口
    `-- workspace             # 工作目录入口
```

## CI

`.github/workflows/ci.yml` 在 push 到 `main` 和提交 PR 时自动运行：

- **Format & Lint**：`gofmt` 校验、`go vet`、`golangci-lint`
- **Build, Test & Coverage**：构建、单测覆盖率、上传 [Codecov](https://codecov.io)、清理
- **Race Tests**：在临时 PostgreSQL 与 MySQL 8 依赖上执行 `go test -race ./...`；MySQL migration/repository live 测试使用独立 migration 账号和表级 DML 白名单应用账号
- **Deployment**：校验 Docker Compose/Kustomize 清单，构建镜像并运行 PostgreSQL + 服务 golden path，验证 `/healthz`、`/readyz` 和第一条 `/v1/chat`

独立的 `.github/workflows/fault-injection-e2e.yml` 会在 `main` 的 push、面向 `main` 的 PR 和手动触发时运行：

```bash
go test ./examples/fault-injection-e2e -count=1 -v
go test -race ./examples/fault-injection-e2e -count=1 -v
```

该套件不使用生产凭据或外部网络服务；覆盖矩阵和受控 failure hooks 见
[`examples/fault-injection-e2e/README.md`](examples/fault-injection-e2e/README.md) 与
[`docs/docs/issue-99-fault-injection.md`](docs/docs/issue-99-fault-injection.md)。

完整的环境变量参考、Kubernetes Secret 约束和 Compose/Kubernetes 操作步骤见
[部署、配置与快速开始](docs/docs/deployment.md)。

Codecov 对 project 和 patch 状态均使用 **85%**、零容差的报告目标。它会将状态发布到 PR；要使已发布的
状态成为合并门禁，仓库管理员还需在 GitHub 分支保护或 ruleset 中要求对应的状态（当前仓库尚未配置该规则）。
覆盖率上传使用仓库 Secret `CODECOV_TOKEN`（见 Codecov 仓库设置页获取）；`scripts/coverage.sh` 只产出上传
工件，并不实现本地 `--min` 门禁。

## 快速开始

服务启动会调用 `bootstrap.NewFromEnvironment`，缺少数据库、身份、Admin 或模型配置时会在绑定 HTTP
端口前失败。下面提供 Compose 快速开始；源码模式适用于连接自有 PostgreSQL、模型和身份配置。

### Docker Compose（推荐的最小可运行部署）

```bash
git clone https://github.com/XnLemon/trpc-agent-service.git
cd trpc-agent-service

cp deploy/example.env deploy/service.env
./scripts/quickstart.sh
```

默认参数模板是仓库内的 [`deploy/example.env`](deploy/example.env)；复制后的
`deploy/service.env` 只用于本地覆盖，不应提交真实凭据。

脚本会构建服务镜像，等待 PostgreSQL 和服务健康检查，并验证 `/healthz`、`/readyz`；成功后
服务继续运行在 `http://127.0.0.1:8080`。`deploy/service.env` 仅供本地使用，已被 Git 和
Docker build context 忽略，不能提交真实凭据。

要在空数据库上直接验证第一条对话，使用开发专用 golden path：

```bash
./scripts/quickstart.sh --demo
```

它会先运行 `trpc-service demo --confirm`，幂等创建或复用 Tenant、Agent App、离线
deterministic Model、InMemory Backend 和已发布 Revision，然后以 fake provider 启动服务，
并实际 POST `/v1/chat` 断言固定响应 `Hello from the tRPC Agent Service demo.`。命令输出只含
资源 ID 与非敏感运行时变量，不输出 DSN、token 或模型密钥；可重复执行，发现歧义状态时会
fail closed。该路径只支持 PostgreSQL、仅用于本地开发，不会在普通服务启动时自动建资源。

默认运行（不带 `--demo`）仍只验证数据库迁移、bootstrap 和 HTTP 部署入口，不会创建任何控制面
资源。生产或真实模型场景请按 [Issue #67 首次运行初始化](https://xnlemon.github.io/trpc-agent-service/issue-67-first-run-init/)
执行显式初始化，再通过 Admin API 创建并发布运行配置。

### 源码模式

需要连接自己的 PostgreSQL、模型和身份配置时，可以直接构建并启动 Go 服务：

```bash
./scripts/build.sh

export TRPC_POSTGRES_DSN='postgres://postgres:postgres@127.0.0.1:5432/trpc_control_plane?sslmode=disable'
export TRPC_INIT_TENANT_KEY='local'
export TRPC_INIT_TENANT_NAME='Local Tenant'
export TRPC_INIT_APP_KEY='assistant'
export TRPC_INIT_APP_NAME='Local Assistant'
./bin/trpc-service init --confirm
```

将初始化命令输出的 `TRPC_TENANT_ID` 和 `TRPC_APP_ID` 作为服务配置，并补齐运行时必需项：

```bash
export TRPC_API_TOKEN='replace-with-api-token'
export TRPC_TENANT_ID='t_...'
export TRPC_APP_ID='app_...'
export TRPC_ADMIN_TOKEN='replace-with-admin-token'
export TRPC_ADMIN_TENANTS='*'
export TRPC_MODEL_API_KEY='replace-with-model-key'
export TRPC_SESSION_BACKEND='postgres'

./scripts/start.sh
```

控制面默认使用 PostgreSQL，runtime storage 可选 `postgres` 或 `inmemory`；切换 MySQL 时必须同时提供应用账号
`TRPC_MYSQL_DSN` 和迁移账号 `TRPC_MYSQL_MIGRATION_DSN`，Bootstrap 会在绑定 HTTP
端口前完成迁移、schema 和权限校验。迁移账号需要目标数据库的 DDL 权限；应用账号只
授予控制面 14 张表各自完整的表级 `SELECT/INSERT/UPDATE/DELETE`（缺失任何一项也会拒绝），
不授予额外表、全局、schema、列级、routine/`EXECUTE` 或 `PROXY` 权限，也不授予
`CREATE/ALTER/DROP/TRIGGER` 等 DDL 权限。两个 DSN 必须实际登录为不同 MySQL 账号并指向
同一个数据库；Bootstrap 会拒绝超出白名单的直接/角色权限、启用角色权限或 grant option。

首次运行初始化命令只支持 PostgreSQL；MySQL 控制面会在服务启动时自动执行迁移，但当前 MySQL
runtime storage 只能使用 `TRPC_SESSION_BACKEND=inmemory`。正常启动不会自动创建 Tenant 或 Agent App，
具体初始化边界见 [Issue #67 首次运行初始化](https://xnlemon.github.io/trpc-agent-service/issue-67-first-run-init/)。

停止服务：

```bash
./scripts/stop.sh
```
