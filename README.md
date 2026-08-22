# 基于 tRPC-Agent-Go 设计多租户节点化 Agent 部署平台

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
- [ ] 将命令行入口改造成持续运行的服务，并支持优雅停机
- [ ] 增加 Dockerfile、Docker Compose 最小部署和 Kubernetes 生产部署清单
- [ ] 增加配置示例、环境变量说明和可验证的端到端快速开始

### 多租户控制面

- [x] 实现租户根模型、配额、审计保留策略、脱敏级别和生命周期状态
- [x] 实现租户配置校验、乐观锁版本和状态转换规则
- [x] 定义租户 Repository 接口并提供并发安全的 InMemory 实现
- [x] 实现不可变运行时配置快照和带租户命名空间的 Runner 用户/会话身份
- [x] 覆盖租户隔离、并发更新、Context 取消和运行时边界测试
- [ ] 实现 PostgreSQL/MySQL 租户 Repository 和数据库 migration
- [x] 实现租户级 Agent App 根模型和生命周期
- [x] 实现 LLMAgent Revision 版本模型、草稿、不可变发布和内容摘要
- [x] 定义 Agent Repository 并提供租户隔离、并发安全的 InMemory 实现
- [ ] 实现 Channel Binding 领域模型
- [x] 实现 Backend Profile 领域模型
- [x] 完成 Model Profile、Secret Resolver 与最小 Runner 链路设计
- [ ] 实现 Model Profile 控制面、Secret Resolver 契约和最小 Runner 链路
- [ ] 实现租户、Agent、通道和后端配置的 Admin API
- [x] 实现 Agent App 草稿更新、原子发布和版本回滚
- [ ] 实现配置缓存失效、租户级灰度和租户配置版本回滚
- [ ] 接入 KMS/Secret Manager，禁止密钥进入运行时快照、日志和 trace

### Gateway 与 Agent Worker

- [ ] 引入并初始化 tRPC-Agent-Go，建立可执行的 `runner.Runner`
- [ ] 将 Tenant、Agent Revision、Model Profile 和 Backend Profile 组合成 Execution Plan
- [ ] 实现 Agent Registry，按 `tenant_id + agent_app_id + version` 加载 Agent
- [x] 实现 Tenant + App + Revision 不可变执行快照、Factory Cache Key 和无密钥 Factory 输入契约
- [ ] 实现 Gateway 的鉴权、租户解析、Agent 路由、限流和请求去重
- [ ] 实现普通及流式对话 API，并贯穿 `request_id` / `trace_id`
- [ ] 实现无状态 Worker 和基于共享 Session/Memory 后端的水平扩展
- [ ] 实现 `context.Context` 取消、Runner Event 通道排空和 goroutine 生命周期管理
- [ ] 实现健康检查、readiness、优雅摘流和服务关闭

### 数据模型、多后端与同步

- [x] 完成 Tenant 根模型的 PostgreSQL DDL、约束、生命周期和框架映射设计
- [x] 完成 Agent App/Revision 的 PostgreSQL DDL、发布事务、回滚和框架映射设计
- [x] 完成 Backend Profile 的 PostgreSQL DDL、生命周期、运行时快照和框架映射设计
- [ ] 将 Tenant、Agent App/Revision DDL 落地为数据库 migration
- [ ] 补齐 channel binding、session、event、memory、summary、artifact 和 audit log 的可执行 DDL
- [ ] 定义 Session、Memory、Summary、Knowledge、Artifact 和 Audit 的统一访问接口
- [ ] 实现租户级 Backend Registry/Factory 和后端路由
- [ ] 接入 InMemory、Redis 及 PostgreSQL/MySQL Session 后端
- [ ] 接入至少一种向量库和一种对象存储
- [ ] 实现同一 Session 并发写入的版本/CAS/事务控制和事件序号
- [ ] 明确并实现 event、state、summary 的更新顺序及冲突重放
- [ ] 实现 Memory 写入后的跨节点可见性和异步向量索引
- [ ] 实现 Redis 到 SQL、以及本地到远端向量库的租户级迁移流程
- [ ] 实现全量复制、双写、增量追平、校验、切读和回滚工具

### IM Channel Adapter

- [ ] 定义统一 Channel Adapter、入站消息和出站回复接口
- [ ] 实现外部消息到 `model.Message` / `runner.Runner.Run` 的转换
- [ ] 实现 Runner Event 到文本、流式消息和卡片消息的转换
- [ ] 接入企业微信或微信相关通道
- [ ] 再接入至少一种不同 IM 通道，例如 Telegram
- [ ] 实现 webhook 验签、账号与租户绑定、用户身份映射
- [ ] 使用 `tenant + channel + message_id` 实现幂等去重和缓存回复
- [ ] 实现单聊/群聊 Session ID 规则及跨群、跨租户隔离
- [ ] 处理消息分段、频率限制、异步回复、图片/文件、撤回和失败重试
- [ ] 增加重复投递、乱序、验签失败和跨租户访问测试

### 治理、安全与可观测性

- [ ] 使用 Plugin / Guardrail / Callbacks 实现租户级治理链
- [ ] 实现工具白名单、调用前鉴权、密钥注入和危险操作二次确认
- [ ] 实现 IM 用户权限校验、敏感信息脱敏和租户级预算控制
- [ ] 实现包含 README 指定字段的不可篡改审计日志
- [ ] 接入 OpenTelemetry tracing、metrics 和结构化日志
- [ ] 串联 IM callback、Gateway、Runner、Model、Tool、Storage 和 IM reply 的 trace
- [ ] 采集请求量、延迟、错误率、IM 成功率、token、成本和后端延迟指标
- [ ] 提供租户级 dashboard、告警规则并控制指标 label 基数

### 可靠性、运维与测试

- [x] 覆盖 Agent App/Revision 边界、租户隔离、发布回滚、并发冲突、Context 取消和执行快照测试
- [x] 对 Agent App 和 InMemory Repository 运行 race 与重复稳定性测试
- [ ] 实现模型、工具、数据库和 IM 故障的超时、重试、熔断及降级策略
- [ ] 实现 IM 异步重试队列、指数退避和死信处理
- [ ] 完成容量模型，并对并发 Session、Redis/SQL QPS 和 IM 峰值进行压测
- [ ] 完成备份恢复、故障演练和租户级发布回滚流程
- [ ] 增加 Storage Adapter 契约测试和端到端消息链路测试
- [ ] 增加多租户越权、密钥泄漏、并发一致性和故障注入测试
- [ ] 在 CI 中运行 `go test -race ./...`，并设置有效覆盖率门槛
- [ ] 增加依赖漏洞、镜像和提交密钥扫描

### 设计交付物

- [ ] 将架构设计扩充为 2000–4000 字的完整方案
- [ ] 补全包含 Gateway、Worker、Channel、Storage、Guardrail、Telemetry 和外部系统的架构图
- [ ] 补全“企业微信消息 → Agent → Tool → Session/Memory → IM 回复”核心时序图
- [ ] 完成数据同步、消息幂等和多后端一致性取舍专项说明
- [ ] 对比至少两种 IM 通道的协议、限制和回复机制
- [ ] 列出至少 8 个生产风险及对应缓解措施
- [ ] 持续标注可直接复用的 tRPC-Agent-Go 能力与平台新增模块边界

## 代码目录

下面只是一个示范目录，用来说明平台需要覆盖的职责分层。实现时不必严格按这个结构组织代码，只要模块边界清晰、能对应到设计方案即可。

```txt
|-- README.md              # 说明文档，包含设计、安装、使用
|-- go.mod                 # Go module 定义
|-- .github
|   `-- workflows          # CI 流程（PR / push 触发）
|-- scripts
|   |-- build.sh           # 构建项目
|   |-- clean.sh           # 清理中间产物
|   |-- coverage.sh        # 运行单测覆盖率
|   |-- format.sh          # 格式化 Go 代码（--check 为 CI 校验模式）
|   |-- lint.sh            # 静态检查
|   |-- start.sh           # 启动服务
|   `-- stop.sh            # 停止服务
|-- data                   # 服务运行时数据
|-- docs                   # 各模块说明与架构设计文档
|-- cmd
|   `-- trpc-service       # 命令行入口，可直接启动服务
`-- trpcservice            # 源码
    |-- agent              # 基于 tRPC-Agent-Go 的 Agent 定义
    |-- channels           # 对接 IM 的 Channel Adapter
    |-- config             # 租户与节点配置
    |-- log                # 日志级别与脱敏
    |-- metrics            # 监控指标
    |-- skill              # 可运行的 Skill
    |-- tenant             # 多租户模型与隔离
    |-- tool               # 平台 Tool
    |-- version.go         # 版本信息
    |-- web                # 管理 / 对话页面
    `-- workspace          # 工作目录，包含本地、容器等沙箱环境
```

## CI

`.github/workflows/ci.yml` 在 push 到 `main` 和提交 PR 时自动运行：

- **Format & Lint**：`gofmt` 校验、`go vet`、`golangci-lint`
- **Build, Test & Coverage**：构建、单测覆盖率、上传 [Codecov](https://codecov.io)、清理

覆盖率上传使用仓库 Secret `CODECOV_TOKEN`（见 Codecov 仓库设置页获取），未配置时上传步骤不阻断 CI。

## 快速开始

```bash
git clone https://github.com/XnLemon/trpc-agent-service.git
cd trpc-agent-service

./scripts/build.sh
./scripts/start.sh
```

停止服务：

```bash
./scripts/stop.sh
```
