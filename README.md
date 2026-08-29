<div align="center">
  <h1>tRPC Agent Service</h1>
  <p>面向生产落地的多租户、节点化 Agent 部署平台</p>

  <p>
    <a href="https://github.com/XnLemon/trpc-agent-service/actions/workflows/ci.yml"><img src="https://github.com/XnLemon/trpc-agent-service/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
    <a href="https://github.com/XnLemon/trpc-agent-service/actions/workflows/docs.yml"><img src="https://github.com/XnLemon/trpc-agent-service/actions/workflows/docs.yml/badge.svg?branch=main" alt="Docs"></a>
    <a href="https://codecov.io/gh/XnLemon/trpc-agent-service"><img src="https://codecov.io/gh/XnLemon/trpc-agent-service/branch/main/graph/badge.svg" alt="Codecov"></a>
    <a href="https://github.com/XnLemon/trpc-agent-service/actions/workflows/publish-image.yml"><img src="https://github.com/XnLemon/trpc-agent-service/actions/workflows/publish-image.yml/badge.svg" alt="Container image"></a>
  </p>
</div>

**tRPC Agent Service** 将 [tRPC-Agent-Go](https://github.com/trpc-group/trpc-agent-go) 的 Agent、Runner、Tool 和 Session 能力装配成一个可部署的平台服务。它把租户、Agent 应用、模型、数据后端和 IM 通道纳入同一套受控配置，并通过版本化执行快照、共享运行时状态、幂等和可观测性支撑多节点运行。

它适合希望把单个 Agent 原型演进为团队或企业服务的场景：对话 API、企业微信/Telegram 接入、按租户的配置与审计，以及 Docker Compose 和 Kubernetes 部署都由同一套平台边界承载。

## 为什么使用它

- **多租户控制面**：Tenant、Agent App、Revision、Model Profile、Backend Profile 和 Channel Binding 均有明确的租户边界、版本和状态迁移规则。
- **无状态 Agent Worker**：Gateway 负责鉴权、路由、限流、幂等和执行计划；Worker 使用共享 Session/Event/Outbox 状态，可以独立扩展和恢复。
- **安全的配置链路**：控制面只保存无密钥配置和 `secret_ref`；模型密钥、IM 凭据和数据库连接信息在受控 resolver 边界注入，不进入快照、日志或 trace。
- **可演进的运行时**：Model/Backend/Channel 使用 provider registry 和 capability adapter，配置发布、回滚、灰度和缓存失效保持可审计。
- **服务化入口**：提供普通 `/v1/chat`、SSE `/v1/chat/stream`、Admin API、健康检查和 readiness；企业微信与 Telegram 适配器复用同一 Gateway 执行链路。
- **可观测与可运维**：贯穿请求、模型、工具、存储和回复投递的 trace、metrics、结构化日志、Outbox 重试和死信边界。

## 架构总览

控制面管理“允许执行什么”，数据面处理“现在执行什么”。一次请求在建立可信租户主体后，加载不可变的 `ExecutionPlan`，再交给 Runner/Agent/Tool 和租户作用域的存储能力。

![tRPC Agent Service production architecture](docs/docs/assets/architecture-overview.png)

```text
Admin API -> SQL Control Plane -> Registry / Cache -> Execution Plan
                                                     |
IM / HTTP -> Channel Adapter -> Gateway -> Queue/Outbox -> Agent Worker
                                                     |
                                    Runner -> Model / Tool / Guardrail
                                                     |
                         Session / Event / Memory / Knowledge / Artifact / Audit
```

详细的组件职责、可信路由和消息时序见[生产架构设计](docs/docs/architecture.md)。

## 当前能力

当前仓库提供一条可运行的服务骨架和完整的控制面纵向链路：

- PostgreSQL 控制面、migration、显式 `init` 初始化和受认证的 Admin API；
- Tenant/App/Revision 的草稿、发布、回滚、灰度候选和乐观锁；
- OpenAI 模型 provider，以及不访问外部服务的 deterministic fake provider；
- InMemory 与 PostgreSQL runtime storage、Session/Event、Reply Outbox 和租约恢复；
- 普通及流式 HTTP Chat API，企业微信自建应用文本 webhook，Telegram 文本 long polling；
- OpenTelemetry trace/metrics、Prometheus 导出路径、审计事件和脱敏错误；
- Docker Compose 本地验证、Kubernetes Kustomize base，以及版本 tag 触发的 GHCR 镜像发布。

以下能力仍不是当前默认生产路径：Redis 或独立向量/对象存储 provider 的完整装配、IM 媒体与 rich update、完整 Plugin/Guardrail 治理链、容量压测、备份恢复和故障演练。它们的设计边界和后续路线记录在专项文档中。

## 快速开始：离线 Golden Path

该路径使用 PostgreSQL、fake model 和固定响应验证从空数据库到第一条对话，不需要 OpenAI、IM 或 Secret Manager 凭据。

```bash
git clone https://github.com/XnLemon/trpc-agent-service.git
cd trpc-agent-service

./scripts/quickstart.sh --demo
```

脚本会构建镜像、启动 PostgreSQL、幂等创建 Tenant/App/Model/Backend/Revision，等待 `/healthz` 和 `/readyz`，然后实际调用 `/v1/chat`。成功响应为：

```json
{
  "text": "Hello from the tRPC Agent Service demo.",
  "status": "complete",
  "done": true
}
```

发送一条自己的请求：

```bash
body='{"content":"hello from the local golden path","external_user_id":"quickstart-user","conversation_kind":"direct","external_peer_id":"quickstart"}'

curl -i \
  -H 'Authorization: Bearer local-api-token' \
  -H 'Content-Type: application/json' \
  --data "$body" \
  http://127.0.0.1:8080/v1/chat
```

`--demo` 是开发验收入口，不会自动替代真实部署。Windows/WSL、Docker 清理和完整验证说明见[部署、配置与快速开始](docs/docs/deployment.md)。

## 真实模型与生产部署

真实部署遵循“显式初始化 → Admin API 配置 → 发布 Revision → 正常启动”的顺序：

1. 准备 PostgreSQL、模型 provider、API/Admin token 和 Secret 管理方案；
2. 使用 `trpc-service init --confirm` 创建首个 Tenant 和 draft App；
3. 通过 Admin API 创建 Model Profile、Backend Profile 和 Agent Revision，并发布 Revision；
4. 将生成的 Tenant/App ID 和运行时凭据注入服务，启动非 demo 模式；
5. 通过 `/readyz`、普通 API 和审计/metrics 验证服务。

准备好 Secret、已发布的镜像 tag/digest 和 Kubernetes overlay 后：

```bash
# 生产镜像由版本 tag 触发 .github/workflows/publish-image.yml 发布到 GHCR。
# Kubernetes 部署使用已发布的 tag 或 digest，不要使用本地 CI 镜像名。
kubectl apply -k deploy/kubernetes
```

从零配置、Secret 约束、Kubernetes overlay 和 Admin API 字段见[部署文档](docs/docs/deployment.md)与[首次运行初始化](docs/docs/issue-67-first-run-init.md)。

## 可观测性

运行时 dashboard、trace 和 metrics 示例位于 `docs/docs/assets/issue-88/`：

| Metrics | Runtime dashboard | Trace |
| --- | --- | --- |
| ![metrics](docs/docs/assets/issue-88/metrics-explore.png) | ![runtime dashboard](docs/docs/assets/issue-88/runtime-dashboard.png) | ![trace](docs/docs/assets/issue-88/trace-explore.png) |

这些截图对应[生产可观测性文档](docs/docs/issue-79-observability.md)和 [Prometheus 说明](docs/docs/issue-88-prometheus.md)。

## 项目结构

```text
cmd/trpc-service/       CLI：init、demo 和服务进程
trpcservice/bootstrap/  数据库、provider、runtime 和 HTTP 装配
trpcservice/tenant/     多租户模型与 repository
trpcservice/agent/      Agent App、Revision 与发布
trpcservice/model/      Model Profile、Secret Resolver、Factory
trpcservice/backend/    Backend Profile、Capability Registry/Factory
trpcservice/gateway/    鉴权、路由、Execution Plan、HTTP/SSE
trpcservice/channels/   Telegram、企业微信 Channel Adapter
trpcservice/runtime/    Session、Event、Queue、Outbox、Storage
trpcservice/admin/      Admin API 与管理员认证
migrations/             PostgreSQL/MySQL schema 与 migration
deploy/                 Compose、Kubernetes、OTel/Prometheus 配置
examples/               fault-injection、Telegram、WeCom E2E
docs/                   架构、协议、运维和验收文档
```

## 开发与验证

需要 Go 1.21 或更高版本。常用本地检查：

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
python -m mkdocs build --strict -f docs/mkdocs.yml
```

CI 在 push/PR 时执行格式、静态检查、测试、覆盖率、race 和部署 smoke test；故障注入、Telegram live E2E、WeCom E2E 和文档构建有独立 workflow。提交代码前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 文档导航

- [生产架构设计](docs/docs/architecture.md)：控制面、数据面、可信路由、执行计划和恢复模型
- [部署、配置与快速开始](docs/docs/deployment.md)：Compose、Kubernetes、环境变量和 GHCR 镜像
- [首次运行初始化](docs/docs/issue-67-first-run-init.md)：`trpc-service init` 与幂等边界
- [Gateway、Execution Plan 与 HTTP/SSE](docs/docs/gateway.md)：请求契约、鉴权、限流和流式响应
- [PostgreSQL 控制面与启动装配](docs/docs/postgresql-control-plane.md)：migration、repository 和 bootstrap
- [原始任务书](docs/docs/project-brief.md)：项目最初的背景、要求、交付物和验收标准
- [完整文档站](https://xnlemon.github.io/trpc-agent-service/)

## 许可证

本仓库当前未附带正式许可证文件；在将代码用于外部发布或商业部署前，请先确认项目维护者的授权范围。
