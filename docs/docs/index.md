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
  固定模型解析、密钥边界、Execution Plan 和 tRPC-Agent-Go Runner 的纵向契约。
- [生产架构设计](architecture.md)：Issue #24 的控制面/数据面拓扑、可信 Channel Binding、
  企业微信全链路、幂等、迁移和能力边界设计。文档交付不等于对应生产代码已实现。

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
- [运维方案](ops.md) — 发布灰度、监控审计、故障恢复、容量模型和生产风险清单

## CI

仓库在 push 到 `main` 和 PR 时自动运行格式、静态检查、构建、测试覆盖率与文档构建,详见仓库根目录的 `.github/workflows/`。
