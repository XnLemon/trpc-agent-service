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

- [架构设计](architecture.md) — 组件拓扑、消息链路、租户隔离
- [数据模型](data-model.md) — 核心表结构、多后端适配、数据同步
- [运维方案](ops.md) — 监控审计、故障恢复、容量评估

## CI

仓库在 push 到 `main` 和 PR 时自动运行格式、静态检查、构建、测试覆盖率与文档构建,详见仓库根目录的 `.github/workflows/`。
