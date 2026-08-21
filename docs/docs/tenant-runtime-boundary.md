# Tenant 运行时边界

本页描述 issue #5 的第三个小 scope：怎样把 tenant 包的配置快照交给未来的 Gateway 和 Worker。它不实现 Gateway、Worker、IM Channel 或 tRPC-Agent-Go 的 Runner；这些能力应直接复用框架而不是由平台层重写。

## 一次执行的边界

1. Channel Adapter 验签、用绑定关系解析可信的 tenant_id，不得接受请求头或消息载荷中的 tenant ID。
2. 控制面以该 ID 读取 tenant，先执行 tenant.Validate，再创建 tenant.ConfigurationSnapshot。快照包含固定 version，不含模型 API key、IM token 或后端密码。
3. Gateway 用 tenant.WithConfigurationSnapshot 将快照放入执行的 context.Context，然后将同一个 context 传给 tRPC-Agent-Go 的 runner.Runner、Tool 和 Storage 调用。
4. Worker 在执行期间只消费快照。配置更新会产生新的 tenant version，只影响后续执行，避免一次执行混用两套配置。

ConfigurationSnapshotFromContext 会返回副本，因此调用方不能修改其他组件看见的租户状态。

## Runner 身份

使用 tenant.NewRunnerIdentity(tenantID, externalUserID, externalSessionID) 构造 Runner 所需的 userID 和 sessionID。编码是长度前缀格式，避免 tenant + external 的简单拼接发生碰撞。IM Adapter 决定 externalSessionID：单聊通常由外部用户标识组成，群聊必须包含外部群标识。无论 Runner 使用什么字符串，平台的持久化查询仍必须将独立的 tenant_id 作为条件，不能把 key 前缀视为唯一隔离手段。

## 可直接复用与当前限制

| 需求 | 直接复用 tRPC-Agent-Go | 本阶段平台代码 |
| --- | --- | --- |
| 执行取消和链路传递 | context.Context 与 runner.Runner | 将可信配置快照附加到 context |
| 用户/会话命名空间 | Runner 的 userID / sessionID | 生成无歧义的租户前缀身份 |
| 开发期会话/记忆 | InMemory Session / Memory 能力 | InMemoryRepository 保存 tenant 控制面数据 |
| Agent、Tool、Channel | Runner、Agent 编排、Tool/MCP、OpenClaw Channel | 未实现 |

InMemoryRepository 只适用于单进程开发和测试：数据不持久化，也不会跨 Worker 同步。Redis、SQL、向量库、对象存储、数据迁移和跨节点一致性仍由后续 issue 单独定义和实现。
