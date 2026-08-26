# Issue #71：Tenant-scoped Provider Registries

本阶段为运行时多租户装配提供进程内注册表契约。它们是本地开发和确定性测试的适配器；生产环境可以用 KMS、Secret Manager 或服务发现实现相同的接口。

## Secret

`model.SecretRegistry` 使用 `(tenant_id, secret_ref)` 作为唯一 key，注册、替换、删除和解析均要求显式租户。解析失败统一返回脱敏错误，secret 值不会出现在错误、`String` 表示、计划、缓存 key 或持久化对象中。`Close` 会清空值并拒绝后续写入。

## Model Provider

`model.ModelProviderRegistry` 使用 `(tenant_id, provider)` 路由 `ModelFactory`。工厂输入会在调用边界 clone；未知租户或 provider fail closed，调用上下文取消优先于 provider 结果。

## Backend Provider

`backend.ProviderRegistry` 使用 `(tenant_id, capability, provider)` 路由 `CapabilityProvider`。它只持有工厂引用，不持有已物化 capability；后续 #70 负责从冻结的 `StorageFactoryInput` 构造和关闭 Session 等 capability。

## Channel Provider

`channels.ProviderRegistry` 使用 `(tenant_id, channel, provider_account_id)` 路由 `ProviderFactory`，共享层只依赖 `runtime/outbox.Provider`，因此 Telegram、WeCom 等具体 adapter 不会反向污染控制面模型。

## Storage / Session materialization

`backend.StorageFactory` 接收 `ExecutionPlan.StorageFactoryInput()` 的防御性副本，按 capability/provider 从注册表解析实现，并将临时 secret 只传给当前工厂调用。返回的 `CapabilitySet` 由创建它的 Runner 持有；Runner 关闭时释放所有实现了 `Close` 的 capability。Session capability 必须实现 tRPC-Agent-Go 的 `session.Service`，否则 materialization fail closed。

旧版 `runtime.NewRunner` 仍接受借用的 Session service；提供 StorageFactory 时启用新路径，未提供时保持兼容。后续 bootstrap 阶段会把注册表和 StorageFactory 接到多租户生产装配。

所有注册表都是进程内、线程安全、可关闭的实现。它们不提供跨进程一致性、轮换广播或持久化；这些属于后续 bootstrap/cache 工作。
