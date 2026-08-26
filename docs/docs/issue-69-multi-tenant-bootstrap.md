# Issue #69：Multi-tenant Bootstrap

生产 bootstrap 不再要求所有运行时对象绑定到一个 `RuntimeTenantID`。默认的单租户环境变量仍兼容；需要多个 API 租户时使用：

```text
TRPC_API_IDENTITIES=token-a|t_<tenant-a>|app_<app-a>|service-a,token-b|t_<tenant-b>|app_<app-b>|service-b
```

每个 identity 都会注册独立的 Model provider、Secret scope 和 Backend Session provider。Gateway 仍从控制面按可信 principal 解析完整 ExecutionPlan；Session capability 从该 plan 的 tenant scope materialize，因此多个租户可在同一进程并发执行。

`TRPC_API_IDENTITIES` 与旧的 `TRPC_API_TOKEN`/`TRPC_TENANT_ID`/`TRPC_APP_ID` 互斥：未设置 identity 列表时继续使用旧字段。Model API key 仍只从环境输入边界注入 SecretRegistry，绝不会写入计划、缓存或数据库。

多租户 audit writer 按事件 `tenant_id` 懒加载 tenant-bound PostgreSQL store，并为每次写入设置对应的 RLS scope。WeCom 的真实多账户凭据仍需要后续 channel provider 配置；本阶段不扩大协议能力。

当前 `WECOM_CALLBACK_TOKEN` 等单套 WeCom 环境凭据只能和一个 API identity 一起使用。若同时配置多条 `TRPC_API_IDENTITIES` 与 WeCom 凭据，bootstrap 会 fail closed；它不会任意挑选一个租户来验签或运行 outbox worker。多账号 WeCom credential registry/worker group 是独立的 channel 配置工作。
