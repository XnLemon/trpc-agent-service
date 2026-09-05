# app、agent 与 runtime 包边界

本页是 `trpcservice/app`、`trpcservice/agent` 和
`trpcservice/runtime` 的实现契约。它把当前已经落地的职责和后续重构
必须遵守的依赖方向写清楚，避免“包名正确但职责继续漂移”。

本页不新增 `control-plane` domain，也不要求把控制面代码搬到新的顶层
包。控制面是系统语义；应用与版本领域仍由 `app` 负责，组合装配和测试
辅助代码可以继续留在内部实现区域。

## 总体方向

```text
外部协议 / HTTP
        |
        v
Gateway -------> runtime: Plan、执行协调、租约和调度
                   |
                   v
                 agent: tRPC-Agent-Go Agent / Runner 组装
                   |
                   v
             tRPC-Agent-Go

bootstrap 负责把 app、agent、runtime、storage 和 Gateway 的具体实现组合起来。
```

这里的箭头表示调用和依赖方向，不表示所有实现必须位于不同进程。单进程
部署仍然必须保留相同的生命周期和所有权边界。

## 包职责

| 包 | 拥有的概念 | 不应拥有的概念 |
| --- | --- | --- |
| `trpcservice/app` | Agent App、不可变 Revision、发布/回滚生命周期、领域校验和 Repository 契约 | Runner、Agent 组装、执行调度、队列、Outbox、运行时存储实现 |
| `trpcservice/agent` | tRPC-Agent-Go 的 Agent/Runner/Session 适配、Agent execution snapshot、Runner 构造和租户能力绑定 | ExecutionPlan 解析、租约/队列调度、回复投递、数据迁移、App/Revision 生命周期变更 |
| `trpcservice/runtime` | ExecutionPlan、配置快照组合、PlanResolver、Runner Registry、执行协调和内部调度 | App/Revision 领域生命周期、具体 `llmagent`/Runner 组装、协议适配和渠道回复 |
| `trpcservice/runtime/storage` | 租户范围内的 Session、Event、Memory、Artifact 等能力契约及其后端适配 | 选择执行租户、解析 Plan、驱动 Runner、回复发送策略 |
| `trpcservice/runtime/outbox` | 回复物化、发送、重试和死信边界 | Agent 编排、控制面配置和执行调度 |
| `trpcservice/runtime/migration` | 后端迁移、双写、校验和切换工具 | 在线执行、Runner 生命周期和请求路由 |
| `trpcservice/gateway` | 可信身份建立、协议中立的请求/事件转换、入口幂等和调用 runtime | Agent/Runner 的具体构造、控制面领域变更 |
| `trpcservice/bootstrap` | 具体 Repository、Provider、Registry、Dispatcher 和 Worker 的组合装配 | 业务领域规则和新的跨层抽象 |

`app` 是最稳定的领域底座；`agent` 是平台与上游 Agent 框架之间的适配
边界；`runtime` 使用这些能力完成一次服务内部执行，但不反向拥有上游
Agent 的实现。

## 允许的依赖

当前实现和目标演进遵守以下规则：

1. `app` 不依赖 `agent`、`runtime`、Gateway 或运行时存储。App/Revision
   的领域不变量不能由执行路径反向定义。
2. `agent` 可以消费 `app` 的不可变快照，以及 Backend、Model、Tool 和
   Tenant 的能力；上游 `trpc-agent-go` 的 Agent、Runner、Session 类型和
   组装逻辑由 `agent` 持有。
3. `runtime` 可以消费 `app` 和 `agent` 提供的快照/工厂输入契约，并持有
   Plan、调度和执行协调；具体 Agent/Runner 的构造必须通过 `agent` 的
   边界完成。
4. `runtime/execution` 可以在窄的 Runner 事件转换边界使用上游事件类型，
   但不得在这里组装 `llmagent`、模型 Provider 或 Session 实现。
5. `runtime/storage`、`runtime/outbox` 和 `runtime/migration` 是 runtime
   的子边界。它们可以提供能力给调用方，但不能把调度、认证或控制面
   生命周期带回存储实现。
6. `bootstrap` 是具体实现的组合根。新的跨包依赖优先在组合根注入，
   不通过全局变量、隐式 Context 值或跨层反向调用建立。
7. 新增接口应放在实际消费者所属的包；只有同一契约确实被多个独立
   消费者共享时，才考虑建立中立的能力包。

## 当前过渡性边界

`trpcservice/agent/sessionstore` 当前依赖
`trpcservice/runtime/storage`，用于把上游 Session 行为接到租户范围的
持久化能力。这是一个隔离的适配器依赖，不代表根 `agent` 包拥有 runtime
存储；后续应评估把它改成由组合根注入的 Session 持久化能力，或把明确的
桥接契约放在消费者侧。

同样，`runtime` 当前需要读取 `agent` 的 execution snapshot 和 factory
input，以便为完整 Plan 建立 Runner 缓存键。这是 runtime 消费 agent 契约，
不是 runtime 重新实现 Agent。只有当这条依赖阻碍真实的下一步扩展时，才
引入更窄的中立 capability contract，不为预想中的扩展提前抽象。

## 一次执行的所有权

```text
Gateway
  - 验证外部身份并建立可信请求
  - 调用 runtime 解析固定 ExecutionPlan
  - 把已接受的请求交给 runtime execution

runtime
  - 按固定 Plan 获取 Runner lease
  - 驱动一次执行并负责取消、drain、lease 和执行事件流

agent
  - 根据固定输入构造或复用上游 Agent / Runner
  - 绑定租户范围的 Model、Tool、Session 和 Storage 能力

runtime/storage 与 runtime/outbox
  - 分别负责持久化能力和回复交付
  - 不拥有 Gateway 的协议输出或 Runner 的生命周期
```

异步执行中，创建资源的一方必须明确关闭责任：runtime execution 关闭
自己的执行事件流和 Runner lease，Gateway 关闭对外的 Dispatch 事件流，
Outbox 负责回复投递资源。Context 始终由调用链显式传递，不存储在长期
对象中。

## 后续重构规则

- 先维持本页契约，再拆 `runtime/storage`、`runtime/outbox` 和
  `runtime/migration` 的实现边界。
- 移动代码时优先移动所有权和测试，不为了包名创建重复类型或兼容层。
- 任何跨租户存储能力都必须携带显式 `tenant_id`；字符串前缀不能替代
  授权和数据隔离。
- 任何影响导出 API、持久化格式、事件顺序、取消或关闭语义的变化，都
  必须单独说明兼容性，不作为目录整理的附带结果。
- 每个后续 PR 应包含与边界相关的契约测试，并验证
  `go test ./...`、Race（如果涉及并发）和 `git diff --check`。

本页描述的是包的责任，不是未来必须一次完成的目录迁移清单。具体移动
应继续拆成小的、可独立验证和回滚的 PR，并在 tracker #130 中链接对应
实现。
