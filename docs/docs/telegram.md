# Telegram 长轮询 Channel Adapter

> Issue #31 的实现契约与状态记录。Telegram long polling 普通文本路径已实现并由单元、race
> 与全仓验证覆盖；Webhook、媒体/rich update、持久化和跨节点能力仍明确不在本 Issue 范围内。

## 1. 交付边界

Telegram 适配器是一个绑定级别的协议入口，不创建第二套租户或 Runner 路由。一个适配器实例
只代表一个 active Telegram Binding，运行链路固定为：

```text
Telegram getUpdates
  -> Update.Message 校验和规范化
  -> 已验证的 channels.RoutingTarget
  -> gateway.Channel Principal
  -> gateway.DispatchService
  -> 完整消费脱敏 DispatchEvent
  -> 聚合并分段
  -> Telegram sendMessage
```

本 Issue 只实现 long polling 和普通文本消息。Webhook、媒体、命令、回调、持久化 outbox、跨
节点 polling ownership 与分布式幂等保留给后续 Issue；`WebhookPath` 仍是绑定配置中的预留字段，
不能被本适配器读取为运行模式。

## 2. 适配器边界与构造

实现包位于 `trpcservice/channels/telegram`。公开构造配置的语义如下：

| 配置 | 约束 |
| --- | --- |
| `BotToken` | 仅为运行时输入，不能写入 Binding、Plan、缓存键、日志、trace 或错误；构造成功后只由 SDK client 持有 |
| `Target` | 必须由现有 trusted boundary 创建并通过 `Validate()`；必须是 active Telegram Binding 的 RoutingTarget |
| `Dispatcher` | 使用现有 `gateway.DispatchService`；适配器不能直接调用 Runner |
| `Idempotency` | 可注入现有 `gateway.IdempotencyStore`；未注入时由适配器拥有一个进程内实例，不宣称跨进程保证 |
| `APIBaseURL` | 可选 HTTPS origin，仅用于 SDK Bot API；不从 Telegram update 或 webhook 字段读取 |
| `HTTPClient` | 可选的 SDK HTTP client，测试使用 fake/`httptest`，不要求真实凭据 |
| `PollTimeout` | 可选 long-poll timeout；采用 SDK 默认值时不自行覆盖 |
| `Workers` | 零值为 1；大于 1 必须由调用方显式配置，并由 SDK 同步处理 handler 生命周期 |
| `ErrorHook` | 只接收稳定的适配器错误类别，不接收 SDK/provider 原始错误、token 或 endpoint 凭据 |
| `Factory` | 注入式 Bot factory；生产实现才依赖 `github.com/go-telegram/bot`，测试不创建网络 client |

构造函数接收 Context，先创建带默认 update handler 的 client，再调用 `getMe`，把返回的 Bot user
ID 规范化为十进制字符串并与 `Target.ProviderAccountID` 精确比较。创建失败、`getMe` 失败或身份
不一致都 fail closed；在身份通过前不得处理任何 update。

适配器内部只保存由 `gateway.NewChannelPrincipal(Target)` 产生的 principal。Telegram update
不包含并且不能覆盖 tenant、binding、app、model、profile 或 routing hint；显示名、username 和
标题只可作为未来展示元数据，不能参与认证、session 或 Runner identity。

Bot factory 对 SDK 使用以下固定策略：

- `WithSkipGetMe`，由适配器在自己的 Context 中执行并校验 `getMe`；
- `WithDefaultHandler` 指向适配器的单 update handler；
- `WithNotAsyncHandlers`，使 `Run(ctx)` 结束时不会遗留 SDK 自己创建的异步 handler goroutine；
- `WithWorkers` 采用已验证的 worker 数量；
- 可选地设置 server URL、HTTP client 和 polling timeout；
- SDK polling error 只转换成稳定的 `polling` hook 事件，不能直接透传或记录原始错误。

## 3. 入站规范化与幂等

第一版只接受 `Update.Message` 中的普通文本：

| Telegram 字段 | Gateway 字段 | 规则 |
| --- | --- | --- |
| `Message.Text` | `InboundMessage.Content` | 必须非空；继续交给 `InboundMessage.Normalize()` 做 trim/长度校验 |
| `Message.From.ID` | `ExternalUserID` | 必须存在且非零；不能使用 username/姓名 |
| private `Message.Chat.ID` | `ConversationDirect` + `ExternalPeerID` | chat ID 是稳定会话身份 |
| group/supergroup `Message.Chat.ID` | `ConversationGroup` + `ExternalChatID` | chat ID 进入群会话身份 |
| `Message.MessageThreadID` | `ExternalThreadID` | 大于零时保留；发送回复时原样作为 forum thread |
| `Update.ID` + trusted `BindingID` | `ExternalMessageID` / `RequestID` | 使用长度前缀编码生成稳定、无碰撞的 binding-aware ID |

编辑消息、channel post、callback/inline、service update、无 sender/chat/text、未知 chat 类型和
媒体-only update 都以稳定的非敏感原因忽略或拒绝，且不得进入 Dispatch。所有合法消息先用固定
principal 调用 `IdempotencyStore.Begin`：

- pending duplicate 不再次调用 Dispatch，也不启动隐藏 retry；
- completed duplicate 重用已缓存的脱敏 DispatchEvent，并只重新发送一个聚合后的逻辑回复；
- dispatch 或发送前的处理失败释放 claim，允许调用方按既有进程内策略重新处理；
- 该 store 只保证当前进程，不能暗示跨节点、重启恢复或持久化语义。

## 4. Dispatch 与回复

适配器把规范化消息和可信 principal 交给 `DispatchService.Dispatch`，`RequestID` 使用上节生成
的稳定值，Context 原样向下传递。它必须读完事件 channel 直到关闭，不能在第一个 `message` 或
`done` 后提前退出。只拼接 `DispatchEventMessage.Text`；收到脱敏 `error`、stream 异常或空的
dispatch stream 时，发送固定的适配器级失败文本，不暴露 provider error、stack trace、Secret
或 repository 细节。

正常文本回复按 Unicode code point 切分，每段最多 4096 个 code point，并逐段调用：

```text
sendMessage(chat_id=Message.Chat.ID,
            message_thread_id=Message.MessageThreadID when > 0,
            text=chunk)
```

不为每个 partial event 发送 Telegram 消息，不在本 Issue 引入编辑消息、队列、退避或后台重试。
`sendMessage` 失败只通过稳定的 `send` hook 暴露；若已发送部分分段，不回滚也不启动隐式重试。

## 5. 生命周期与错误脱敏

```mermaid
sequenceDiagram
    participant C as Service Context
    participant A as Telegram Adapter
    participant B as Bot SDK
    participant G as Gateway Dispatch
    participant T as Telegram API

    C->>A: New(ctx, runtime token, trusted Target)
    A->>B: New + getMe
    B-->>A: bot user ID
    A->>A: compare provider_account_id; mismatch fails closed
    C->>A: Run(ctx)
    A->>B: Start(ctx)
    B->>T: getUpdates (long polling)
    T-->>B: Update.Message
    B->>A: HandleUpdate(ctx, update)
    A->>G: trusted Principal + normalized InboundMessage
    G-->>A: complete redacted DispatchEvent stream
    A->>T: one or more sendMessage chunks
    C-->>B: cancel ctx
    B-->>A: stop polling and synchronous handlers
    A-->>C: Run returns
```

`Run(ctx)` 是阻塞入口；Context 取消必须同时结束 SDK polling、在途 Dispatch 和 sendMessage。适配器
不创建自己的 retry goroutine，不保存 request Context，不持有 Runner lease；lease 和 event drain
由现有 Gateway contract 管理。`Close` 只关闭适配器拥有的进程内幂等 store，不能关闭调用方注入
的 store 或 HTTP client。

错误 hook 只使用 `initialization`、`polling`、`update`、`dispatch`、`send` 等稳定 operation 和
适配器 sentinel error。原始 SDK error 只能用于本地判断，不能出现在返回值、hook payload、日志、
trace 或 Telegram 回复中。

## 6. 文档与代码验收清单

README 和 MkDocs 状态应明确区分已交付与后续能力：

- [x] SDK 版本固定，Bot factory/client 可注入，`Run(ctx)` 和单 update handler 可测试；
- [x] `getMe` 身份校验、tenant/Binding/Runner 隔离、普通文本映射和 binding-aware 幂等通过测试；
- [x] Dispatch 完整消费、单逻辑回复、4096 code point 分段、forum thread 路由和失败脱敏通过测试；
- [x] cancellation、polling error、send failure、duplicate delivery 和资源生命周期通过测试；
- [x] Telegram long polling 已实现；Webhook、持久化幂等/outbox、媒体、跨节点 ownership
      和其他 rich update 明确保持未勾选。

参考：[Telegram Bot API](https://core.telegram.org/bots/api)、
[getUpdates](https://core.telegram.org/bots/api#getting-updates)、
[github.com/go-telegram/bot](https://github.com/go-telegram/bot)。
