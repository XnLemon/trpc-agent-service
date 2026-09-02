# WeCom AI Bot 长连接

`wecom_aibot` 是独立于自建应用 `wecom` 回调的 WebSocket 通道。服务端主动连接 `wss://openws.work.weixin.qq.com`，使用 Binding 的 `BotID` 与 SecretRef 中的 Bot Secret 发送 `aibot_subscribe` 认证帧。

入站仅支持文本单聊和群聊。回调帧的 `headers.req_id` 被透传到 Dispatcher 的 `RequestID`，`msgid` 作为 durable 幂等键；同一消息在重连或重复帧下不会启动第二次执行。回复由单 writer 按 `req_id` 排队，流式中间片段不进入 Outbox，最终文本写入现有 durable Reply Outbox 并在连接恢复后重试。

配置至少包含：

```json
{
  "channel": "wecom_aibot",
  "provider_account_id": "bot-account",
  "secret_ref": "env/wecom-aibot",
  "protocol": {"wecom_aibot": {"bot_id": "bot_xxx"}}
}
```

运行时必须允许出站 `wss://`，并将连接 Manager 的生命周期交给 Bootstrap Runtime。`BeginShutdown` 会停止新执行、取消连接和心跳；`Close` 等待 read/write pumps 退出。Secret、原始帧和消息正文不会进入日志、trace、audit 或错误响应。

官方协议参考：[文档 60904](https://developer.work.weixin.qq.com/document/60904)；SDK：[aibot-node-sdk](https://github.com/WecomTeam/aibot-node-sdk)。
