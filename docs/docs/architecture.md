# 架构设计

## 组件拓扑

```text
IM 平台(企业微信 / 微信客服 / 公众号)
        │ webhook / 回调
        ▼
┌─────────────────────────────────────────────┐
│                 Agent Gateway                │
│   验签 · 去重 · 租户路由 · 限流 · 审计入口    │
└──────────────┬──────────────────────────────┘
               ▼
┌─────────────────────────────────────────────┐
│                Agent Worker                  │
│  runner.Runner · Agent 编排 · Tool / MCP     │
│  Plugin / Guardrail · (无状态,水平扩展)      │
└───┬──────────┬──────────┬──────────┬────────┘
    ▼          ▼          ▼          ▼
 Session     Memory    Knowledge  Artifact
 (Redis/SQL) (多后端)  (向量库)   (对象存储)
```

## 消息链路

以企业微信用户发消息为例:

1. **Channel Adapter** 接收回调,验签、去重(幂等键:`channel + msg_id`)
2. **Gateway** 根据通道绑定定位租户与 Agent 配置,生成 `session_id`(群聊:`tenant + chat_id`;单聊:`tenant + user_id`)
3. **Runner** 执行 Agent,流式消费 Event
4. **Tool 调用**经租户白名单校验,敏感工具触发二次确认
5. **Session / Memory / Audit** 异步写入对应后端,携带 `trace_id`
6. **Channel Adapter** 将回复转换为 IM 消息(分段、卡片、异步重试)

## 租户隔离

| 维度 | 机制 |
| --- | --- |
| 配置 | 租户级 Agent / 模型 / 通道配置,Admin API 管理 |
| 数据 | Session / Memory / Artifact 按后端实例或键前缀隔离 |
| 工具 | 租户工具白名单 + 密钥注入,不落盘不进日志 |
| 日志 | 脱敏中间件过滤 token / key / 手机号 |
| 成本 | 按租户统计 token 消耗与调用次数 |

> 本页为设计骨架,详细内容随实现补充。
