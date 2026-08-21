# 数据模型

## 核心表结构

```text
tenant            租户(tenant_id, 配置, 审计策略, 预算)
agent_app         Agent 应用(租户级, 模型与工具配置)
channel_binding   IM 通道绑定(tenant + channel + 账号 → agent_app)
session           会话(tenant_id, session_id, 状态, TTL)
message / event   消息与会话事件(session 内有序)
memory            长期记忆(租户 + 用户维度, 可检索)
summary           会话摘要(滚动压缩)
audit_log         审计日志(tenant, channel, user, tool, cost, trace_id)
```

## 多后端适配

| 后端 | 适合存储 | 一致性 |
| --- | --- | --- |
| InMemory | 单测、本地开发 | 无持久化 |
| Redis | 热点 Session、短期 Memory | 最终一致(主从异步) |
| MySQL / PostgreSQL | 持久 Session、审计、配置 | 强一致 |
| 向量库(Qdrant / Milvus) | Knowledge、可检索 Memory | 最终一致 |
| 对象存储(S3) | Artifact、大文件 | 强一致(对象级) |

## 数据同步与幂等

- **并发写入同一 session**:乐观锁(版本号)或后端 CAS;冲突时按事件序号重放
- **跨节点 Memory 可见性**:写后读己之写入;后台任务异步构建检索索引
- **IM 重复投递**:幂等键 `channel + msg_id` 落库去重,重复消息直接返回缓存回复
- **后端迁移**:双写过渡 → 校验对齐 → 切读 → 停写,支持租户级灰度

> 本页为设计骨架,详细内容随实现补充。
