# 运维方案

## 监控指标

- 请求量、模型调用耗时、工具调用耗时
- IM 投递成功率、错误率
- token 消耗、每租户成本
- Session / Memory 后端读写延迟

全部指标带 `tenant_id` 维度,通过 OpenTelemetry 上报;`trace_id` 串起 IM callback → Runner → Tool → Session / Memory → IM 回复全链路。

## 故障恢复

| 故障 | 策略 |
| --- | --- |
| 节点故障 | 无状态 Worker,K8s 自动重建;进行中会话由 IM 重试驱动恢复 |
| IM 重试 | 幂等去重;回复失败进入异步重试队列(指数退避) |
| 数据库短暂不可用 | 降级只读 / 拒绝写入并快速失败;恢复后事件重放 |
| 模型超时 | context 取消 + 重试预算;超限返回兜底回复 |
| 工具执行失败 | Guardrail 拦截异常,返回可解释错误并记录审计 |

Go 侧统一使用 `context.Context` 控制取消,Runner 事件通道排空后退出,避免 goroutine 泄漏。

## 发布与容量

- **灰度发布**:按租户分批切流,配置回滚秒级生效
- **容量评估**:每节点并发 session 数、平均 token 消耗、Redis / SQL QPS、IM 回调峰值
- **最小部署**:Docker Compose(单 Worker + Redis + PostgreSQL)
- **生产部署**:Kubernetes(Gateway / Worker / Channel Adapter 各自独立伸缩)

> 本页为设计骨架,详细内容随实现补充。
