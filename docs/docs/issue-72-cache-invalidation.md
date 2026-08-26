# Issue #72：Precise Runtime Cache Invalidation

`gateway.RunnerRegistry` 现在提供 `InvalidateMatching` 以及 tenant、app、model profile、backend profile 的精确 helper。配置发布、回滚或 disable 时只应调用受影响 scope；无关租户和 profile 的 future entry 保持复用。

失效会从新 lease 视图删除 entry，但不会中断已经借出的 Runner。最后一个 lease `Release` 后才关闭旧 Runner；正在构造的同 key build 会标记为 invalidated，并在构造完成后关闭并重试。

Provider 实例跟随 Runner/CapabilitySet 生命周期，不进入 cache key。多 binding 的 channel 路由仍必须在验签、租户、App 和 binding 状态校验完成后才允许 Runner execution；未知、inactive、duplicate、cross-tenant binding 在 Gateway 前置失败。
