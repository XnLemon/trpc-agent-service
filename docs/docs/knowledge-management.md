# Knowledge 管理与发布

平台只提供已经鉴权的 tenant/app Knowledge 管理面；Agent 运行时仍使用
`tRPC-Agent-Go` 的 `knowledge.Knowledge`/`vectorstore.VectorStore` 合同。
管理请求必须显式带 `{TenantID, AppID}`，Admin API 会先校验租户主体和控制面中已存在的
App，不能通过 URL 或文档 metadata 创建新的命名空间。

## API

在 `/admin/v1/tenants/{tenant_id}/knowledge` 下，App 可通过 `?app_id=...` 指定，或使用
`/apps/{app_id}` 路径：

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/documents` | 按 ID 游标分页列出文档 |
| `POST` | `/documents` | 创建文档；ID 缺省时由服务端生成 |
| `GET/PATCH/DELETE` | `/documents/{id}` | 查询、更新或删除一份文档 |
| `POST` | `/import` | 批量导入/按 ID upsert 文档 |
| `POST` | `/rebuild` | 使用服务端 Embedder 重建 embedding |
| `POST` | `/publish` | 生成并持久化 corpus manifest |
| `GET` | `/versions` | 列出该 App 的发布版本 |

请求只能提交文本、`embedding_text` 和用户 metadata；embedding 由服务端生成，响应永远
不包含向量。`_trpc_app_id`、`_trpc_published` 和 `_trpc_knowledge_version` 是平台保留
metadata key；调用方不能将 `_trpc_app_id` 改成另一个 App。

## 持久化与隔离

PostgreSQL provider 以 tenant/app/document 复合键保存文档，并在每次运行时 Knowledge search 强制
`_trpc_app_id` filter，同时检查返回文档的 metadata。`0023_runtime_knowledge_app_scope.up.sql`
新增 `(tenant_id, app_id, version)` manifest 表和 digest 唯一约束；同一 corpus digest 的
重复发布返回原版本。版本分配在 tenant/App advisory transaction lock 下完成，SQL 连接池由
Bootstrap 借用，VersionStore 不持有或关闭连接池。

本地 demo 可以使用按 tenant/App 隔离的 upstream in-memory VectorStore；生产 PostgreSQL
必须注入 durable VersionStore 和 embedding SecretRef。缺少 provider、Embedder、scope、App
或 embedding 生成失败时，管理请求 fail closed。向量库、embedding provider 和来源凭据不
属于 audit/API 响应，原始 SQL/Secret/provider 错误也不会返回给客户端。

## 运行时发布语义

Manifest 是文档内容、metadata 和 ID 的排序 digest 快照；它不复制向量，也不把向量库伪装
成控制面版本事实源。运行时检索仍受已发布 Agent Revision 的 app scope 和 allowlist 保护。
Provider 重建或跨进程重启后，文档和 manifest 通过 PostgreSQL 恢复；外部 embedding provider
不可用时，创建、更新和 rebuild 不会声称成功。
