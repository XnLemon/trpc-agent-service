# Admin Web UI 开发准备：功能与接口梳理

> 基于当前仓库代码与测试整理，基线提交：`e9a2ffff2608245555b0341262f1959f77485163`（2026-09-01）。本文描述的是已实现行为，不把 Issue 文档中的早期设计当作现状。

## 结论

当前服务已经具备独立的 Admin 控制面 API，可管理 Tenant、Agent App、Agent Revision、Model Profile、Backend Profile 与 Channel Binding 六类配置资源。它适合用于受控的单资源配置和发布操作，但**尚不具备完整后台所需的列表、资源发现、审计查询、用量查询、浏览器身份认证等读取能力**。

因此，推荐按两个阶段推进：

1. 先补齐 Admin API 的查询与身份边界，再做完整的多租户 Web UI。
2. 在接口补齐之前，前端只能做“已知 tenant/app/profile ID 的深链接编辑器”，不能做正常的资源列表、租户切换和运营看板。

## 控制面定位

Admin API 由 Gateway 同一 HTTP 服务暴露，但认证与聊天 API 完全独立：

```mermaid
flowchart LR
  U[管理员浏览器] --> BFF[Web BFF / 反向代理]
  BFF -->|受控 Bearer 凭证| A[/admin/v1/*]
  A --> CP[控制面 Repository]
  CP --> DB[(PostgreSQL / MySQL)]
  A --> AU[审计 Writer]
  A --> CI[Runner 缓存失效]
  G[/v1/chat] --> R[Gateway Runtime]
```

- 路由前缀：`/admin/v1/*`。
- 服务启动时由 `TRPC_ADMIN_TOKEN` 与 `TRPC_ADMIN_TENANTS` 创建静态 Admin principal；它不能用聊天 API token 替代。
- `TRPC_ADMIN_TENANTS=*` 只允许创建**首个** Tenant，不是已有 Tenant 的通配读写权限。创建后需配置明确的 Tenant ID scope。
- 每次成功的控制面写入会触发进程内 Runner 缓存失效；Model、Backend、App、Tenant 与 Binding 的修改会影响后续请求使用的运行时配置。
- `GET /healthz` 和 `GET /readyz` 由 Gateway 提供；前者只表示进程存活，后者表示运行时依赖可接流。

## 已有功能

| 资源 | 已有管理能力 | 在运行时中的作用 | Web UI 页面 |
| --- | --- | --- | --- |
| Tenant | 创建、读取、完整配置替换、状态迁移 | 隔离根节点、限流/并发/预算/审计/默认 App 与 Backend | 租户概览、设置、状态操作 |
| Agent App | 创建、读取、元数据修改、状态迁移 | 应用身份与当前发布版本指针 | 应用列表、详情、发布控制 |
| Agent Revision | 创建草稿、更新草稿、发布、回滚、设置/清除 Canary | 可执行 Agent 配置；已发布版本不可修改 | 版本编辑器、发布与回滚历史 |
| Model Profile | 创建、读取、完整配置替换、状态迁移 | Provider、模型、endpoint、secret reference、生成参数 | 模型配置页 |
| Backend Profile | 创建、读取、完整配置替换、状态迁移 | Session/Memory 等运行时能力的提供者绑定 | 存储后端配置页 |
| Channel Binding | 创建、读取、完整配置替换、状态迁移 | 企业微信或 Telegram 入站流量到 Agent App 的路由 | 渠道绑定页 |
| 审计与用量 | 领域层支持记录、查询和聚合 | 记录控制面变更、执行结果、成本和用量 | 尚未向 Admin HTTP 暴露 |

所有资源均为租户作用域。当前 API 不支持删除；`disabled` 是终态，不能恢复。

## 现有 HTTP 接口

所有成功响应统一为：

```json
{"request_id":"<id>","data":{}}
```

所有失败响应统一为：

```json
{"request_id":"<id>","error":"<stable-category>"}
```

写请求可带 `X-Request-ID`；服务端会原样放入响应，否则生成 UUID。请求字段接受项目定义的 snake_case。资源对象的返回字段当前多数来自 Go 默认 JSON 编码，因而是 `TenantID`、`DisplayName` 这类 PascalCase，只有内部配置中声明过 JSON tag 的字段（例如 `secret_ref`）保持 snake_case。前端实现前应先在 BFF 或新增 API 版本中统一响应命名。

### Tenant

| 方法 | 路径 | 用途 | 成功响应 |
| --- | --- | --- | --- |
| POST | `/admin/v1/tenants` | 创建 Tenant；仅 global principal 且只能创建首个 Tenant | `201 Tenant` |
| GET | `/admin/v1/tenants/{tenant_id}` | 查询一个获授权 Tenant | `200 Tenant` |
| PATCH | `/admin/v1/tenants/{tenant_id}` | 以 `expected_version` 完整替换可变配置 | `200 Tenant` |
| POST | `/admin/v1/tenants/{tenant_id}/status` | 迁移 Tenant 状态 | `200 {tenant,event}` |

Tenant 可编辑字段包括展示名、RPM、最大并发、月 token/金额预算、账单币种、审计保留天数、日志脱敏级别、trace 采样率、默认 App 与默认 Backend。Tenant key 与 ID 不可修改。

### Agent App 与 Revision

| 方法 | 路径 | 用途 | 成功响应 |
| --- | --- | --- | --- |
| POST | `/admin/v1/tenants/{tenant_id}/apps` | 创建 Draft 状态的 App | `201 App` |
| GET | `/admin/v1/tenants/{tenant_id}/apps/{app_id}` | 查询 App | `200 App` |
| PATCH | `/admin/v1/tenants/{tenant_id}/apps/{app_id}` | 完整替换展示名与描述 | `200 App` |
| POST | `/admin/v1/tenants/{tenant_id}/apps/{app_id}/status` | 迁移 App 状态 | `200 {app,event}` |
| POST | `/admin/v1/tenants/{tenant_id}/apps/{app_id}/revisions` | 创建下一个 Revision 的草稿 | `201 Revision` |
| PATCH | `/admin/v1/tenants/{tenant_id}/apps/{app_id}/revisions/{revision}` | 完整替换草稿配置 | `200 Revision` |
| POST | `/admin/v1/tenants/{tenant_id}/apps/{app_id}/revisions/{revision}/publish` | 原子发布草稿并更新 current revision | `200 {app,revision,event}` |
| POST | `/admin/v1/tenants/{tenant_id}/apps/{app_id}/rollback` | 将 current revision 指回一个已发布版本 | `200 {app,event}` |
| POST | `/admin/v1/tenants/{tenant_id}/apps/{app_id}/canary` | 设置或清除已发布的 candidate revision | `200 {app,event}` |

Revision 配置包含 `description`、`instruction`、`global_instruction`、`model_profile_id`、`generation`、`runtime` 与 `tools`。当前仅支持 `kind=llm`、`schema_version=1`。已发布 Revision 不能修改，只能创建新 Draft、发布或回滚。

> 注意：当前请求键标准化遗漏了 `global_instruction`；使用 snake_case 提交时会被静默忽略。实施 Web UI 前应先修复服务端映射并增加线协议测试，或暂时以 `GlobalInstruction` 发送该字段。该问题也说明不应让前端直接依赖当前不一致的 JSON 命名。

### Model Profile

| 方法 | 路径 | 用途 | 成功响应 |
| --- | --- | --- | --- |
| POST | `/admin/v1/tenants/{tenant_id}/models` | 创建模型配置 | `201 {profile,event}` |
| GET | `/admin/v1/tenants/{tenant_id}/models/{profile_id}` | 查询模型配置 | `200 Profile` |
| PATCH | `/admin/v1/tenants/{tenant_id}/models/{profile_id}` | 完整替换模型配置 | `200 {profile,event}` |
| POST | `/admin/v1/tenants/{tenant_id}/models/{profile_id}/status` | 暂停、恢复或禁用 | `200 {profile,event}` |

配置结构为 `provider`、`model`、可选 `endpoint`、受 Catalog 限制的 `options`、`secret_ref` 与 `generation`。`secret_ref` 是不透明引用；页面只能编辑或展示引用，不能接收、显示或回显明文密钥。

### Backend Profile

| 方法 | 路径 | 用途 | 成功响应 |
| --- | --- | --- | --- |
| POST | `/admin/v1/tenants/{tenant_id}/backends` | 创建存储后端配置 | `201 {profile,event}` |
| GET | `/admin/v1/tenants/{tenant_id}/backends/{profile_id}` | 查询存储后端配置 | `200 Profile` |
| PATCH | `/admin/v1/tenants/{tenant_id}/backends/{profile_id}` | 完整替换能力绑定 | `200 {profile,event}` |
| POST | `/admin/v1/tenants/{tenant_id}/backends/{profile_id}/status` | 暂停、恢复或禁用 | `200 {profile,event}` |

每个 `bindings[]` 项包含 `capability`、`provider`、`endpoint`、`options`、`secret_ref`。可用能力为 `session`、`memory`、`summary`、`knowledge`、`artifact`、`audit`。Active Profile 至少要有一个 `session` binding。

### Channel Binding

| 方法 | 路径 | 用途 | 成功响应 |
| --- | --- | --- | --- |
| POST | `/admin/v1/tenants/{tenant_id}/bindings` | 创建渠道绑定 | `201 {binding,event}` |
| GET | `/admin/v1/tenants/{tenant_id}/bindings/{binding_id}` | 查询渠道绑定 | `200 Binding` |
| PATCH | `/admin/v1/tenants/{tenant_id}/bindings/{binding_id}` | 完整替换渠道可变配置 | `200 {binding,event}` |
| POST | `/admin/v1/tenants/{tenant_id}/bindings/{binding_id}/status` | 激活、暂停、恢复或禁用 | `200 {binding,event}` |

支持 `wecom` 与 `telegram`。Binding 保存 `provider_account_id`、`public_route_key_digest`、`app_id`、`secret_ref` 和非秘密 `protocol` 配置；不保存路由原文和渠道凭据。企业微信可配置 `corp_id`、`agent_id`、`receive_id`；Telegram 可配置 HTTPS `api_base_url` 与以 `/` 开头的 `webhook_path`。

## 写入约束与 UI 交互规则

### 乐观锁与完整替换

所有 `PATCH` 与状态/发布/回滚/Canary 操作都要求页面带上最新版本号：

| 操作 | 必填版本字段 |
| --- | --- |
| Tenant / App / Profile / Binding 配置与状态 | `expected_version` |
| 创建 Revision | `expected_app_version` |
| 更新 Draft | `expected_app_version`、`expected_draft_version` |
| 发布 Draft | `expected_app_version`、`expected_draft_version` |
| 回滚与 Canary | `expected_app_version` |

虽然 HTTP 方法为 `PATCH`，实现语义是**完整替换**而不是局部更新。编辑页必须先读取最新对象，以完整表单提交所有可变字段，不能只提交一个变更字段。

收到 `409 conflict` 后，前端应保留用户草稿、重新读取服务端对象、提示版本已变更，并让用户确认覆盖内容后再以新的版本号提交。不能自动重试覆盖。

### 状态机

| 资源 | 初始状态 | 合法迁移 |
| --- | --- | --- |
| Tenant | `active` 或 `suspended` | `active -> suspended/disabled`；`suspended -> active/disabled` |
| Agent App | `draft` | 首次发布使 `draft -> active`；之后 `active <-> suspended`，均可到 `disabled`；Draft 只能到 `disabled` |
| Model / Backend Profile | `active` 或 `suspended` | `active <-> suspended`，均可到 `disabled` |
| Channel Binding | `draft` | `draft -> active/disabled`；`active <-> suspended`，均可到 `disabled` |

页面应根据当前状态只展示合法操作。`disabled` 不展示“恢复”按钮；高影响操作（发布、回滚、禁用、激活渠道、设置 Canary）需要二次确认并要求填写 reason。

### 审计字段

状态迁移、Model/Backend/Binding 的创建和配置修改、Revision 发布、回滚与 Canary 都需要 `reason`、`correlation_id`。服务端将 actor 固定为 Admin principal，调用方不能伪造。

前端应在一次用户操作开始时生成一个 UUID 作为 `correlation_id`，并在确认弹窗中强制填写 `reason`。同一次提交发生网络不确定性时应保留这两个值，便于人工核对审计记录。

### 审计失败的特殊处理

`audit_unavailable`（503）并不保证写入未发生：当前 Handler 先提交 Repository 变更，再写审计；若审计写失败，会向客户端返回 503，但控制面数据可能已经更新，缓存失效也尚未触发。此时 UI 应：

1. 不自动重试写请求。
2. 立即重新读取目标资源并比较版本、状态或内容摘要。
3. 将结果标记为“提交状态待确认”，提示管理员按 request/correlation ID 排查审计链路。

## 建议的信息架构

| 路由/页面 | 核心内容 | 依赖的服务端能力 |
| --- | --- | --- |
| `/login` | 企业身份登录与会话建立 | 新增浏览器身份认证或 BFF |
| `/tenants` | 可访问租户列表、状态、默认配置 | 缺少 Tenant list / current principal |
| `/tenants/:tenantId/overview` | 配置健康度、默认 App/Backend、readyz 状态 | 缺少聚合查询；可先做详情卡片 |
| `/tenants/:tenantId/apps` | App 列表、状态、当前/Canary 版本 | 缺少 App list |
| `/tenants/:tenantId/apps/:appId` | 元数据、版本列表、草稿编辑、发布/回滚/Canary | 缺少 Revision list 与 detail read |
| `/tenants/:tenantId/models` | Model Profile 列表与配置表单 | 缺少 Model list 与 Catalog |
| `/tenants/:tenantId/backends` | Backend Profile 列表、能力绑定编辑器 | 缺少 Backend list 与 Catalog |
| `/tenants/:tenantId/bindings` | 渠道列表、路由、协议和状态管理 | 缺少 Binding list；缺少路由 digest 辅助 |
| `/tenants/:tenantId/audit` | 控制面变更、执行失败、用量成本 | 领域层已有能力，但无 HTTP API |

首个可交付版本建议只包含“租户设置、App/Revision、Model、Backend、Binding”五类配置页面和基础状态操作；审计与用量页面在其读取 API 完成后加入。不要先做假数据 Dashboard，因为当前不存在能支撑实时指标的 Admin HTTP 聚合接口。

## 必须补齐的服务端接口

### P0：完整后台上线前

| 能力 | 建议接口 | 原因 |
| --- | --- | --- |
| 当前用户与权限 | `GET /admin/v1/me` | 返回 subject、role、显式 tenant scopes、是否可创建首租户；用于菜单和路由守卫 |
| Tenant 列表 | `GET /admin/v1/tenants?cursor=&limit=` | 目前只能按已知 ID 获取 Tenant，无法做租户选择 |
| 资源列表 | `GET .../apps`、`models`、`backends`、`bindings` | 列表页、下拉引用选择、空状态和搜索的基本前提 |
| Revision 读取 | `GET .../revisions` 与 `GET .../revisions/{revision}` | 现在无法展示、恢复或比较历史版本；HTTP 层也没有 Revision GET |
| Catalog 读取 | `GET /admin/v1/catalog/models`、`GET /admin/v1/catalog/backends` | 表单需要受信 provider、model、capability、option、endpoint/secret policy；不能在前端硬编码 |
| 线协议统一 | 新增 `/admin/v2` 或 BFF 适配层 | 请求 snake_case、响应 PascalCase 且有字段映射缺口，不适合作为长期 Web 合约 |
| 浏览器认证 | OIDC/SSO 或 BFF session | 现有静态 token 不能下发给浏览器，也没有用户级 subject、CORS、CSRF 防护 |

列表接口应采用 cursor pagination、稳定排序和显式 filter，最低返回 `id`、`key`、`display_name`、`status`、`version`、`updated_at` 以及页面所需的关联 ID。必须由服务端按 principal scope 过滤，不能将 Tenant scope 交给浏览器判断。

### P1：运营与可维护性

| 能力 | 建议接口/设计 |
| --- | --- |
| 审计日志 | `GET /admin/v1/tenants/{tenant_id}/audit`，支持 event type、时间范围、cursor；严禁返回 secret 或路由原文 |
| 用量与成本 | `GET .../usage?from=&to=&group_by=app,model,channel`，复用现有 `audit.Aggregator` |
| 配置预检 | 表单提交前/后提供 `validate` 或 dry-run；至少返回稳定字段级错误码 |
| 渠道路由摘要 | 服务端计算 `public_route_key_digest`，浏览器只在本次提交中传入原始 route key，不持久化、不回显 |
| 健康摘要 | 受控的 `/admin/v1/system/health`，聚合 readiness 与依赖类别，不泄露 DSN、endpoint 或 secret reference |
| 变更可追溯 | 按 `correlation_id` 查询操作结果与审计状态，解决网络中断和 `audit_unavailable` 场景 |

## 前端技术与安全边界

- 不要把 `TRPC_ADMIN_TOKEN` 打包到前端、浏览器 localStorage 或客户端配置。它目前是唯一静态 Admin 凭证，泄露等同于该 scope 的控制面权限泄露。
- 推荐部署形态是同域 Web BFF：浏览器使用 HttpOnly、Secure、SameSite Cookie；BFF 验证用户身份、执行 CSRF 校验、向 Admin API 注入服务端保管的凭证或映射后的 principal。
- 当前 `StaticAuthenticator` 的 subject 固定为 `admin`，因此审计无法区分具体网页操作者。上线多人后台前，应让 Admin principal 从可信身份提供方携带真实 subject，并保留租户 scope。
- API 错误只提供稳定类别：`invalid_request`、`unauthorized`、`forbidden`、`not_found`、`conflict`、`storage_unavailable`、`audit_unavailable`、`internal_error`。表单层需根据类别给出通用提示，不能依赖后端错误文本。
- 未授权的 GET 返回 `404 not_found`，写入返回 `403 forbidden`。页面在深链接失败时统一显示无权限/资源不存在，避免暴露跨租户对象是否存在。
- API 中仅允许 `secret_ref`，明文密钥、DSN、Token、route key 原文均不应进入表单回显、日志、埋点、错误 toast 或前端状态持久化。

## 实施顺序与验收

1. **先收敛服务端合约**：完成 P0 查询接口、snake_case 响应 DTO、`global_instruction` 映射修复，并为每个接口覆盖权限、分页、跨租户隐藏、版本冲突和秘密字段脱敏测试。
2. **建立 BFF 身份边界**：接入登录、会话、CSRF、真实 subject 与 scope；确保浏览器端永远拿不到 Admin token。
3. **实现资源管理页面**：先实现列表/详情/新建/完整编辑，统一封装 `request_id`、`correlation_id`、版本号和 `409` 冲突处理。
4. **实现发布工作流**：Draft 编辑、版本比较、发布确认、回滚、Canary 设置/清除；所有不可逆或影响流量的操作必须二次确认和填写 reason。
5. **接入审计与用量**：在 P1 API 可用后实现可筛选审计、用量趋势和成本汇总，不在前端拼接或估算来源数据。

完成标准：管理员可在不知晓内部 ID 的前提下，在授权 Tenant 内完成资源浏览、创建、编辑、状态迁移、Draft 发布、回滚与 Canary；所有写入能处理冲突与不确定结果；不会向浏览器暴露 Admin token、明文凭据或跨租户资源。

## 代码证据索引

| 主题 | 位置 |
| --- | --- |
| Admin 路由、响应封装、错误映射、缓存失效 | `trpcservice/admin/api.go` |
| Admin 静态认证与租户 scope 行为 | `trpcservice/admin/auth.go` |
| Gateway 对 `/admin/v1/*` 的挂载与健康检查 | `trpcservice/gateway/http.go` |
| Bootstrap 的 Admin Handler 装配 | `trpcservice/bootstrap/bootstrap.go`、`trpcservice/bootstrap/environment.go` |
| Tenant、App/Revision、Model、Backend、Binding 领域约束 | `trpcservice/tenant/`、`trpcservice/agent/`、`trpcservice/model/`、`trpcservice/backend/`、`trpcservice/channels/` |
| 审计与用量领域查询能力 | `trpcservice/audit/audit.go`、`trpcservice/audit/postgres/repository.go` |
| Admin API 路由与写入行为测试 | `trpcservice/admin/api_test.go`、`trpcservice/bootstrap/restart_test.go` |
