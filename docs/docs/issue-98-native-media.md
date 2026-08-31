# Issue #98：原生媒体附件与富回复

Issue #98 在 Issue #77 的通道能力基础上补齐协议中立附件契约。目标不是让通道 payload
绕过 Gateway，而是在验证后的 channel 边界内下载、限流、校验并持久化媒体，然后只把
安全的 `attachment.Reference` 和可加载的 `ContentPart` 交给 Runner。

## MVP 范围

- 入站附件类型固定为 `image`、`video`、`audio`、`document`，引用包含 ID、MIME、
  大小、SHA-256、原始文件名和 provider 文件身份。
- Telegram 入站会保留图片、文档、音频和视频的 provider file id，使用受控
  `MediaDownloader` 下载后立即转存到租户隔离 attachment store。
- WeCom 入站会在签名、AES 解密、`ReceiveID` 和 `AgentID` 全部通过后处理
  `image`、`file`、`voice`、`video`；未知类型和缺少 `MediaId` 的回调 fail closed。
- Gateway 将附件绑定到 durable `message_event` 后，Runner 才能通过 tenant/event/reference
  读取内容；Runner 不接触 Telegram 下载链接、WeCom `media_id` 下载 URL、access token 或
  channel secret。
- 出站 Outbox 支持结构化 `kind + attachment ref + fallback`；Telegram 和 WeCom 对
  `image`、`document` 走原生发送，`audio`、`video` 或不支持能力时使用确定性文本 fallback。

## 入站生命周期

```text
channel callback/update
  -> 验签、解密、绑定候选校验
  -> 识别 provider media/file id
  -> 受控 downloader 下载，限制大小和响应形态
  -> attachment store 写入 bytes + metadata
  -> Gateway 记录 message_event 并绑定 attachment
  -> Runner 按模型能力读取 ContentParts
```

附件 ID 使用 `(tenant, binding, external_message_id, ordinal/provider_id)` 语义生成，保证
重复回调和重试不会产生新的逻辑附件。attachment store 必须返回 defensive copy，并按
tenant/event/reference 验证读取；未绑定到 durable event 的附件不能被 Runner 加载。

## 模型边界

`trpc-agent-go` 已支持 `ContentParts`，本仓库的 Responses 适配器按模型 profile 的显式输入
能力转换：

| 附件 | Runner 内容 | OpenAI Responses 映射 |
| --- | --- | --- |
| text | `ContentPart{Type:text}` | `input_text` |
| image | image content part | `input_image` |
| document/file | file content part | `input_file` |
| mp3/wav audio | audio content part | `input_audio` |
| video | 保留附件和 fallback 文本 | 不声明视频理解 |

视频可以安全收发和存储，但当前不承诺模型视频理解。抽帧、OCR、ASR 和视频分析是后续独立
能力，不属于 #98 MVP。

## 出站语义

`ReplyOutbox` 的媒体段是不可变结构：`Kind` 表示协议中立类型，`Attachment` 指向已验证内容，
`Payload` 可作为 caption，`Fallback` 是目标通道无法表达原生媒体时必须使用的确定性文本。

- Telegram 使用 attachment reader 构造 SDK upload，图片走 `SendPhoto`，文档走
  `SendDocument`；视频和音频先保守发送 fallback。
- WeCom 图片和文档先上传临时素材，再分别发送 `image` 或 `file` 应用消息；未配置 reader 或
  不支持的 kind 发送 fallback。
- Provider 成功 receipt 继续写回 Outbox 状态机；token、provider URL、原始响应和消息正文不进入
  日志或审计。

## 存储与上线

当前 in-memory 和 PostgreSQL runtime store 都实现了 attachment store。PostgreSQL 版本复用
现有 object boundary，但二进制内容仍落在数据库内，适合受限 MVP 和 deterministic E2E，不适合
长期生产视频流量。生产视频或大文件上线前应补 S3/COS 一类流式对象存储实现，并明确：

- 每租户数量、单文件大小、总容量和 MIME allow-list；
- 保留期、引用计数、清理任务和 dead-letter 后的处置；
- downloader 超时、取消、重试和限速；
- 跨 tenant/binding 的读取拒绝和 provider secret 脱敏；
- 部署文档中的对象存储 endpoint、凭据轮换和迁移策略。

## 验证证据

- Telegram 入站、原生图片/文档出站、fallback 和 cancellation 使用 fake SDK/reader 测试。
- WeCom 入站媒体回调使用加密 XML 和 fake downloader 测试，证明不使用 `PicUrl`。
- WeCom 出站图片/文档使用 `httptest` 验证临时素材上传和发送 payload。
- Gateway、in-memory/PostgreSQL runtime store 和 migrations 覆盖 tenant/event/reference 绑定、
  幂等和清理。
