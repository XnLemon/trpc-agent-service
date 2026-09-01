# Issue #111: Tool-driven Media Reply

Issue #98 provides the channel, attachment store, Gateway and Outbox transport
for native media. It intentionally does not make ordinary model text create a
media reply. Issue #111 adds the narrow execution bridge: an explicitly
allowlisted agent tool can select a server-owned attachment and queue a
structured reply.

## Current MVP

The first installed tool is send_test_image. A published revision must
explicitly include it in ToolAuthorization; unlisted tools are not exposed to
the model, optional unavailable tools are omitted, and required unavailable
tools fail Runner construction.

When the model invokes the tool, the service:

1. verifies the tool admission policy and records tool.allowed;
2. writes a fixed, valid PNG through the tenant-scoped attachment store and
   binds it to the already durable inbound event;
3. records only a protocol-neutral image reply intent and tool.executed;
4. materializes that intent as an immutable ReplyOutbox image segment with
   the trusted reply target, fallback, request ID and trace ID.

The tool result visible to the model contains only status: queued and a safe
message. It does not expose attachment IDs, object keys, raw bytes, provider
media IDs, Telegram/WeCom URLs, access tokens, or channel secrets.

## Delivery And Failure Semantics

The existing #98 providers send a structured image segment natively when the
destination supports it. Otherwise the persisted deterministic fallback is
sent. Outbox replay uses its stable reply identity, and duplicate channel
callbacks are rejected before another Runner turn can create another media
reply.

If the Runner reports a tool/model error, Gateway stores the existing
deterministic execution fallback with the original request/trace correlation.
Storage/provider error details and raw tool arguments remain outside the
channel reply and audit payloads.

## Deliberate Boundaries

send_test_image is a transport smoke-test tool, not an image-generation
service. Image generation, arbitrary file selection, video understanding, OCR,
ASR, and specialized external agent services remain separate capabilities.
New tools should implement the same context-bound tool.Factory contract:
they may create a validated attachment reference and ReplyIntent, but must not
widen the Runner-facing contract with provider-specific credentials or
download URLs.
