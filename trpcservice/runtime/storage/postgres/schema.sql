-- Package-owned base table schema for trpcservice/runtime/storage/postgres.
-- Keep cross-package foreign keys, functions, triggers, grants, and later
-- evolution steps in the migration orchestrator.

CREATE TABLE IF NOT EXISTS public.runtime_session (
    tenant_id   TEXT NOT NULL,
    session_id  TEXT NOT NULL CHECK (length(btrim(session_id)) BETWEEN 1 AND 256),
    status      TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'closed')),
    version     BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    state       JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(state) = 'object'),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE IF NOT EXISTS public.message_event (
    tenant_id            TEXT NOT NULL,
    event_id             TEXT NOT NULL CHECK (length(btrim(event_id)) BETWEEN 1 AND 256),
    session_id           TEXT NOT NULL,
    binding_id           TEXT NOT NULL,
    external_message_id  TEXT NOT NULL CHECK (length(btrim(external_message_id)) BETWEEN 1 AND 512),
    idempotency_key      TEXT NOT NULL DEFAULT '' CHECK (length(idempotency_key) <= 512),
    event_seq            BIGINT NOT NULL CHECK (event_seq >= 1),
    status               TEXT NOT NULL DEFAULT 'received'
                         CHECK (status IN ('received', 'running', 'completed', 'execution_reconciling', 'reply_pending', 'replied', 'failed')),
    fencing_token        BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    lease_owner          TEXT NOT NULL DEFAULT '' CHECK (length(lease_owner) <= 256),
    lease_expires_at     TIMESTAMPTZ,
    reply_id             TEXT NOT NULL DEFAULT '',
    segment_count        INT NOT NULL DEFAULT 0 CHECK (segment_count >= 0),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, event_id),
    UNIQUE (tenant_id, session_id, event_seq),
    UNIQUE (tenant_id, binding_id, external_message_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES public.runtime_session(tenant_id, session_id),
    FOREIGN KEY (tenant_id, binding_id) REFERENCES public.channel_binding(tenant_id, binding_id)
);

CREATE TABLE IF NOT EXISTS public.reply_outbox (
    tenant_id           TEXT NOT NULL,
    reply_id            TEXT NOT NULL CHECK (length(btrim(reply_id)) BETWEEN 1 AND 256),
    event_id            TEXT NOT NULL,
    segment_index       INT NOT NULL CHECK (segment_index >= 0),
    segment_count       INT NOT NULL CHECK (segment_count > segment_index),
    payload             TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'sending', 'sent', 'retryable', 'dead_letter')),
    attempts            INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    fencing_token       BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    lease_owner         TEXT NOT NULL DEFAULT '' CHECK (length(lease_owner) <= 256),
    lease_expires_at    TIMESTAMPTZ,
    provider_message_id TEXT NOT NULL DEFAULT '' CHECK (length(provider_message_id) <= 512),
    last_error_class    TEXT NOT NULL DEFAULT '' CHECK (length(last_error_class) <= 128),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, reply_id, segment_index),
    UNIQUE (tenant_id, event_id, reply_id, segment_index),
    FOREIGN KEY (tenant_id, event_id) REFERENCES public.message_event(tenant_id, event_id)
);

CREATE TABLE IF NOT EXISTS public.runtime_event_history (
    tenant_id   TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    event_id    TEXT NOT NULL CHECK (length(btrim(event_id)) BETWEEN 1 AND 256),
    payload     JSONB NOT NULL,
    history_seq BIGINT GENERATED ALWAYS AS IDENTITY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id, event_id),
    UNIQUE (tenant_id, session_id, history_seq),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES public.runtime_session(tenant_id, session_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.runtime_reply_correlation (
    tenant_id  TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    request_id TEXT NOT NULL CHECK (length(btrim(request_id)) BETWEEN 1 AND 256),
    trace_id   TEXT NOT NULL DEFAULT '' CHECK (length(trace_id) <= 256),
    PRIMARY KEY (tenant_id, event_id),
    FOREIGN KEY (tenant_id, event_id) REFERENCES public.message_event(tenant_id, event_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.runtime_memory (
    tenant_id  TEXT NOT NULL,
    memory_id  TEXT NOT NULL CHECK (length(btrim(memory_id)) BETWEEN 1 AND 256),
    user_id    TEXT NOT NULL CHECK (length(btrim(user_id)) BETWEEN 1 AND 256),
    session_id TEXT NOT NULL DEFAULT '' CHECK (length(session_id) <= 256),
    content    TEXT NOT NULL CHECK (length(btrim(content)) > 0),
    topics     JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(topics) = 'array'),
    metadata   JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    embedding  JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(embedding) = 'array'),
    version    BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, memory_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE IF NOT EXISTS public.runtime_summary (
    tenant_id  TEXT NOT NULL,
    session_id TEXT NOT NULL,
    filter_key TEXT NOT NULL DEFAULT '' CHECK (length(filter_key) <= 256),
    text       TEXT NOT NULL CHECK (length(btrim(text)) > 0),
    event_seq  BIGINT NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    version    BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id, filter_key),
    FOREIGN KEY (tenant_id, session_id) REFERENCES public.runtime_session(tenant_id, session_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.runtime_knowledge (
    tenant_id   TEXT NOT NULL,
    document_id TEXT NOT NULL CHECK (length(btrim(document_id)) BETWEEN 1 AND 256),
    content     TEXT NOT NULL CHECK (length(btrim(content)) > 0),
    metadata    JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    embedding   JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(embedding) = 'array'),
    digest      TEXT NOT NULL CHECK (length(digest) <= 128),
    version     BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, document_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE IF NOT EXISTS public.runtime_artifact (
    tenant_id  TEXT NOT NULL,
    artifact_id TEXT NOT NULL CHECK (length(btrim(artifact_id)) BETWEEN 1 AND 256),
    session_id TEXT NOT NULL DEFAULT '' CHECK (length(session_id) <= 256),
    name       TEXT NOT NULL DEFAULT '' CHECK (length(name) <= 512),
    mime_type  TEXT NOT NULL DEFAULT '' CHECK (length(mime_type) <= 256),
    content    BYTEA NOT NULL,
    version    BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, artifact_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE IF NOT EXISTS public.runtime_audit_log (
    tenant_id  TEXT NOT NULL,
    audit_id   TEXT NOT NULL CHECK (length(btrim(audit_id)) BETWEEN 1 AND 256),
    event_type TEXT NOT NULL CHECK (length(btrim(event_type)) BETWEEN 1 AND 128),
    payload    JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload) = 'object'),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, audit_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE IF NOT EXISTS public.runtime_vector_index (
    tenant_id   TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'generic' CHECK (length(btrim(source)) BETWEEN 1 AND 128),
    document_id TEXT NOT NULL CHECK (length(btrim(document_id)) BETWEEN 1 AND 256),
    content     TEXT NOT NULL DEFAULT '',
    metadata    JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    embedding   JSONB NOT NULL CHECK (jsonb_typeof(embedding) = 'array'),
    version     BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, source, document_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE IF NOT EXISTS public.runtime_object (
    tenant_id    TEXT NOT NULL,
    object_key   TEXT NOT NULL CHECK (length(btrim(object_key)) BETWEEN 1 AND 1024),
    content_type TEXT NOT NULL DEFAULT '' CHECK (length(content_type) <= 256),
    content      BYTEA NOT NULL,
    size         BIGINT NOT NULL CHECK (size >= 0),
    etag         TEXT NOT NULL CHECK (length(etag) <= 128),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, object_key),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE IF NOT EXISTS public.runtime_attachment (
    tenant_id   TEXT NOT NULL,
    attachment_id TEXT NOT NULL CHECK (length(btrim(attachment_id)) BETWEEN 1 AND 256),
    kind        TEXT NOT NULL CHECK (kind IN ('image', 'video', 'audio', 'document')),
    mime_type   TEXT NOT NULL CHECK (length(btrim(mime_type)) BETWEEN 1 AND 256),
    name        TEXT NOT NULL DEFAULT '' CHECK (length(name) <= 512),
    size        BIGINT NOT NULL CHECK (size > 0 AND size <= 67108864),
    sha256      TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    provider    TEXT NOT NULL DEFAULT '' CHECK (length(provider) <= 64),
    provider_id TEXT NOT NULL DEFAULT '' CHECK (length(provider_id) <= 512),
    event_id    TEXT,
    expires_at  TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, attachment_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id),
    FOREIGN KEY (tenant_id, attachment_id)
        REFERENCES public.runtime_object(tenant_id, object_key)
        ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, event_id)
        REFERENCES public.message_event(tenant_id, event_id)
        ON DELETE SET NULL (event_id)
);

CREATE INDEX IF NOT EXISTS message_event_pending_idx
    ON public.message_event (tenant_id, status, updated_at)
    WHERE status IN ('received', 'running', 'execution_reconciling', 'reply_pending');

CREATE INDEX IF NOT EXISTS reply_outbox_delivery_idx
    ON public.reply_outbox (tenant_id, status, updated_at)
    WHERE status IN ('pending', 'retryable', 'sending');

CREATE INDEX IF NOT EXISTS runtime_memory_user_idx ON public.runtime_memory (tenant_id, user_id, updated_at DESC) WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS runtime_artifact_session_idx ON public.runtime_artifact (tenant_id, session_id, artifact_id);

CREATE INDEX IF NOT EXISTS runtime_audit_time_idx ON public.runtime_audit_log (tenant_id, occurred_at, audit_id);
