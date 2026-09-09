-- Platform-owned summary, audit, and attachment content storage.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.runtime_summary (
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

CREATE TABLE public.runtime_audit_log (
    tenant_id   TEXT NOT NULL,
    audit_id    TEXT NOT NULL CHECK (length(btrim(audit_id)) BETWEEN 1 AND 256),
    event_type  TEXT NOT NULL CHECK (length(btrim(event_type)) BETWEEN 1 AND 128),
    payload     JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(payload) = 'object'),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, audit_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);
CREATE INDEX runtime_audit_time_idx ON public.runtime_audit_log (tenant_id, occurred_at, audit_id);

CREATE TABLE public.runtime_attachment_content (
    tenant_id    TEXT NOT NULL,
    attachment_id TEXT NOT NULL CHECK (length(btrim(attachment_id)) BETWEEN 1 AND 256),
    content_type TEXT NOT NULL DEFAULT '' CHECK (length(content_type) <= 256),
    content      BYTEA NOT NULL,
    size         BIGINT NOT NULL CHECK (size >= 0),
    etag         TEXT NOT NULL CHECK (length(etag) <= 128),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, attachment_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

REVOKE ALL ON TABLE public.runtime_summary, public.runtime_audit_log, public.runtime_attachment_content FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_summary, public.runtime_audit_log, public.runtime_attachment_content TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_summary, public.runtime_audit_log, public.runtime_attachment_content TO migration_owner;
