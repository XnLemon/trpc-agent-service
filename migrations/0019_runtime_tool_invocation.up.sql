-- Durable tool side-effect lifecycle. Raw arguments and provider results are
-- never persisted; ArgsSHA256 only detects conflicting retries.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.runtime_tool_invocation (
    tenant_id      TEXT NOT NULL REFERENCES public.tenant(tenant_id) ON DELETE CASCADE,
    invocation_id  TEXT NOT NULL CHECK (length(btrim(invocation_id)) BETWEEN 1 AND 256),
    event_id       TEXT NOT NULL CHECK (length(btrim(event_id)) BETWEEN 1 AND 256),
    request_id     TEXT NOT NULL CHECK (length(btrim(request_id)) BETWEEN 1 AND 256),
    trace_id       TEXT NOT NULL DEFAULT '' CHECK (length(trace_id) <= 256),
    tool_call_id   TEXT NOT NULL CHECK (length(btrim(tool_call_id)) BETWEEN 1 AND 256),
    tool_name      TEXT NOT NULL CHECK (length(btrim(tool_name)) BETWEEN 1 AND 256),
    args_sha256    TEXT NOT NULL CHECK (args_sha256 ~ '^[0-9a-f]{64}$'),
    status         TEXT NOT NULL CHECK (status IN ('prepared','dispatching','accepted','succeeded','failed','denied','unknown','manual')),
    owner          TEXT NOT NULL DEFAULT '' CHECK (length(owner) <= 256),
    fencing_token  BIGINT NOT NULL DEFAULT 1 CHECK (fencing_token >= 1),
    error_class    TEXT NOT NULL DEFAULT '' CHECK (length(error_class) <= 128),
    reviewer_id    TEXT NOT NULL DEFAULT '' CHECK (length(reviewer_id) <= 256),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, invocation_id),
    UNIQUE (tenant_id, event_id, tool_call_id)
);

CREATE INDEX runtime_tool_invocation_reconcile_idx
    ON public.runtime_tool_invocation (tenant_id, status, updated_at);
CREATE INDEX runtime_tool_invocation_request_idx
    ON public.runtime_tool_invocation (tenant_id, request_id, created_at);

REVOKE ALL ON TABLE public.runtime_tool_invocation FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON public.runtime_tool_invocation TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_tool_invocation TO migration_owner;
