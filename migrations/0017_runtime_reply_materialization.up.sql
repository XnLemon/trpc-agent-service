-- Issue #107: durable recovery for the execution-to-reply materialization gap.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.runtime_reply_materialization (
    tenant_id                 TEXT NOT NULL,
    event_id                  TEXT NOT NULL CHECK (length(btrim(event_id)) BETWEEN 1 AND 256),
    reply_id                  TEXT NOT NULL CHECK (length(btrim(reply_id)) BETWEEN 1 AND 256),
    request_id                TEXT NOT NULL DEFAULT '' CHECK (length(request_id) <= 256),
    trace_id                  TEXT NOT NULL DEFAULT '' CHECK (length(trace_id) <= 256),
    trace_parent              TEXT NOT NULL DEFAULT '' CHECK (length(trace_parent) <= 256),
    payload                   TEXT NOT NULL DEFAULT '',
    segments                  JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(segments) = 'array'),
    reply_binding_id          TEXT NOT NULL DEFAULT '',
    reply_conversation_kind  TEXT NOT NULL DEFAULT '',
    reply_receiver_id         TEXT NOT NULL DEFAULT '',
    reply_thread_id           TEXT NOT NULL DEFAULT '',
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, event_id),
    FOREIGN KEY (tenant_id, event_id)
        REFERENCES public.message_event(tenant_id, event_id)
        ON DELETE CASCADE,
    CHECK ((payload <> '' AND segments = '[]'::jsonb) OR (payload = '' AND jsonb_array_length(segments) > 0))
);

CREATE INDEX runtime_reply_materialization_pending_idx
    ON public.runtime_reply_materialization (tenant_id, updated_at, event_id);

REVOKE ALL ON TABLE public.runtime_reply_materialization FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_reply_materialization TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_reply_materialization TO migration_owner;
