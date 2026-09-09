-- Add immutable Agent App identity to the side-effect ledger. Existing rows
-- cannot be assigned an app safely because migration 0019 intentionally did not
-- retain it; fail closed instead of guessing a namespace during upgrade.
SET LOCAL search_path = pg_catalog, public, pg_temp;

ALTER TABLE public.runtime_tool_invocation
    ADD COLUMN app_id TEXT;

DO $migration$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.runtime_tool_invocation
        WHERE app_id IS NULL
    ) THEN
        RAISE EXCEPTION 'runtime_tool_invocation contains rows without app identity';
    END IF;
END
$migration$;

ALTER TABLE public.runtime_tool_invocation
    ALTER COLUMN app_id SET NOT NULL,
    ADD CONSTRAINT runtime_tool_invocation_app_id_ck
        CHECK (app_id ~ '^app_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    ADD CONSTRAINT runtime_tool_invocation_app_fk
        FOREIGN KEY (tenant_id, app_id)
        REFERENCES public.agent_app (tenant_id, app_id)
        ON DELETE CASCADE;

ALTER TABLE public.runtime_tool_invocation
    DROP CONSTRAINT runtime_tool_invocation_pkey,
    ADD CONSTRAINT runtime_tool_invocation_pkey PRIMARY KEY (tenant_id, app_id, invocation_id),
    DROP CONSTRAINT runtime_tool_invocation_tenant_id_event_id_tool_call_id_key,
    ADD CONSTRAINT runtime_tool_invocation_event_call_key UNIQUE (tenant_id, app_id, event_id, tool_call_id);

DROP INDEX IF EXISTS public.runtime_tool_invocation_reconcile_idx;
DROP INDEX IF EXISTS public.runtime_tool_invocation_request_idx;
CREATE INDEX runtime_tool_invocation_reconcile_idx
    ON public.runtime_tool_invocation (tenant_id, app_id, status, updated_at);
CREATE INDEX runtime_tool_invocation_request_idx
    ON public.runtime_tool_invocation (tenant_id, app_id, request_id, created_at);

REVOKE ALL ON TABLE public.runtime_tool_invocation FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON public.runtime_tool_invocation TO tenant_app_writer;
GRANT REFERENCES ON public.agent_app TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_tool_invocation TO migration_owner;
