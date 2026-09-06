-- Issue #48: append-only upstream Runner event history for durable session recovery.
SET LOCAL search_path = pg_catalog, public, pg_temp;

REVOKE ALL ON TABLE public.runtime_event_history FROM PUBLIC;
GRANT SELECT, INSERT ON public.runtime_event_history TO tenant_app_writer;
-- The idempotent append uses ON CONFLICT DO UPDATE to return an existing row.
-- Limit the runtime role to that no-op key-column update; payload and delete
-- mutations remain unavailable.
GRANT UPDATE (event_id) ON public.runtime_event_history TO tenant_app_writer;
GRANT ALL PRIVILEGES ON TABLE public.runtime_event_history TO migration_owner;
GRANT USAGE, SELECT ON SEQUENCE public.runtime_event_history_history_seq_seq TO tenant_app_writer;
GRANT ALL PRIVILEGES ON SEQUENCE public.runtime_event_history_history_seq_seq TO migration_owner;
