-- Issue #76: durable tenant-scoped execution queue.
SET LOCAL search_path = pg_catalog, public, pg_temp;

REVOKE ALL ON TABLE public.runtime_execution_queue FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON public.runtime_execution_queue TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_execution_queue TO migration_owner;
