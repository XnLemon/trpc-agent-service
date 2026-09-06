-- Issue #98: tenant-scoped attachment metadata and event ownership.
SET LOCAL search_path = pg_catalog, public, pg_temp;

REVOKE ALL ON TABLE public.runtime_attachment FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_attachment TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_attachment TO migration_owner;
