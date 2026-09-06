-- Issue #75: tenant-scoped Memory, Summary, Knowledge, Artifact, Audit,
-- vector-index and object-storage metadata.
SET LOCAL search_path = pg_catalog, public, pg_temp;

REVOKE ALL ON TABLE public.runtime_memory, public.runtime_summary, public.runtime_knowledge, public.runtime_artifact, public.runtime_audit_log, public.runtime_vector_index, public.runtime_object FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_memory, public.runtime_summary, public.runtime_knowledge, public.runtime_artifact, public.runtime_audit_log, public.runtime_vector_index, public.runtime_object TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_memory, public.runtime_summary, public.runtime_knowledge, public.runtime_artifact, public.runtime_audit_log, public.runtime_vector_index, public.runtime_object TO migration_owner;
