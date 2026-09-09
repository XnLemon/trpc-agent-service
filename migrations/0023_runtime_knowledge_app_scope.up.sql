-- Make the Knowledge vector partition durable at tenant/app/document scope.
-- Existing rows are accepted only when the platform can prove their app
-- identity from the reserved metadata key; ambiguous rows fail closed rather
-- than being assigned to an arbitrary App.
SET LOCAL search_path = pg_catalog, public, pg_temp;

ALTER TABLE public.runtime_knowledge_document
    ADD COLUMN app_id TEXT;

UPDATE public.runtime_knowledge_document
SET app_id = metadata ->> '_trpc_app_id'
WHERE app_id IS NULL;

DO $migration$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.runtime_knowledge_document
        WHERE app_id IS NULL
           OR length(btrim(app_id)) = 0
           OR metadata ->> '_trpc_app_id' IS DISTINCT FROM app_id
    ) THEN
        RAISE EXCEPTION 'runtime_knowledge_document contains rows without app identity';
    END IF;
END
$migration$;

ALTER TABLE public.runtime_knowledge_document
    ALTER COLUMN app_id SET NOT NULL,
    ADD CONSTRAINT runtime_knowledge_document_app_id_ck
        CHECK (length(btrim(app_id)) BETWEEN 1 AND 256
            AND metadata ->> '_trpc_app_id' = app_id),
    DROP CONSTRAINT runtime_knowledge_document_pkey,
    ADD CONSTRAINT runtime_knowledge_document_pkey PRIMARY KEY (tenant_id, app_id, document_id),
    ADD CONSTRAINT runtime_knowledge_document_app_fk
        FOREIGN KEY (tenant_id, app_id)
        REFERENCES public.agent_app (tenant_id, app_id)
        ON DELETE CASCADE;

DROP INDEX IF EXISTS public.runtime_knowledge_document_updated_idx;
CREATE INDEX runtime_knowledge_document_updated_idx
    ON public.runtime_knowledge_document (tenant_id, app_id, updated_at, document_id);

REVOKE ALL ON TABLE public.runtime_knowledge_document FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_knowledge_document TO tenant_app_writer;
GRANT REFERENCES ON public.agent_app TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_knowledge_document TO migration_owner;
