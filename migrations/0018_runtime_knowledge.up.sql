-- Durable upstream Knowledge VectorStore records.
-- Embeddings remain JSONB so managed PostgreSQL deployments do not need the
-- pgvector extension merely to apply the control-plane migration. The adapter
-- can be replaced by a pgvector-backed implementation behind the same
-- vectorstore.VectorStore contract when an operator provisions that extension.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.runtime_knowledge_document (
    tenant_id       TEXT NOT NULL REFERENCES public.tenant(tenant_id) ON DELETE CASCADE,
    document_id     TEXT NOT NULL CHECK (length(btrim(document_id)) BETWEEN 1 AND 512),
    name            TEXT NOT NULL DEFAULT '' CHECK (length(name) <= 2048),
    content         TEXT NOT NULL,
    embedding_text  TEXT NOT NULL DEFAULT '',
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    embedding       JSONB NOT NULL CHECK (jsonb_typeof(embedding) = 'array'),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, document_id)
);

CREATE INDEX runtime_knowledge_document_metadata_idx
    ON public.runtime_knowledge_document USING GIN (metadata);
CREATE INDEX runtime_knowledge_document_updated_idx
    ON public.runtime_knowledge_document (tenant_id, updated_at, document_id);

REVOKE ALL ON TABLE public.runtime_knowledge_document FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_knowledge_document TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_knowledge_document TO migration_owner;
