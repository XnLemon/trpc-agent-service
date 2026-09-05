-- Issue #114: tenant-scoped PostgreSQL/pgvector knowledge source and index.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE public.runtime_pgvector_documents (
    tenant_id           TEXT NOT NULL,
    collection          TEXT NOT NULL CHECK (length(btrim(collection)) BETWEEN 1 AND 63),
    knowledge_id        TEXT NOT NULL CHECK (length(btrim(knowledge_id)) BETWEEN 1 AND 256),
    document_id         TEXT NOT NULL CHECK (length(btrim(document_id)) BETWEEN 1 AND 256),
    chunk_id            TEXT NOT NULL CHECK (length(btrim(chunk_id)) BETWEEN 1 AND 256),
    source              TEXT NOT NULL DEFAULT '' CHECK (length(source) <= 256),
    source_version      TEXT NOT NULL DEFAULT '' CHECK (length(source_version) <= 256),
    content             TEXT NOT NULL CHECK (length(btrim(content)) > 0),
    content_ref         TEXT NOT NULL DEFAULT '' CHECK (length(content_ref) <= 1024),
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    embedding_model     TEXT NOT NULL CHECK (length(btrim(embedding_model)) BETWEEN 1 AND 256),
    embedding_version   TEXT NOT NULL CHECK (length(btrim(embedding_version)) BETWEEN 1 AND 256),
    embedding_dimension INTEGER NOT NULL CHECK (embedding_dimension BETWEEN 1 AND 2000),
    checksum            TEXT NOT NULL CHECK (length(btrim(checksum)) BETWEEN 1 AND 256),
    index_status        TEXT NOT NULL DEFAULT 'pending' CHECK (index_status IN ('pending','ready','failed','dead_letter','deleted')),
    embedding           vector,
    CHECK (embedding IS NULL OR vector_dims(embedding) = embedding_dimension),
    version             BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    attempts            INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error_class    TEXT NOT NULL DEFAULT '' CHECK (length(last_error_class) <= 128),
    deleted_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, collection, knowledge_id, document_id, chunk_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE INDEX runtime_pgvector_ready_idx
    ON public.runtime_pgvector_documents (tenant_id, index_status, updated_at DESC)
    WHERE deleted_at IS NULL AND index_status = 'ready';

REVOKE ALL ON TABLE public.runtime_pgvector_documents FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_pgvector_documents TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_pgvector_documents TO migration_owner;
