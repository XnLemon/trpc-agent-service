-- Durable tenant/app-scoped Knowledge publication manifests. Vector rows are
-- still owned by the upstream VectorStore; this table records the platform
-- publication fact and content digest without storing embeddings or secrets.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.runtime_knowledge_version (
    tenant_id      TEXT NOT NULL,
    app_id         TEXT NOT NULL,
    version        BIGINT NOT NULL CHECK (version >= 1),
    document_count INTEGER NOT NULL CHECK (document_count >= 0),
    content_digest TEXT NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
    actor_id       TEXT NOT NULL CHECK (length(btrim(actor_id)) BETWEEN 1 AND 256),
    published_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, app_id, version),
    UNIQUE (tenant_id, app_id, content_digest),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES public.agent_app(tenant_id, app_id)
        ON DELETE CASCADE
);

CREATE INDEX runtime_knowledge_version_app_idx
    ON public.runtime_knowledge_version (tenant_id, app_id, version DESC);

REVOKE ALL ON TABLE public.runtime_knowledge_version FROM PUBLIC;
GRANT SELECT, INSERT ON public.runtime_knowledge_version TO tenant_app_writer;
-- PostgreSQL checks FK parents under the inserting role. This grants only the
-- constraint privilege; the runtime role still cannot read or mutate App rows.
GRANT REFERENCES ON public.agent_app TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_knowledge_version TO migration_owner;
