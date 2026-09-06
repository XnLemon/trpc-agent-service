-- Package-owned base table schema for trpcservice/app/postgres.
-- Keep cross-package foreign keys, functions, triggers, grants, and later
-- evolution steps in the migration orchestrator.

CREATE TABLE IF NOT EXISTS public.agent_app (
    tenant_id        TEXT NOT NULL REFERENCES public.tenant(tenant_id),
    app_id           TEXT NOT NULL
                     CHECK (app_id ~ '^app_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    app_key          TEXT NOT NULL
                     CHECK (app_key ~ '^[a-z][a-z0-9-]{1,63}$'),
    display_name     TEXT NOT NULL
                     CHECK (display_name = public.trim_control_plane_text(display_name)
                            AND pg_catalog.length(display_name) BETWEEN 1 AND 200),
    description      TEXT NOT NULL DEFAULT ''
                     CHECK (description = public.trim_control_plane_text(description)
                            AND pg_catalog.length(description) <= 2000),
    status           TEXT NOT NULL DEFAULT 'draft'
                     CHECK (status IN ('draft', 'active', 'suspended', 'disabled')),
    current_revision BIGINT,
    version          BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, app_id),
    UNIQUE (tenant_id, app_key),
    CHECK (
        (status = 'draft' AND current_revision IS NULL)
        OR (status IN ('active', 'suspended') AND current_revision IS NOT NULL)
        OR status = 'disabled'
    )
);

CREATE TABLE IF NOT EXISTS public.agent_app_revision (
    tenant_id          TEXT NOT NULL,
    app_id             TEXT NOT NULL,
    revision           BIGINT NOT NULL CHECK (revision >= 1),
    state              TEXT NOT NULL DEFAULT 'draft'
                       CHECK (state IN ('draft', 'published')),
    draft_version      BIGINT NOT NULL DEFAULT 1 CHECK (draft_version >= 1),
    agent_kind         TEXT NOT NULL CHECK (agent_kind IN ('llm', 'chain')),
    schema_version     INT NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    description        TEXT NOT NULL DEFAULT ''
                       CHECK (description = public.trim_control_plane_text(description)
                              AND pg_catalog.length(description) <= 2000),
    instruction        TEXT NOT NULL
                       CHECK (pg_catalog.length(public.trim_control_plane_text(instruction)) BETWEEN 1 AND 65536),
    global_instruction TEXT NOT NULL DEFAULT ''
                       CHECK (pg_catalog.length(global_instruction) <= 65536),
    model_profile_id   TEXT NOT NULL,
    generation_config  JSONB NOT NULL DEFAULT '{}'::jsonb
                       CHECK (pg_catalog.jsonb_typeof(generation_config) = 'object'
                              AND public.jsonb_has_safe_keys(generation_config)),
    runtime_policy     JSONB NOT NULL DEFAULT '{}'::jsonb
                       CHECK (pg_catalog.jsonb_typeof(runtime_policy) = 'object'
                              AND public.jsonb_has_safe_keys(runtime_policy)),
    content_digest     TEXT,
    published_at       TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, app_id, revision),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES public.agent_app(tenant_id, app_id),
    FOREIGN KEY (tenant_id, model_profile_id)
        REFERENCES public.model_profile(tenant_id, profile_id),
    CHECK (
        (state = 'draft' AND content_digest IS NULL AND published_at IS NULL)
        OR
        (state = 'published'
         AND content_digest ~ '^[0-9a-f]{64}$'
         AND published_at IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS public.agent_app_revision_tool (
    tenant_id TEXT NOT NULL,
    app_id    TEXT NOT NULL,
    revision  BIGINT NOT NULL,
    tool_id   TEXT NOT NULL CHECK (pg_catalog.length(public.trim_control_plane_text(tool_id)) > 0),
    required  BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (tenant_id, app_id, revision, tool_id),
    FOREIGN KEY (tenant_id, app_id, revision)
        REFERENCES public.agent_app_revision(tenant_id, app_id, revision)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.agent_app_change_outbox (
    event_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type        TEXT NOT NULL CHECK (event_type IN (
                          'published', 'rolled_back', 'suspended', 'resumed', 'disabled'
                      )),
    tenant_id         TEXT NOT NULL,
    app_id            TEXT NOT NULL,
    previous_status   TEXT,
    current_status    TEXT NOT NULL CHECK (current_status IN ('draft', 'active', 'suspended', 'disabled')),
    previous_revision BIGINT,
    current_revision  BIGINT,
    content_digest    TEXT CHECK (content_digest IS NULL OR content_digest ~ '^[0-9a-f]{64}$'),
    actor_type        TEXT NOT NULL,
    actor_id          TEXT NOT NULL,
    reason            TEXT NOT NULL,
    correlation_id    TEXT NOT NULL,
    previous_version  BIGINT NOT NULL CHECK (previous_version >= 0),
    next_version      BIGINT NOT NULL CHECK (next_version = previous_version + 1),
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES public.agent_app(tenant_id, app_id),
    CHECK (actor_type = public.trim_control_plane_text(actor_type)
           AND actor_id = public.trim_control_plane_text(actor_id)
           AND reason = public.trim_control_plane_text(reason)
           AND correlation_id = public.trim_control_plane_text(correlation_id)
           AND pg_catalog.length(actor_type) > 0
           AND pg_catalog.length(actor_id) > 0
           AND pg_catalog.length(reason) BETWEEN 1 AND 1000
           AND pg_catalog.length(correlation_id) > 0)
);
