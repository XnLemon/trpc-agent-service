-- Package-owned base table schema for trpcservice/model/postgres.
-- Keep cross-package foreign keys, functions, triggers, grants, and later
-- evolution steps in the migration orchestrator.

CREATE TABLE IF NOT EXISTS public.model_profile (
    tenant_id       TEXT NOT NULL REFERENCES public.tenant(tenant_id),
    profile_id      TEXT NOT NULL
                    CHECK (profile_id ~ '^mp_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    profile_key     TEXT NOT NULL
                    CHECK (profile_key ~ '^[a-z][a-z0-9-]{1,63}$'),
    display_name    TEXT NOT NULL
                    CHECK (display_name = public.trim_control_plane_text(display_name)
                           AND pg_catalog.length(display_name) BETWEEN 1 AND 200),
    description     TEXT NOT NULL DEFAULT ''
                    CHECK (description = public.trim_control_plane_text(description)
                           AND pg_catalog.length(description) <= 2000),
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'suspended', 'disabled')),
    schema_version  INT NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    provider        TEXT NOT NULL CHECK (provider ~ '^[a-z][a-z0-9_-]{0,63}$'),
    model           TEXT NOT NULL CHECK (model ~ '^[a-z][a-z0-9._:-]{0,127}$'),
    endpoint        TEXT NOT NULL DEFAULT '' CHECK (public.control_plane_endpoint_is_safe(endpoint)),
    options         JSONB NOT NULL DEFAULT '{}'::jsonb
                    CHECK (public.jsonb_object_string_values(options)
                           AND public.jsonb_has_safe_keys(options)),
    secret_ref      TEXT NOT NULL DEFAULT ''
                    CHECK (public.control_plane_secret_ref_is_safe(secret_ref)),
    generation      JSONB NOT NULL DEFAULT '{}'::jsonb
                    CHECK (pg_catalog.jsonb_typeof(generation) = 'object'
                           AND public.jsonb_has_safe_keys(generation)),
    content_digest  TEXT NOT NULL CHECK (content_digest ~ '^[0-9a-f]{64}$'),
    version         BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, profile_id),
    UNIQUE (tenant_id, profile_key)
);

CREATE TABLE IF NOT EXISTS public.model_profile_change_outbox (
    event_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type        TEXT NOT NULL CHECK (event_type IN (
                          'created', 'configuration_updated', 'suspended', 'resumed', 'disabled'
                      )),
    tenant_id         TEXT NOT NULL,
    profile_id        TEXT NOT NULL,
    previous_status   TEXT CHECK (previous_status IS NULL OR previous_status IN ('active', 'suspended')),
    current_status    TEXT NOT NULL CHECK (current_status IN ('active', 'suspended', 'disabled')),
    previous_digest   TEXT,
    current_digest    TEXT NOT NULL CHECK (current_digest ~ '^[0-9a-f]{64}$'),
    actor_type        TEXT NOT NULL,
    actor_id          TEXT NOT NULL,
    reason            TEXT NOT NULL,
    correlation_id    TEXT NOT NULL,
    previous_version  BIGINT NOT NULL CHECK (previous_version >= 0),
    next_version      BIGINT NOT NULL CHECK (next_version = previous_version + 1),
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, profile_id)
        REFERENCES public.model_profile(tenant_id, profile_id),
    CHECK ((event_type = 'created'
            AND previous_status IS NULL AND previous_digest IS NULL
            AND current_status IN ('active', 'suspended')
            AND previous_version = 0 AND next_version = 1)
           OR (event_type <> 'created'
               AND previous_status IS NOT NULL
               AND previous_digest ~ '^[0-9a-f]{64}$'
               AND previous_version >= 1)),
    CHECK (actor_type = public.trim_control_plane_text(actor_type)
           AND actor_id = public.trim_control_plane_text(actor_id)
           AND reason = public.trim_control_plane_text(reason)
           AND correlation_id = public.trim_control_plane_text(correlation_id)
           AND pg_catalog.length(actor_type) > 0
           AND pg_catalog.length(actor_id) > 0
           AND pg_catalog.length(reason) BETWEEN 1 AND 1000
           AND pg_catalog.length(correlation_id) > 0)
);
