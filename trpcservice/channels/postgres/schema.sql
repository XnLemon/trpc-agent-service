-- Package-owned base table schema for trpcservice/channels/postgres.
-- Keep cross-package foreign keys, functions, triggers, grants, and later
-- evolution steps in the migration orchestrator.

CREATE TABLE IF NOT EXISTS public.channel_binding (
    tenant_id               TEXT NOT NULL REFERENCES public.tenant(tenant_id),
    binding_id              TEXT NOT NULL
                            CHECK (binding_id ~ '^cb_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    binding_key             TEXT NOT NULL
                            CHECK (binding_key ~ '^[a-z][a-z0-9-]{1,63}$'),
    channel                 TEXT NOT NULL CHECK (channel IN ('wecom', 'telegram')),
    provider_account_id     TEXT NOT NULL
                            CHECK (provider_account_id = public.trim_control_plane_text(provider_account_id)
                                   AND pg_catalog.length(provider_account_id) BETWEEN 1 AND 256),
    public_route_key_digest TEXT NOT NULL CHECK (public_route_key_digest ~ '^[0-9a-f]{64}$'),
    app_id                  TEXT NOT NULL
                            CHECK (app_id ~ '^app_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    secret_ref              TEXT NOT NULL
                            CHECK (public.control_plane_secret_ref_is_safe(secret_ref)
                                   AND pg_catalog.length(secret_ref) BETWEEN 1 AND 256),
    protocol_config         JSONB NOT NULL DEFAULT '{}'::jsonb
                            CHECK (pg_catalog.jsonb_typeof(protocol_config) = 'object'
                                   AND public.jsonb_has_safe_keys(protocol_config)),
    schema_version          INT NOT NULL DEFAULT 1 CHECK (schema_version = 1),
    status                  TEXT NOT NULL DEFAULT 'draft'
                            CHECK (status IN ('draft', 'active', 'suspended', 'disabled')),
    version                 BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    config_digest           TEXT NOT NULL CHECK (config_digest ~ '^[0-9a-f]{64}$'),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, binding_id),
    UNIQUE (tenant_id, binding_key),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES public.agent_app(tenant_id, app_id)
);

CREATE TABLE IF NOT EXISTS public.channel_binding_change_outbox (
    event_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type        TEXT NOT NULL CHECK (event_type IN (
                          'created', 'configuration_updated', 'activated',
                          'suspended', 'resumed', 'disabled'
                      )),
    tenant_id         TEXT NOT NULL,
    binding_id        TEXT NOT NULL,
    previous_status   TEXT,
    current_status    TEXT NOT NULL CHECK (current_status IN ('draft', 'active', 'suspended', 'disabled')),
    previous_digest   TEXT,
    current_digest    TEXT NOT NULL CHECK (current_digest ~ '^[0-9a-f]{64}$'),
    actor_type        TEXT NOT NULL,
    actor_id          TEXT NOT NULL,
    reason            TEXT NOT NULL,
    correlation_id    TEXT NOT NULL,
    previous_version  BIGINT NOT NULL CHECK (previous_version >= 0),
    next_version      BIGINT NOT NULL CHECK (next_version = previous_version + 1),
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, binding_id)
        REFERENCES public.channel_binding(tenant_id, binding_id),
    CHECK (actor_type = public.trim_control_plane_text(actor_type)
           AND actor_id = public.trim_control_plane_text(actor_id)
           AND reason = public.trim_control_plane_text(reason)
           AND correlation_id = public.trim_control_plane_text(correlation_id)
           AND pg_catalog.length(actor_type) > 0
           AND pg_catalog.length(actor_id) > 0
           AND pg_catalog.length(reason) BETWEEN 1 AND 1000
           AND pg_catalog.length(correlation_id) > 0)
);

CREATE INDEX IF NOT EXISTS channel_binding_candidate_idx
    ON public.channel_binding (channel, public_route_key_digest)
    WHERE status = 'active';

CREATE UNIQUE INDEX IF NOT EXISTS channel_binding_active_account_idx
    ON public.channel_binding (channel, provider_account_id)
    WHERE status = 'active';
