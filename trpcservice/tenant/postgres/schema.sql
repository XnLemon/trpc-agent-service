-- Package-owned base table schema for trpcservice/tenant/postgres.
-- Keep cross-package foreign keys, functions, triggers, grants, and later
-- evolution steps in the migration orchestrator.

CREATE TABLE IF NOT EXISTS public.tenant (
    tenant_id       TEXT PRIMARY KEY
                    CHECK (tenant_id ~ '^t_[0-7][0-9A-HJKMNP-TV-Z]{25}$'),
    tenant_key      TEXT NOT NULL UNIQUE
                    CHECK (tenant_key ~ '^[a-z][a-z0-9-]{1,63}$'),
    display_name    TEXT NOT NULL
                    CHECK (display_name = public.trim_control_plane_text(display_name)
                           AND pg_catalog.length(display_name) BETWEEN 1 AND 200),
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'suspended', 'disabled')),

    rate_limit_rpm             BIGINT,
    max_concurrent_executions  BIGINT,
    monthly_token_budget       BIGINT,
    monthly_spend_limit_minor  BIGINT,
    billing_currency            CHAR(3),
    CHECK (rate_limit_rpm IS NULL OR rate_limit_rpm >= 0),
    CHECK (max_concurrent_executions IS NULL OR max_concurrent_executions > 0),
    CHECK (monthly_token_budget IS NULL OR monthly_token_budget >= 0),
    CHECK (monthly_spend_limit_minor IS NULL OR monthly_spend_limit_minor >= 0),
    CHECK (monthly_spend_limit_minor IS NULL OR billing_currency IS NOT NULL),
    CHECK (billing_currency IS NULL OR billing_currency ~ '^[A-Z]{3}$'),

    audit_retention_days  INT NOT NULL DEFAULT 90 CHECK (audit_retention_days > 0),
    log_masking_level     TEXT NOT NULL DEFAULT 'basic'
                          CHECK (log_masking_level IN ('none', 'basic', 'strict')),
    trace_sampling_rate   REAL NOT NULL DEFAULT 1.0
                          CHECK (trace_sampling_rate BETWEEN 0 AND 1),

    default_agent_app_id       TEXT,
    default_backend_profile_id TEXT,
    version         BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.tenant_status_change_outbox (
    event_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id         TEXT NOT NULL REFERENCES public.tenant(tenant_id),
    previous_status   TEXT NOT NULL CHECK (previous_status IN ('active', 'suspended')),
    next_status       TEXT NOT NULL CHECK (next_status IN ('active', 'suspended', 'disabled')),
    actor_type        TEXT NOT NULL CHECK (pg_catalog.length(public.trim_control_plane_text(actor_type)) > 0),
    actor_id          TEXT NOT NULL CHECK (pg_catalog.length(public.trim_control_plane_text(actor_id)) > 0),
    reason            TEXT NOT NULL CHECK (pg_catalog.length(public.trim_control_plane_text(reason)) BETWEEN 1 AND 1000),
    previous_version  BIGINT NOT NULL,
    next_version      BIGINT NOT NULL CHECK (next_version = previous_version + 1),
    correlation_id    TEXT NOT NULL CHECK (pg_catalog.length(public.trim_control_plane_text(correlation_id)) > 0),
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((previous_status, next_status) IN (
        ('active', 'suspended'), ('active', 'disabled'),
        ('suspended', 'active'), ('suspended', 'disabled')
    ))
);

CREATE TABLE IF NOT EXISTS public.tenant_configuration_outbox (
    event_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id         TEXT NOT NULL REFERENCES public.tenant(tenant_id),
    previous_version  BIGINT NOT NULL,
    next_version      BIGINT NOT NULL CHECK (next_version = previous_version + 1),
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
