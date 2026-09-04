CREATE TABLE public.agent_runtime_profile (
    tenant_id TEXT NOT NULL REFERENCES public.tenant(tenant_id),
    profile_id TEXT NOT NULL,
    runtime_key TEXT NOT NULL,
    runtime_kind TEXT NOT NULL,
    execution_mode TEXT NOT NULL CHECK (execution_mode IN ('builtin','in_process','remote')),
    implementation_ref TEXT NOT NULL,
    runtime_version TEXT NOT NULL,
    schema_version INT NOT NULL CHECK (schema_version >= 1),
    implementation_digest TEXT NOT NULL,
    config_digest TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    capabilities JSONB NOT NULL DEFAULT '[]'::jsonb,
    governance_mode TEXT NOT NULL CHECK (governance_mode IN ('full','perimeter')),
    secret_ref TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','active','disabled')),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, profile_id), UNIQUE (tenant_id, runtime_key)
);
ALTER TABLE public.agent_app_revision ADD COLUMN IF NOT EXISTS runtime_profile_id TEXT;
ALTER TABLE public.agent_app_revision ADD COLUMN IF NOT EXISTS runtime_kind TEXT;
ALTER TABLE public.agent_app_revision ADD COLUMN IF NOT EXISTS runtime_mode TEXT;
ALTER TABLE public.agent_app_revision ADD COLUMN IF NOT EXISTS runtime_version TEXT;
ALTER TABLE public.agent_app_revision ADD COLUMN IF NOT EXISTS runtime_digest TEXT;
ALTER TABLE public.agent_app_revision ADD COLUMN IF NOT EXISTS runtime_config_digest TEXT;
ALTER TABLE public.agent_app_revision ADD COLUMN IF NOT EXISTS runtime_governance TEXT;
