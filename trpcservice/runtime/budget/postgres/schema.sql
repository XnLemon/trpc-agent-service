-- Package-owned base table schema for trpcservice/runtime/budget/postgres.
-- Keep migration history and privilege changes in the migration orchestrator.

CREATE TABLE IF NOT EXISTS public.runtime_budget_ledger (
    tenant_id       TEXT NOT NULL,
    period_start    DATE NOT NULL,
    used_tokens     BIGINT NOT NULL DEFAULT 0 CHECK (used_tokens >= 0),
    reserved_tokens BIGINT NOT NULL DEFAULT 0 CHECK (reserved_tokens >= 0),
    used_minor      BIGINT NOT NULL DEFAULT 0 CHECK (used_minor >= 0),
    reserved_minor  BIGINT NOT NULL DEFAULT 0 CHECK (reserved_minor >= 0),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, period_start),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE TABLE IF NOT EXISTS public.runtime_budget_reservation (
    tenant_id           TEXT NOT NULL,
    reservation_id      TEXT NOT NULL,
    period_start        DATE NOT NULL,
    token_limit         BIGINT CHECK (token_limit IS NULL OR token_limit >= 0),
    spend_limit_minor   BIGINT CHECK (spend_limit_minor IS NULL OR spend_limit_minor >= 0),
    currency            TEXT NOT NULL DEFAULT '',
    estimated_tokens    BIGINT NOT NULL CHECK (estimated_tokens >= 0),
    estimated_minor     BIGINT NOT NULL CHECK (estimated_minor >= 0),
    actual_tokens       BIGINT CHECK (actual_tokens IS NULL OR actual_tokens >= 0),
    actual_minor        BIGINT CHECK (actual_minor IS NULL OR actual_minor >= 0),
    state               TEXT NOT NULL CHECK (state IN ('reserved', 'settled', 'released')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, reservation_id),
    FOREIGN KEY (tenant_id, period_start)
        REFERENCES public.runtime_budget_ledger(tenant_id, period_start)
);

CREATE INDEX IF NOT EXISTS runtime_budget_reservation_state_idx
    ON public.runtime_budget_reservation (tenant_id, period_start, state, updated_at);
