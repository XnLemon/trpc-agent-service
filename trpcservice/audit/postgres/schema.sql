-- Package-owned base table schema for trpcservice/audit/postgres.
-- Keep cross-package foreign keys, functions, triggers, grants, and later
-- evolution steps in the migration orchestrator.

CREATE TABLE IF NOT EXISTS public.audit_event (
    tenant_id            TEXT NOT NULL CHECK (length(btrim(tenant_id)) BETWEEN 1 AND 256),
    event_id             TEXT NOT NULL CHECK (length(btrim(event_id)) BETWEEN 1 AND 256),
    schema_version       INTEGER NOT NULL CHECK (schema_version = 1),
    event_type           TEXT NOT NULL CHECK (event_type IN (
        'control_plane.changed', 'execution.started', 'execution.completed',
        'execution.failed', 'execution.canceled', 'execution.timed_out',
        'execution.fallback', 'tool.allowed', 'tool.denied',
        'tool.approval_required', 'im.authorization_allowed',
        'im.authorization_denied', 'im.ingress_accepted', 'im.ingress_duplicate',
        'im.delivery_sent', 'im.delivery_retry_scheduled',
        'im.delivery_dead_lettered', 'im.delivery_reconciled', 'budget.rejected',
        'content.redacted', 'audit_incomplete'
    )),
    channel              TEXT,
    user_id              TEXT,
    session_id           TEXT,
    agent_app_id         TEXT,
    revision             BIGINT CHECK (revision IS NULL OR revision >= 0),
    model_profile_id     TEXT,
    tool_name            TEXT,
    decision              TEXT CHECK (decision IS NULL OR decision IN (
        'allow', 'deny', 'approval_required', 'accepted', 'duplicate', 'rejected'
    )),
    latency_ms            BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
    error_type            TEXT CHECK (error_type IS NULL OR error_type IN (
        'canceled', 'timeout', 'invalid', 'unauthenticated', 'rate_limited',
        'duplicate', 'unavailable', 'storage', 'model', 'tool', 'provider_error',
        'budget', 'redacted', 'conflict'
    )),
    input_tokens          BIGINT CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens         BIGINT CHECK (output_tokens IS NULL OR output_tokens >= 0),
    model_cost_minor      BIGINT CHECK (model_cost_minor IS NULL OR model_cost_minor >= 0),
    tool_cost_minor       BIGINT CHECK (tool_cost_minor IS NULL OR tool_cost_minor >= 0),
    currency              CHAR(3),
    budget_used_tokens    BIGINT CHECK (budget_used_tokens IS NULL OR budget_used_tokens >= 0),
    budget_used_minor     BIGINT CHECK (budget_used_minor IS NULL OR budget_used_minor >= 0),
    execution_result      TEXT CHECK (execution_result IS NULL OR execution_result IN (
        'success', 'failure', 'canceled', 'timeout', 'rejected'
    )),
    provider              TEXT,
    model                 TEXT,
    request_id            TEXT,
    trace_id              TEXT,
    correlation_id        TEXT,
    actor_type            TEXT,
    actor_id              TEXT,
    reason                TEXT CHECK (reason IS NULL OR length(reason) <= 4000),
    previous_version     BIGINT,
    next_version          BIGINT,
    occurred_at           TIMESTAMPTZ NOT NULL,
    digest                CHAR(64) NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (tenant_id, event_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id),
    CHECK ((model_cost_minor IS NULL AND tool_cost_minor IS NULL AND budget_used_minor IS NULL)
        OR (currency IS NOT NULL AND currency ~ '^[A-Z]{3}$')),
    CHECK ((previous_version IS NULL) = (next_version IS NULL)
        AND (previous_version IS NULL
             OR (previous_version >= 0 AND previous_version < 9223372036854775807
                 AND next_version = previous_version + 1)))
);

CREATE TABLE IF NOT EXISTS public.execution_audit_handoff (
    tenant_id TEXT NOT NULL REFERENCES public.tenant(tenant_id),
    handoff_id TEXT NOT NULL CHECK (length(btrim(handoff_id)) BETWEEN 1 AND 256),
    request_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','finalized','repairable')),
    result TEXT,
    error_type TEXT,
    latency_ms BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, handoff_id)
);

CREATE INDEX IF NOT EXISTS audit_event_timeline_idx
    ON public.audit_event (tenant_id, occurred_at, event_id);
CREATE INDEX IF NOT EXISTS audit_event_app_idx
    ON public.audit_event (tenant_id, agent_app_id, occurred_at);
CREATE INDEX IF NOT EXISTS audit_event_channel_idx
    ON public.audit_event (tenant_id, channel, occurred_at);
CREATE INDEX IF NOT EXISTS audit_event_model_profile_idx
    ON public.audit_event (tenant_id, model_profile_id, occurred_at);
CREATE INDEX IF NOT EXISTS execution_audit_handoff_state_idx
    ON public.execution_audit_handoff (tenant_id, state, updated_at);
