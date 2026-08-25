-- Durable execution audit handoff. It is mutable only through reserve/finalize
-- functions; the projected audit_event remains append-only.
CREATE TABLE public.execution_audit_handoff (
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
ALTER TABLE public.execution_audit_handoff ENABLE ROW LEVEL SECURITY;
CREATE POLICY execution_audit_handoff_scope ON public.execution_audit_handoff
USING (current_setting('app.tenant_id', true) = tenant_id)
WITH CHECK (current_setting('app.tenant_id', true) = tenant_id);
CREATE INDEX execution_audit_handoff_state_idx ON public.execution_audit_handoff (tenant_id, state, updated_at);
