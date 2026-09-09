BEGIN;

-- The original audit migration predates several runtime event producers. Keep
-- the append-only table and its tenant/RLS contract, but align the event type
-- allowlist with audit.Event's immutable contract.
ALTER TABLE public.audit_event
    DROP CONSTRAINT audit_event_event_type_check;

ALTER TABLE public.audit_event
    ADD CONSTRAINT audit_event_event_type_check CHECK (event_type IN (
        'control_plane.changed', 'execution.started', 'execution.completed',
        'execution.failed', 'execution.canceled', 'execution.timed_out',
        'execution.fallback', 'execution.canary_selected', 'tool.allowed',
        'tool.denied', 'tool.approval_required', 'tool.executed',
        'tool.reconciliation_required',
        'im.authorization_allowed', 'im.authorization_denied',
        'im.ingress_accepted', 'im.ingress_duplicate', 'im.delivery_sent',
        'im.delivery_retry_scheduled', 'im.delivery_dead_lettered',
        'im.delivery_reconciled', 'budget.rejected', 'content.redacted',
        'audit_incomplete'
    ));

COMMIT;
