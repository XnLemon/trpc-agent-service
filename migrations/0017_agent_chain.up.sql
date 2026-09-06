-- Allow the first composite Agent definition while keeping schema version 1.
ALTER TABLE public.agent_app_revision
    DROP CONSTRAINT IF EXISTS agent_app_revision_agent_kind_check;

ALTER TABLE public.agent_app_revision
    ADD CONSTRAINT agent_app_revision_agent_kind_check
    CHECK (agent_kind IN ('llm', 'chain'));

-- The budget ledger is runtime state: application workers may reserve and
-- settle it, while direct control-plane table writes remain unavailable to
-- tenant-admin callers.
REVOKE ALL ON TABLE public.runtime_budget_ledger, public.runtime_budget_reservation FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON public.runtime_budget_ledger, public.runtime_budget_reservation TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_budget_ledger, public.runtime_budget_reservation TO migration_owner;
