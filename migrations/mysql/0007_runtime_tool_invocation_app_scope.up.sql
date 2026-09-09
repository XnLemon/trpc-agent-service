-- Bind every durable tool invocation to one immutable Agent App. Existing
-- rows without this identity make a safe online backfill impossible; changing
-- the column to NOT NULL intentionally fails closed in that case.
ALTER TABLE runtime_tool_invocation
    ADD COLUMN app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL AFTER tenant_id;

ALTER TABLE runtime_tool_invocation
    ADD CONSTRAINT runtime_tool_invocation_app_required_ck CHECK (app_id IS NOT NULL);

ALTER TABLE runtime_tool_invocation
    MODIFY COLUMN app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (tenant_id, app_id, invocation_id),
    DROP INDEX runtime_tool_invocation_event_call_idx,
    ADD UNIQUE KEY runtime_tool_invocation_event_call_idx (tenant_id, app_id, event_id, tool_call_id),
    ADD KEY runtime_tool_invocation_app_status_idx (tenant_id, app_id, status, created_at),
    ADD CONSTRAINT runtime_tool_invocation_app_fk
        FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app (tenant_id, app_id);
