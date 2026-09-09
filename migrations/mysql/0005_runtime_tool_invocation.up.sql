-- Durable tool side-effect ledger for the MySQL runtime adapter.
-- The application account receives only table DML through the existing
-- privilege contract; migration ownership remains with the migration account.
CREATE TABLE IF NOT EXISTS runtime_tool_invocation (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    invocation_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    event_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    request_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    trace_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    tool_call_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    tool_name VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    args_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    owner VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    fencing_token BIGINT NOT NULL DEFAULT 1,
    error_class VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '',
    reviewer_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, invocation_id),
    UNIQUE KEY runtime_tool_invocation_event_call_idx (tenant_id, event_id, tool_call_id),
    KEY runtime_tool_invocation_status_idx (tenant_id, status, created_at),
    CONSTRAINT runtime_tool_invocation_status_ck CHECK (status IN ('prepared', 'dispatching', 'accepted', 'succeeded', 'failed', 'denied', 'unknown', 'manual')),
    CONSTRAINT runtime_tool_invocation_fence_ck CHECK (fencing_token >= 1),
    CONSTRAINT runtime_tool_invocation_sha_ck CHECK (CHAR_LENGTH(args_sha256) = 64),
    CONSTRAINT runtime_tool_invocation_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
