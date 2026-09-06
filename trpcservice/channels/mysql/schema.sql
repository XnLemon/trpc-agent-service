-- Package-owned base table schema for trpcservice/channels/mysql.
-- Keep cross-package foreign keys, functions, triggers, grants, and later
-- evolution steps in the migration orchestrator.

CREATE TABLE IF NOT EXISTS channel_binding (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    binding_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    binding_key VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    channel VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    provider_account_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    public_route_key_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    secret_ref VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    protocol_config JSON NOT NULL,
    schema_version INT NOT NULL,
    status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    version BIGINT NOT NULL,
    config_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    active_provider_account_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin GENERATED ALWAYS AS (CASE WHEN status = 'active' THEN provider_account_id ELSE NULL END) STORED,
    PRIMARY KEY (tenant_id, binding_id),
    UNIQUE KEY channel_binding_key_idx (tenant_id, binding_key),
    UNIQUE KEY channel_binding_active_account_idx (channel, active_provider_account_id),
    KEY channel_binding_candidate_idx (channel, public_route_key_digest, status),
    CONSTRAINT channel_binding_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id),
    CONSTRAINT channel_binding_app_fk FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app (tenant_id, app_id),
    CONSTRAINT channel_binding_channel_ck CHECK (channel IN ('wecom', 'telegram')),
    CONSTRAINT channel_binding_status_ck CHECK (status IN ('draft', 'active', 'suspended', 'disabled')),
    CONSTRAINT channel_binding_schema_ck CHECK (schema_version = 1),
    CONSTRAINT channel_binding_version_ck CHECK (version >= 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS channel_binding_change_outbox (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    event_type VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    binding_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    current_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    current_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor_type VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    actor_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    reason VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    correlation_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    KEY channel_binding_event_idx (tenant_id, binding_id, event_id),
    CONSTRAINT channel_binding_event_fk FOREIGN KEY (tenant_id, binding_id) REFERENCES channel_binding (tenant_id, binding_id),
    CONSTRAINT channel_binding_event_version_ck CHECK (next_version = previous_version + 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

