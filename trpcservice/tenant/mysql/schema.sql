-- Package-owned base table schema for trpcservice/tenant/mysql.
-- Keep cross-package foreign keys, functions, triggers, grants, and later
-- evolution steps in the migration orchestrator.

CREATE TABLE IF NOT EXISTS tenant (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    tenant_key VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    display_name VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'active',
    rate_limit_rpm BIGINT NULL,
    max_concurrent_executions BIGINT NULL,
    monthly_token_budget BIGINT NULL,
    monthly_spend_limit_minor BIGINT NULL,
    billing_currency VARCHAR(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    audit_retention_days INT NOT NULL DEFAULT 90,
    log_masking_level VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'basic',
    trace_sampling_rate DOUBLE NOT NULL DEFAULT 1.0,
    default_agent_app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    default_backend_profile_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (tenant_id),
    UNIQUE KEY tenant_key_idx (tenant_key),
    CONSTRAINT tenant_status_ck CHECK (status IN ('active', 'suspended', 'disabled')),
    CONSTRAINT tenant_rate_limit_ck CHECK (rate_limit_rpm IS NULL OR rate_limit_rpm >= 0),
    CONSTRAINT tenant_concurrency_ck CHECK (max_concurrent_executions IS NULL OR max_concurrent_executions > 0),
    CONSTRAINT tenant_token_budget_ck CHECK (monthly_token_budget IS NULL OR monthly_token_budget >= 0),
    CONSTRAINT tenant_spend_limit_ck CHECK (monthly_spend_limit_minor IS NULL OR monthly_spend_limit_minor >= 0),
    CONSTRAINT tenant_currency_ck CHECK (monthly_spend_limit_minor IS NULL OR billing_currency IS NOT NULL),
    CONSTRAINT tenant_retention_ck CHECK (audit_retention_days > 0),
    CONSTRAINT tenant_masking_ck CHECK (log_masking_level IN ('none', 'basic', 'strict')),
    CONSTRAINT tenant_sampling_ck CHECK (trace_sampling_rate BETWEEN 0 AND 1),
    CONSTRAINT tenant_version_ck CHECK (version >= 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS tenant_status_change_outbox (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    next_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    actor_type VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    actor_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    reason VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    correlation_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    KEY tenant_status_event_idx (tenant_id, event_id),
    CONSTRAINT tenant_status_event_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id),
    CONSTRAINT tenant_status_event_version_ck CHECK (next_version = previous_version + 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS tenant_configuration_outbox (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    CONSTRAINT tenant_configuration_event_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id),
    CONSTRAINT tenant_configuration_event_version_ck CHECK (next_version = previous_version + 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
