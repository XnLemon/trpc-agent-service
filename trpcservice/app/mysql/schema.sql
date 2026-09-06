-- Package-owned base table schema for trpcservice/app/mysql.
-- Keep cross-package foreign keys, functions, triggers, grants, and later
-- evolution steps in the migration orchestrator.

CREATE TABLE IF NOT EXISTS agent_app (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    app_key VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    display_name VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    description VARCHAR(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    current_revision BIGINT NULL,
    version BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (tenant_id, app_id),
    UNIQUE KEY agent_app_key_idx (tenant_id, app_key),
    CONSTRAINT agent_app_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id),
    CONSTRAINT agent_app_status_ck CHECK (status IN ('draft', 'active', 'suspended', 'disabled')),
    CONSTRAINT agent_app_status_revision_ck CHECK ((status = 'draft' AND current_revision IS NULL) OR (status IN ('active', 'suspended') AND current_revision IS NOT NULL) OR status = 'disabled'),
    CONSTRAINT agent_app_version_ck CHECK (version >= 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS agent_app_revision (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    revision BIGINT NOT NULL,
    state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    draft_version BIGINT NOT NULL,
    agent_kind VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    schema_version INT NOT NULL,
    description VARCHAR(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    instruction TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    global_instruction TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    model_profile_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    generation_config JSON NOT NULL,
    runtime_policy JSON NOT NULL,
    content_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    published_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (tenant_id, app_id, revision),
    CONSTRAINT agent_revision_app_fk FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app (tenant_id, app_id),
    CONSTRAINT agent_revision_model_fk FOREIGN KEY (tenant_id, model_profile_id) REFERENCES model_profile (tenant_id, profile_id),
    CONSTRAINT agent_revision_state_ck CHECK (state IN ('draft', 'published')),
    CONSTRAINT agent_revision_kind_ck CHECK (agent_kind IN ('llm', 'chain')),
    CONSTRAINT agent_revision_schema_ck CHECK (schema_version = 1),
    CONSTRAINT agent_revision_version_ck CHECK (draft_version >= 1),
    CONSTRAINT agent_revision_digest_ck CHECK ((state = 'draft' AND content_digest IS NULL AND published_at IS NULL) OR (state = 'published' AND content_digest IS NOT NULL AND published_at IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS agent_app_revision_tool (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    revision BIGINT NOT NULL,
    tool_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    required BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (tenant_id, app_id, revision, tool_id),
    CONSTRAINT agent_revision_tool_fk FOREIGN KEY (tenant_id, app_id, revision) REFERENCES agent_app_revision (tenant_id, app_id, revision) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS agent_app_change_outbox (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    event_type VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    current_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_revision BIGINT NULL,
    current_revision BIGINT NULL,
    content_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    actor_type VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    actor_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    reason VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    correlation_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    KEY agent_app_event_idx (tenant_id, app_id, event_id),
    CONSTRAINT agent_app_event_fk FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app (tenant_id, app_id),
    CONSTRAINT agent_app_event_version_ck CHECK (next_version = previous_version + 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
