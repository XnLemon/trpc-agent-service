-- MySQL 8 control-plane schema.  Every statement is intentionally
-- idempotent: MySQL DDL implicitly commits, so the migration runner records a
-- statement checkpoint and resumes forward after a partial failure.

-- The following optional identity links are added after both sides exist.
-- The migration runner checkpoints each statement and treats duplicate
-- constraint errors as already-applied on recovery.
ALTER TABLE agent_app
    ADD CONSTRAINT agent_app_current_revision_fk
    FOREIGN KEY (tenant_id, app_id, current_revision)
    REFERENCES agent_app_revision (tenant_id, app_id, revision);

ALTER TABLE tenant
    ADD CONSTRAINT tenant_default_agent_fk
    FOREIGN KEY (tenant_id, default_agent_app_id)
    REFERENCES agent_app (tenant_id, app_id);

ALTER TABLE tenant
    ADD CONSTRAINT tenant_default_backend_fk
    FOREIGN KEY (tenant_id, default_backend_profile_id)
    REFERENCES backend_profile (tenant_id, profile_id);

-- Cross-row lifecycle and identity guards mirror the PostgreSQL control-plane
-- contract. MySQL has no deferrable constraint triggers, so backend profiles are
-- created disabled, bindings are written, and the repository then moves the
-- profile to its requested status in the same transaction.

CREATE TRIGGER agent_app_revision_guard_ins
BEFORE INSERT ON agent_app
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    IF NEW.current_revision IS NOT NULL THEN
        SELECT state INTO revision_state
        FROM agent_app_revision
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id AND revision = NEW.current_revision;
        IF revision_state IS NULL OR revision_state <> 'published' THEN
            SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'agent app current revision must be published';
        END IF;
    ELSEIF NEW.status IN ('active', 'suspended') THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'active or suspended agent app requires a current revision';
    END IF;
END;

CREATE TRIGGER agent_app_revision_guard_upd
BEFORE UPDATE ON agent_app
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    IF NEW.current_revision IS NOT NULL THEN
        SELECT state INTO revision_state
        FROM agent_app_revision
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id AND revision = NEW.current_revision;
        IF revision_state IS NULL OR revision_state <> 'published' THEN
            SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'agent app current revision must be published';
        END IF;
    ELSEIF NEW.status IN ('active', 'suspended') THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'active or suspended agent app requires a current revision';
    END IF;
END;

CREATE TRIGGER agent_revision_immutable_upd
BEFORE UPDATE ON agent_app_revision
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.app_id <> OLD.app_id OR NEW.revision <> OLD.revision THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'agent app revision identity is immutable';
    END IF;
    IF OLD.state = 'published' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'published agent app revision is immutable';
    END IF;
END;

CREATE TRIGGER agent_revision_immutable_del
BEFORE DELETE ON agent_app_revision
FOR EACH ROW
BEGIN
    IF OLD.state = 'published' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'published agent app revision is immutable';
    END IF;
END;

CREATE TRIGGER agent_revision_tool_guard_ins
BEFORE INSERT ON agent_app_revision_tool
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    SELECT state INTO revision_state
    FROM agent_app_revision
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id AND revision = NEW.revision;
    IF revision_state = 'published' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'published agent app tool authorization is immutable';
    END IF;
END;

CREATE TRIGGER agent_revision_tool_guard_upd
BEFORE UPDATE ON agent_app_revision_tool
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    SELECT state INTO revision_state
    FROM agent_app_revision
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id AND revision = OLD.revision;
    IF revision_state = 'published' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'published agent app tool authorization is immutable';
    END IF;
END;

CREATE TRIGGER agent_revision_tool_guard_del
BEFORE DELETE ON agent_app_revision_tool
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    SELECT state INTO revision_state
    FROM agent_app_revision
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id AND revision = OLD.revision;
    IF revision_state = 'published' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'published agent app tool authorization is immutable';
    END IF;
END;

CREATE TRIGGER tenant_identity_immutable_upd
BEFORE UPDATE ON tenant
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.tenant_key <> OLD.tenant_key THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'tenant identity is immutable';
    END IF;
END;

CREATE TRIGGER model_profile_identity_immutable_upd
BEFORE UPDATE ON model_profile
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.profile_id <> OLD.profile_id OR NEW.profile_key <> OLD.profile_key THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'model profile identity is immutable';
    END IF;
END;

CREATE TRIGGER agent_app_identity_immutable_upd
BEFORE UPDATE ON agent_app
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.app_id <> OLD.app_id OR NEW.app_key <> OLD.app_key THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'agent app identity is immutable';
    END IF;
END;

CREATE TRIGGER backend_profile_insert_guard
BEFORE INSERT ON backend_profile
FOR EACH ROW
BEGIN
    IF NEW.status <> 'disabled' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'backend profile must be created disabled before bindings';
    END IF;
END;

CREATE TRIGGER backend_profile_lifecycle_guard
BEFORE UPDATE ON backend_profile
FOR EACH ROW
BEGIN
    IF NEW.status <> 'disabled' AND NOT EXISTS (
        SELECT 1 FROM backend_profile_binding
        WHERE tenant_id = NEW.tenant_id AND profile_id = NEW.profile_id
    ) THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'non-disabled backend profile requires a binding';
    END IF;
    IF NEW.status = 'active' AND NOT EXISTS (
        SELECT 1 FROM backend_profile_binding
        WHERE tenant_id = NEW.tenant_id AND profile_id = NEW.profile_id AND capability = 'session'
    ) THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'active backend profile requires a session binding';
    END IF;
END;

CREATE TRIGGER backend_profile_identity_guard
BEFORE UPDATE ON backend_profile
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.profile_id <> OLD.profile_id OR NEW.profile_key <> OLD.profile_key THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'backend profile identity is immutable';
    END IF;
END;

CREATE TRIGGER backend_binding_identity_guard
BEFORE UPDATE ON backend_profile_binding
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.profile_id <> OLD.profile_id OR NEW.capability <> OLD.capability THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'backend profile binding identity is immutable';
    END IF;
END;

CREATE TRIGGER backend_binding_delete_guard
AFTER DELETE ON backend_profile_binding
FOR EACH ROW
BEGIN
    DECLARE profile_status VARCHAR(16) DEFAULT NULL;
    SELECT status INTO profile_status
    FROM backend_profile
    WHERE tenant_id = OLD.tenant_id AND profile_id = OLD.profile_id;
    IF profile_status IS NOT NULL AND profile_status <> 'disabled' AND NOT EXISTS (
        SELECT 1 FROM backend_profile_binding
        WHERE tenant_id = OLD.tenant_id AND profile_id = OLD.profile_id
    ) THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'non-disabled backend profile requires a binding';
    END IF;
    IF profile_status = 'active' AND NOT EXISTS (
        SELECT 1 FROM backend_profile_binding
        WHERE tenant_id = OLD.tenant_id AND profile_id = OLD.profile_id AND capability = 'session'
    ) THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'active backend profile requires a session binding';
    END IF;
END;

CREATE TRIGGER channel_binding_identity_guard
BEFORE UPDATE ON channel_binding
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.binding_id <> OLD.binding_id OR NEW.binding_key <> OLD.binding_key OR NEW.channel <> OLD.channel THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'channel binding identity is immutable';
    END IF;
END;
