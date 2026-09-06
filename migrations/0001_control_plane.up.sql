BEGIN;

-- This migration is executed by a controlled database/migration owner. The
-- runtime roles below are deliberately NOLOGIN and receive only the grants at
-- the end of the migration.
SET LOCAL search_path = pg_catalog, public, pg_temp;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'migration_owner') THEN
        EXECUTE 'CREATE ROLE migration_owner NOLOGIN NOINHERIT';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'tenant_admin_writer') THEN
        EXECUTE 'CREATE ROLE tenant_admin_writer NOLOGIN NOINHERIT';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'tenant_app_writer') THEN
        EXECUTE 'CREATE ROLE tenant_app_writer NOLOGIN NOINHERIT';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.trim_control_plane_text(value TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT pg_catalog.btrim(
        value,
        U&'\0009\000A\000B\000C\000D\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000'
    )
$$;

CREATE OR REPLACE FUNCTION public.jsonb_object_string_values(value JSONB)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT pg_catalog.jsonb_typeof(value) = 'object'
       AND NOT EXISTS (
           SELECT 1
           FROM pg_catalog.jsonb_each(value) AS item(key, item_value)
           WHERE pg_catalog.jsonb_typeof(item.item_value) <> 'string'
       )
$$;

CREATE OR REPLACE FUNCTION public.jsonb_has_safe_keys(document JSONB)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    item RECORD;
    child JSONB;
BEGIN
    IF document IS NULL THEN
        RETURN FALSE;
    END IF;
    IF pg_catalog.jsonb_typeof(document) = 'object' THEN
        FOR item IN
            SELECT object_item.key, object_item.item_value
            FROM pg_catalog.jsonb_each(document) AS object_item(key, item_value)
        LOOP
            IF pg_catalog.lower(item.key) IN (
                'access_key', 'access_token', 'api_key', 'apikey', 'api_secret',
                'app_secret', 'authorization', 'bearer', 'bot_token',
                'client_secret', 'connection_string', 'credential', 'credentials',
                'dsn', 'encryption_key', 'password', 'passwd', 'passphrase',
                'private_key', 'privatekey', 'pwd', 'refresh_token', 'secret',
                'secret_key', 'secret_ref', 'secretref', 'signing_key', 'token',
                'username', 'webhook_secret'
            ) THEN
                RETURN FALSE;
            END IF;
            child := item.item_value;
            IF NOT public.jsonb_has_safe_keys(child) THEN
                RETURN FALSE;
            END IF;
        END LOOP;
    ELSIF pg_catalog.jsonb_typeof(document) = 'array' THEN
        FOR child IN
            SELECT array_item.child
            FROM pg_catalog.jsonb_array_elements(document) AS array_item(child)
        LOOP
            IF NOT public.jsonb_has_safe_keys(child) THEN
                RETURN FALSE;
            END IF;
        END LOOP;
    END IF;
    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_endpoint_is_safe(value TEXT)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog, public, pg_temp
AS $$
    SELECT value IS NOT NULL
       AND value = public.trim_control_plane_text(value)
       AND pg_catalog.length(value) <= 2048
       AND value !~ '[[:cntrl:]]'
       AND value !~ '[[:space:]]'
       AND value !~ '[?#@]'
       AND (
           value = ''
           OR value ~ '^[A-Za-z][A-Za-z0-9+.-]*://[^/[:space:]@?#]+(/[^[:space:]?#]*)?$'
       )
$$;

CREATE OR REPLACE FUNCTION public.control_plane_secret_ref_is_safe(value TEXT)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SET search_path = pg_catalog, public, pg_temp
AS $$
    SELECT value IS NOT NULL
       AND value = public.trim_control_plane_text(value)
       AND pg_catalog.length(value) <= 256
       AND value !~ '[[:cntrl:]]'
       AND value !~ '[[:space:]]'
$$;

CREATE OR REPLACE FUNCTION public.tenant_reject_identity_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.tenant_key IS DISTINCT FROM OLD.tenant_key THEN
        RAISE EXCEPTION 'tenant identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER tenant_identity_immutable
BEFORE UPDATE ON public.tenant
FOR EACH ROW EXECUTE FUNCTION public.tenant_reject_identity_change();

CREATE OR REPLACE FUNCTION public.model_profile_reject_identity_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.profile_id IS DISTINCT FROM OLD.profile_id
       OR NEW.profile_key IS DISTINCT FROM OLD.profile_key THEN
        RAISE EXCEPTION 'model profile identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER model_profile_identity_immutable
BEFORE UPDATE ON public.model_profile
FOR EACH ROW EXECUTE FUNCTION public.model_profile_reject_identity_change();

CREATE OR REPLACE FUNCTION public.agent_app_reject_identity_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.app_id IS DISTINCT FROM OLD.app_id
       OR NEW.app_key IS DISTINCT FROM OLD.app_key THEN
        RAISE EXCEPTION 'agent app identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_identity_immutable
BEFORE UPDATE ON public.agent_app
FOR EACH ROW EXECUTE FUNCTION public.agent_app_reject_identity_change();

ALTER TABLE public.agent_app
    ADD CONSTRAINT fk_agent_app_current_revision
    FOREIGN KEY (tenant_id, app_id, current_revision)
    REFERENCES public.agent_app_revision(tenant_id, app_id, revision);

CREATE OR REPLACE FUNCTION public.agent_app_current_revision_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_revision_state TEXT;
BEGIN
    IF NEW.current_revision IS NULL THEN
        IF NEW.status IN ('active', 'suspended') THEN
            RAISE EXCEPTION 'active or suspended agent app requires a current revision';
        END IF;
        RETURN NEW;
    END IF;
    SELECT state INTO v_revision_state
    FROM public.agent_app_revision
    WHERE tenant_id = NEW.tenant_id
      AND app_id = NEW.app_id
      AND revision = NEW.current_revision;
    IF NOT FOUND OR v_revision_state <> 'published' THEN
        RAISE EXCEPTION 'agent app current revision must be published';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER agent_app_current_revision_published_guard
AFTER INSERT OR UPDATE OF status, current_revision ON public.agent_app
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.agent_app_current_revision_guard();

CREATE OR REPLACE FUNCTION public.agent_app_revision_reject_published_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state = 'published' THEN
            RAISE EXCEPTION 'published agent app revision is immutable';
        END IF;
        -- Draft deletion is intentionally allowed; published history is not.
        RETURN OLD;
    END IF;
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.app_id IS DISTINCT FROM OLD.app_id
       OR NEW.revision IS DISTINCT FROM OLD.revision THEN
        RAISE EXCEPTION 'agent app revision identity is immutable';
    END IF;
    IF OLD.state = 'published' THEN
        RAISE EXCEPTION 'published agent app revision is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_revision_published_immutable
BEFORE UPDATE OR DELETE ON public.agent_app_revision
FOR EACH ROW EXECUTE FUNCTION public.agent_app_revision_reject_published_change();

CREATE OR REPLACE FUNCTION public.agent_app_revision_tool_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_id TEXT;
    v_app_id TEXT;
    v_revision BIGINT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_tenant_id := OLD.tenant_id;
        v_app_id := OLD.app_id;
        v_revision := OLD.revision;
    ELSE
        v_tenant_id := NEW.tenant_id;
        v_app_id := NEW.app_id;
        v_revision := NEW.revision;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.agent_app_revision
        WHERE tenant_id = v_tenant_id AND app_id = v_app_id
          AND revision = v_revision AND state = 'published'
    ) THEN
        RAISE EXCEPTION 'published agent app tool authorization is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_revision_tool_immutable
BEFORE INSERT OR UPDATE OR DELETE ON public.agent_app_revision_tool
FOR EACH ROW EXECUTE FUNCTION public.agent_app_revision_tool_guard();

CREATE OR REPLACE FUNCTION public.backend_profile_reject_disabled_insert()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'disabled' THEN
        RAISE EXCEPTION 'backend profile cannot be created disabled';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER backend_profile_no_disabled_insert
BEFORE INSERT ON public.backend_profile
FOR EACH ROW EXECUTE FUNCTION public.backend_profile_reject_disabled_insert();

CREATE OR REPLACE FUNCTION public.backend_profile_reject_identity_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.profile_id IS DISTINCT FROM OLD.profile_id
       OR NEW.profile_key IS DISTINCT FROM OLD.profile_key THEN
        RAISE EXCEPTION 'backend profile identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER backend_profile_identity_immutable
BEFORE UPDATE ON public.backend_profile
FOR EACH ROW EXECUTE FUNCTION public.backend_profile_reject_identity_change();

CREATE OR REPLACE FUNCTION public.backend_profile_binding_reject_identity_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.profile_id IS DISTINCT FROM OLD.profile_id
       OR NEW.capability IS DISTINCT FROM OLD.capability THEN
        RAISE EXCEPTION 'backend profile binding identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER backend_profile_binding_identity_immutable
BEFORE UPDATE ON public.backend_profile_binding
FOR EACH ROW EXECUTE FUNCTION public.backend_profile_binding_reject_identity_change();

CREATE OR REPLACE FUNCTION public.backend_profile_require_valid_bindings()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_id  TEXT;
    v_profile_id TEXT;
    v_status     TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_tenant_id := OLD.tenant_id;
        v_profile_id := OLD.profile_id;
    ELSE
        v_tenant_id := NEW.tenant_id;
        v_profile_id := NEW.profile_id;
    END IF;
    SELECT status INTO v_status
    FROM public.backend_profile
    WHERE tenant_id = v_tenant_id AND profile_id = v_profile_id;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    IF v_status <> 'disabled' AND NOT EXISTS (
        SELECT 1 FROM public.backend_profile_binding
        WHERE tenant_id = v_tenant_id AND profile_id = v_profile_id
    ) THEN
        RAISE EXCEPTION 'non-disabled backend profile requires at least one binding';
    END IF;
    IF v_status = 'active' AND NOT EXISTS (
        SELECT 1 FROM public.backend_profile_binding
        WHERE tenant_id = v_tenant_id AND profile_id = v_profile_id
          AND capability = 'session'
    ) THEN
        RAISE EXCEPTION 'active backend profile requires a session binding';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER backend_profile_root_bindings_guard
AFTER INSERT OR UPDATE OF status ON public.backend_profile
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.backend_profile_require_valid_bindings();

CREATE CONSTRAINT TRIGGER backend_profile_binding_rows_guard
AFTER INSERT OR UPDATE OR DELETE ON public.backend_profile_binding
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.backend_profile_require_valid_bindings();

CREATE OR REPLACE FUNCTION public.channel_binding_reject_identity_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.binding_id IS DISTINCT FROM OLD.binding_id
       OR NEW.binding_key IS DISTINCT FROM OLD.binding_key
       OR NEW.channel IS DISTINCT FROM OLD.channel THEN
        RAISE EXCEPTION 'channel binding identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER channel_binding_identity_immutable
BEFORE UPDATE ON public.channel_binding
FOR EACH ROW EXECUTE FUNCTION public.channel_binding_reject_identity_change();

ALTER TABLE public.tenant
    ADD CONSTRAINT fk_tenant_default_agent_app
    FOREIGN KEY (tenant_id, default_agent_app_id)
    REFERENCES public.agent_app(tenant_id, app_id);

ALTER TABLE public.tenant
    ADD CONSTRAINT fk_tenant_default_backend_profile
    FOREIGN KEY (tenant_id, default_backend_profile_id)
    REFERENCES public.backend_profile(tenant_id, profile_id);

CREATE OR REPLACE FUNCTION public.transition_tenant_status(
    p_tenant_id TEXT,
    p_expected_version BIGINT,
    p_next_status TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_previous_status TEXT;
    v_previous_version BIGINT;
    v_next_version BIGINT;
    v_event_id BIGINT;
    v_now TIMESTAMPTZ;
BEGIN
    SELECT status, version INTO v_previous_status, v_previous_version
    FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist'; END IF;
    IF v_previous_status = 'disabled' THEN RAISE EXCEPTION 'disabled tenant cannot be re-enabled'; END IF;
    IF v_previous_version <> p_expected_version THEN RAISE EXCEPTION 'tenant version conflict'; END IF;
    IF (v_previous_status, p_next_status) NOT IN (
        ('active', 'suspended'), ('active', 'disabled'),
        ('suspended', 'active'), ('suspended', 'disabled')
    ) THEN RAISE EXCEPTION 'invalid tenant status transition'; END IF;
    IF p_actor_type IS NULL OR pg_catalog.length(public.trim_control_plane_text(p_actor_type)) = 0
       OR p_actor_id IS NULL OR pg_catalog.length(public.trim_control_plane_text(p_actor_id)) = 0
       OR p_reason IS NULL OR pg_catalog.length(public.trim_control_plane_text(p_reason)) NOT BETWEEN 1 AND 1000
       OR p_correlation_id IS NULL OR pg_catalog.length(public.trim_control_plane_text(p_correlation_id)) = 0
    THEN RAISE EXCEPTION 'tenant status transition requires valid audit metadata'; END IF;
    v_now := GREATEST(clock_timestamp(), (SELECT updated_at FROM public.tenant WHERE tenant_id = p_tenant_id));
    UPDATE public.tenant
    SET status = p_next_status, version = version + 1, updated_at = v_now
    WHERE tenant_id = p_tenant_id AND version = p_expected_version
    RETURNING version INTO v_next_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'tenant version conflict'; END IF;
    INSERT INTO public.tenant_status_change_outbox (
        tenant_id, previous_status, next_status, actor_type, actor_id, reason,
        previous_version, next_version, correlation_id, occurred_at
    ) VALUES (
        p_tenant_id, v_previous_status, p_next_status,
        public.trim_control_plane_text(p_actor_type), public.trim_control_plane_text(p_actor_id),
        public.trim_control_plane_text(p_reason), p_expected_version, v_next_version,
        public.trim_control_plane_text(p_correlation_id), v_now
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.update_tenant_configuration(
    p_tenant_id TEXT,
    p_expected_version BIGINT,
    p_display_name TEXT,
    p_rate_limit_rpm BIGINT,
    p_max_concurrent_executions BIGINT,
    p_monthly_token_budget BIGINT,
    p_monthly_spend_limit_minor BIGINT,
    p_billing_currency CHAR(3),
    p_audit_retention_days INT,
    p_log_masking_level TEXT,
    p_trace_sampling_rate REAL,
    p_default_agent_app_id TEXT,
    p_default_backend_profile_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_next_version BIGINT;
    v_now TIMESTAMPTZ;
BEGIN
    PERFORM 1 FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist'; END IF;
    IF p_default_agent_app_id IS NOT NULL THEN
        PERFORM 1 FROM public.agent_app
        WHERE tenant_id = p_tenant_id AND app_id = p_default_agent_app_id
          AND status = 'active' FOR UPDATE;
        IF NOT FOUND THEN RAISE EXCEPTION 'default agent app must exist in the tenant and be active'; END IF;
    END IF;
    IF p_default_backend_profile_id IS NOT NULL THEN
        PERFORM 1 FROM public.backend_profile
        WHERE tenant_id = p_tenant_id AND profile_id = p_default_backend_profile_id
          AND status = 'active' FOR UPDATE;
        IF NOT FOUND THEN RAISE EXCEPTION 'default backend profile must exist in the tenant and be active'; END IF;
    END IF;
    v_now := clock_timestamp();
    UPDATE public.tenant
    SET display_name = p_display_name,
        rate_limit_rpm = p_rate_limit_rpm,
        max_concurrent_executions = p_max_concurrent_executions,
        monthly_token_budget = p_monthly_token_budget,
        monthly_spend_limit_minor = p_monthly_spend_limit_minor,
        billing_currency = p_billing_currency,
        audit_retention_days = p_audit_retention_days,
        log_masking_level = p_log_masking_level,
        trace_sampling_rate = p_trace_sampling_rate,
        default_agent_app_id = p_default_agent_app_id,
        default_backend_profile_id = p_default_backend_profile_id,
        version = version + 1,
        updated_at = GREATEST(v_now, updated_at)
    WHERE tenant_id = p_tenant_id
      AND version = p_expected_version
      AND status <> 'disabled'
    RETURNING version INTO v_next_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'tenant is disabled, has a version conflict, or does not exist'; END IF;
    INSERT INTO public.tenant_configuration_outbox (tenant_id, previous_version, next_version, occurred_at)
    VALUES (p_tenant_id, p_expected_version, v_next_version, GREATEST(v_now, (SELECT updated_at FROM public.tenant WHERE tenant_id = p_tenant_id)));
    RETURN v_next_version;
END;
$$;

CREATE OR REPLACE FUNCTION public.transition_backend_profile_status(
    p_tenant_id TEXT,
    p_profile_id TEXT,
    p_expected_version BIGINT,
    p_next_status TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_previous_status TEXT;
    v_previous_version BIGINT;
    v_digest TEXT;
    v_default_profile_id TEXT;
    v_next_version BIGINT;
    v_now TIMESTAMPTZ;
BEGIN
    SELECT default_backend_profile_id INTO v_default_profile_id
    FROM public.tenant WHERE tenant_id = p_tenant_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist'; END IF;
    SELECT status, version, content_digest INTO v_previous_status, v_previous_version, v_digest
    FROM public.backend_profile
    WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'backend profile does not exist'; END IF;
    IF v_previous_status = 'disabled' THEN RAISE EXCEPTION 'backend profile is disabled'; END IF;
    IF v_previous_version <> p_expected_version THEN RAISE EXCEPTION 'backend profile version conflict'; END IF;
    IF (v_previous_status, p_next_status) NOT IN (
        ('active', 'suspended'), ('active', 'disabled'),
        ('suspended', 'active'), ('suspended', 'disabled')
    ) THEN RAISE EXCEPTION 'invalid backend profile status transition'; END IF;
    IF p_next_status = 'active' AND NOT EXISTS (
        SELECT 1 FROM public.backend_profile_binding
        WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id AND capability = 'session'
    ) THEN RAISE EXCEPTION 'active backend profile requires a session binding'; END IF;
    IF p_next_status = 'disabled' AND v_default_profile_id = p_profile_id THEN
        RAISE EXCEPTION 'tenant default backend profile must be switched first';
    END IF;
    IF p_actor_type IS NULL OR pg_catalog.length(public.trim_control_plane_text(p_actor_type)) = 0
       OR p_actor_id IS NULL OR pg_catalog.length(public.trim_control_plane_text(p_actor_id)) = 0
       OR p_reason IS NULL OR pg_catalog.length(public.trim_control_plane_text(p_reason)) NOT BETWEEN 1 AND 1000
       OR p_correlation_id IS NULL OR pg_catalog.length(public.trim_control_plane_text(p_correlation_id)) = 0
    THEN RAISE EXCEPTION 'backend profile transition requires valid audit metadata'; END IF;
    v_now := GREATEST(clock_timestamp(), (SELECT updated_at FROM public.backend_profile WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id));
    UPDATE public.backend_profile
    SET status = p_next_status, version = version + 1, updated_at = v_now
    WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id AND version = p_expected_version
    RETURNING version INTO v_next_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'backend profile version conflict'; END IF;
    INSERT INTO public.backend_profile_change_outbox (
        event_type, tenant_id, profile_id, previous_status, current_status,
        previous_digest, current_digest, actor_type, actor_id, reason, correlation_id,
        previous_version, next_version, occurred_at
    ) VALUES (
        CASE p_next_status WHEN 'suspended' THEN 'suspended' WHEN 'active' THEN 'resumed' ELSE 'disabled' END,
        p_tenant_id, p_profile_id, v_previous_status, p_next_status, v_digest, v_digest,
        public.trim_control_plane_text(p_actor_type), public.trim_control_plane_text(p_actor_id),
        public.trim_control_plane_text(p_reason), public.trim_control_plane_text(p_correlation_id),
        p_expected_version, v_next_version, v_now
    );
    RETURN v_next_version;
END;
$$;

-- Runtime roles do not receive direct DML on control-plane tables. Repository
-- mutations use SECURITY DEFINER entry points owned by migration_owner; the
-- admin role only reads snapshots/outbox events and executes those entry
-- points. The worker role intentionally has no control-plane table access.
REVOKE ALL PRIVILEGES ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO migration_owner, tenant_admin_writer, tenant_app_writer;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC;
GRANT ALL PRIVILEGES ON public.tenant, public.model_profile,
    public.agent_app, public.agent_app_revision, public.agent_app_revision_tool,
    public.backend_profile, public.backend_profile_binding, public.channel_binding,
    public.model_profile_change_outbox, public.agent_app_change_outbox,
    public.backend_profile_change_outbox, public.channel_binding_change_outbox,
    public.tenant_configuration_outbox, public.tenant_status_change_outbox TO migration_owner;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO migration_owner;
GRANT SELECT ON public.tenant, public.model_profile, public.agent_app,
    public.agent_app_revision, public.agent_app_revision_tool,
    public.backend_profile, public.backend_profile_binding, public.channel_binding,
    public.tenant_status_change_outbox, public.model_profile_change_outbox,
    public.backend_profile_change_outbox, public.agent_app_change_outbox,
    public.channel_binding_change_outbox, public.tenant_configuration_outbox
    TO tenant_admin_writer;
REVOKE ALL ON FUNCTION public.transition_tenant_status(TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.update_tenant_configuration(TEXT, BIGINT, TEXT, BIGINT, BIGINT, BIGINT, BIGINT, CHAR(3), INT, TEXT, REAL, TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.transition_backend_profile_status(TEXT, TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.transition_tenant_status(TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT) TO tenant_admin_writer;
GRANT EXECUTE ON FUNCTION public.update_tenant_configuration(TEXT, BIGINT, TEXT, BIGINT, BIGINT, BIGINT, BIGINT, CHAR(3), INT, TEXT, REAL, TEXT, TEXT) TO tenant_admin_writer;
GRANT EXECUTE ON FUNCTION public.transition_backend_profile_status(TEXT, TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT) TO tenant_admin_writer;
ALTER FUNCTION public.transition_tenant_status(TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT) OWNER TO migration_owner;
ALTER FUNCTION public.update_tenant_configuration(TEXT, BIGINT, TEXT, BIGINT, BIGINT, BIGINT, BIGINT, CHAR(3), INT, TEXT, REAL, TEXT, TEXT) OWNER TO migration_owner;
ALTER FUNCTION public.transition_backend_profile_status(TEXT, TEXT, BIGINT, TEXT, TEXT, TEXT, TEXT, TEXT) OWNER TO migration_owner;

COMMIT;
