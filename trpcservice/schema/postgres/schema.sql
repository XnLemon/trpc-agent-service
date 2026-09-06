-- Shared PostgreSQL prerequisites for the control-plane schema modules.
-- Domain packages own their tables; this package owns only cross-domain
-- validation helpers and bootstrap roles.

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
