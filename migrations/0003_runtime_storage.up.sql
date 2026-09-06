-- Issue #48: tenant-scoped runtime Session, inbound event and reply outbox facts.
SET LOCAL search_path = pg_catalog, public, pg_temp;
REVOKE ALL ON TABLE public.runtime_session, public.message_event, public.reply_outbox FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_session, public.message_event, public.reply_outbox TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_session, public.message_event, public.reply_outbox TO migration_owner;
