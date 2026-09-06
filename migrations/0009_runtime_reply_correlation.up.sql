-- Durable request/trace correlation for asynchronous reply delivery audits.
REVOKE ALL ON TABLE public.runtime_reply_correlation FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_reply_correlation TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_reply_correlation TO migration_owner;
