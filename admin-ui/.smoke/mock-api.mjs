// Temporary in-memory Admin API mock for UI screenshots. Deleted after use.
import http from 'node:http';

const now = () => new Date().toISOString();
const rid = () => crypto.randomUUID();

const tenant = {
  TenantID: 't_01JEXAMPLETENANT0000000001', TenantKey: 'acme', DisplayName: 'Acme 示例租户', Status: 'active',
  RateLimitRPM: 600, MaxConcurrentExecutions: 32, MonthlyTokenBudget: 50000000, MonthlySpendLimitMinor: 200000,
  BillingCurrency: 'CNY', AuditRetentionDays: 120, LogMaskingLevel: 'basic', TraceSamplingRate: 0.25,
  DefaultAgentAppID: 'a_01JEXAMPLEAPP0000000001', DefaultBackendProfileID: 'bp_01JEXAMPLEBACKEND00001',
  Version: 7, CreatedAt: now(), UpdatedAt: now(),
};

const app = {
  TenantID: tenant.TenantID, AppID: 'a_01JEXAMPLEAPP0000000001', AppKey: 'support-bot', DisplayName: '客服机器人',
  Description: '面向售后会话的一线客服 Agent，支持订单查询、物流跟踪与退款工具。', Status: 'active',
  CurrentRevision: 3, CanaryRevision: null, Version: 12, CreatedAt: now(), UpdatedAt: now(),
};

const model = {
  TenantID: tenant.TenantID, ProfileID: 'mp_01JEXAMPLEMODEL000000001', ProfileKey: 'gpt-main', DisplayName: '主力 GPT 模型',
  Description: '默认对话模型，覆盖客服与内部助手场景。', Status: 'active', SchemaVersion: 1,
  Configuration: {
    provider: 'openai', model: 'gpt-4o-mini', endpoint: 'https://api.openai.com/v1',
    options: { organization: 'acme', region: 'sg' }, secret_ref: 'sm://acme/openai-key',
    generation: { temperature: 0.4, top_p: 0.9, max_output_tokens: 2048 },
  },
  ContentDigest: 'sha256:9f2c1a', Version: 5, CreatedAt: now(), UpdatedAt: now(),
};

const backend = {
  TenantID: tenant.TenantID, ProfileID: 'bp_01JEXAMPLEBACKEND00001', ProfileKey: 'primary-store', DisplayName: '主力存储后端',
  Description: 'PostgreSQL 会话 + Redis 记忆。', Status: 'active', SchemaVersion: 1,
  Bindings: [
    { Capability: 'session', Provider: 'postgres', Endpoint: '', Options: { schema: 'runtime' }, SecretRef: 'sm://acme/pg-dsn' },
    { Capability: 'memory', Provider: 'redis', Endpoint: '', Options: { namespace: 'acme' }, SecretRef: 'sm://acme/redis-auth' },
  ],
  ContentDigest: 'sha256:77aa10', Version: 3, CreatedAt: now(), UpdatedAt: now(),
};

const binding = {
  TenantID: tenant.TenantID, BindingID: 'cb_01JEXAMPLEBINDING000001', BindingKey: 'wecom-support', Channel: 'wecom',
  ProviderAccountID: 'ww8f2example', PublicRouteKeyDigest: 'sha256:route-9c41', AppID: app.AppID,
  SecretRef: 'sm://acme/wecom-credentials',
  Protocol: { wecom: { corp_id: 'ww8f2example', agent_id: '1000002', receive_id: '' } },
  Status: 'active', Version: 4, ConfigDigest: 'sha256:cfg-5d20', CreatedAt: now(), UpdatedAt: now(),
};

let draftRevision = null;

const event = (type, prev, next) => ({
  EventType: type, TenantID: tenant.TenantID, Reason: 'screenshot', CorrelationID: rid(),
  PreviousVersion: prev, NextVersion: next, OccurredAt: now(),
});

function send(res, status, data, requestId) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ request_id: requestId, data }));
}

function readBody(req) {
  return new Promise((resolve) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      try { resolve(JSON.parse(raw || '{}')); } catch { resolve({}); }
    });
  });
}

const server = http.createServer(async (req, res) => {
  const requestId = req.headers['x-request-id'] || rid();
  const url = new URL(req.url, 'http://mock');
  const parts = url.pathname.replace(/^\/admin\/v1\/?/, '').split('/').filter(Boolean);
  const body = await readBody(req);
  const bump = (obj) => { obj.Version += 1; obj.UpdatedAt = now(); };

  try {
    if (parts[0] !== 'tenants') throw 404;
    if (parts.length === 1) {
      if (req.method === 'POST') { Object.assign(tenant, { TenantKey: body.tenant_key, DisplayName: body.display_name }); return send(res, 201, tenant, requestId); }
      throw 404;
    }
    const seg = parts.slice(2);
    if (seg.length === 0) {
      if (req.method === 'GET') return send(res, 200, tenant, requestId);
      if (req.method === 'PATCH') {
        const prev = tenant.Version;
        Object.assign(tenant, {
          DisplayName: body.display_name, RateLimitRPM: body.rate_limit_rpm, MaxConcurrentExecutions: body.max_concurrent_executions,
          MonthlyTokenBudget: body.monthly_token_budget, MonthlySpendLimitMinor: body.monthly_spend_limit_minor,
          BillingCurrency: body.billing_currency, AuditRetentionDays: body.audit_retention_days, LogMaskingLevel: body.log_masking_level,
          TraceSamplingRate: body.trace_sampling_rate, DefaultAgentAppID: body.default_agent_app_id, DefaultBackendProfileID: body.default_backend_profile_id,
        });
        bump(tenant);
        return send(res, 200, tenant, requestId);
      }
    }
    if (seg[0] === 'status' && req.method === 'POST') {
      const prev = tenant.Version; tenant.Status = body.next_status; bump(tenant);
      return send(res, 200, { tenant, event: event('status_changed', prev, tenant.Version) }, requestId);
    }
    if (seg[0] === 'apps') {
      if (seg.length === 1 && req.method === 'POST') {
        Object.assign(app, { AppKey: body.app_key, DisplayName: body.display_name, Description: body.description ?? '' });
        return send(res, 201, app, requestId);
      }
      if (seg.length === 2) {
        if (req.method === 'GET') return send(res, 200, app, requestId);
        if (req.method === 'PATCH') {
          Object.assign(app, { DisplayName: body.display_name, Description: body.description ?? '' }); bump(app);
          return send(res, 200, app, requestId);
        }
      }
      if (seg[2] === 'status') { const prev = app.Version; app.Status = body.next_status; bump(app); return send(res, 200, { app, event: event('status_changed', prev, app.Version) }, requestId); }
      if (seg[2] === 'revisions' && seg.length === 3 && req.method === 'POST') {
        const cfg = body.configuration ?? {};
        draftRevision = {
          TenantID: tenant.TenantID, AppID: app.AppID, Revision: 4, State: 'draft', DraftVersion: 1, Kind: 'llm', SchemaVersion: 1,
          Description: cfg.description ?? '', Instruction: cfg.instruction ?? '', GlobalInstruction: cfg.GlobalInstruction ?? '',
          ModelProfileID: cfg.model_profile_id ?? '', Generation: cfg.generation ?? {}, Runtime: cfg.runtime ?? {}, Tools: cfg.tools ?? [],
          ContentDigest: '', PublishedAt: null, CreatedAt: now(), UpdatedAt: now(),
        };
        bump(app);
        return send(res, 201, draftRevision, requestId);
      }
      if (seg[2] === 'revisions' && seg.length === 4 && req.method === 'PATCH') {
        const cfg = body.configuration ?? {};
        Object.assign(draftRevision, {
          Description: cfg.description ?? '', Instruction: cfg.instruction ?? '', GlobalInstruction: cfg.GlobalInstruction ?? '',
          ModelProfileID: cfg.model_profile_id ?? '', Generation: cfg.generation ?? {}, Runtime: cfg.runtime ?? {}, Tools: cfg.tools ?? [],
        });
        draftRevision.DraftVersion += 1; draftRevision.UpdatedAt = now(); bump(app);
        return send(res, 200, draftRevision, requestId);
      }
      if (seg[2] === 'revisions' && seg[4] === 'publish') {
        const prev = app.Version; app.CurrentRevision = draftRevision?.Revision ?? 4; bump(app);
        const published = { ...(draftRevision ?? {}), State: 'published', PublishedAt: now() };
        draftRevision = null;
        return send(res, 200, { app, revision: published, event: event('published', prev, app.Version) }, requestId);
      }
      if (seg[2] === 'rollback') { const prev = app.Version; app.CurrentRevision = body.target_revision; bump(app); return send(res, 200, { app, event: event('rolled_back', prev, app.Version) }, requestId); }
      if (seg[2] === 'canary') { const prev = app.Version; app.CanaryRevision = body.candidate_revision ?? null; bump(app); return send(res, 200, { app, event: event('canary', prev, app.Version) }, requestId); }
    }
    const profileRoutes = { models: model, backends: backend };
    if (profileRoutes[seg[0]]) {
      const store = profileRoutes[seg[0]];
      if (seg.length === 1 && req.method === 'POST') return send(res, 201, { profile: store, event: event('created', 0, store.Version) }, requestId);
      if (seg.length === 2 && req.method === 'GET') return send(res, 200, store, requestId);
      if (seg.length === 2 && req.method === 'PATCH') {
        const prev = store.Version;
        store.DisplayName = body.display_name ?? store.DisplayName; store.Description = body.description ?? store.Description;
        if (body.configuration) store.Configuration = { ...store.Configuration, ...body.configuration };
        if (body.bindings) store.Bindings = body.bindings;
        bump(store);
        return send(res, 200, { profile: store, event: event('updated', prev, store.Version) }, requestId);
      }
      if (seg[2] === 'status') { const prev = store.Version; store.Status = body.next_status; bump(store); return send(res, 200, { profile: store, event: event('status_changed', prev, store.Version) }, requestId); }
    }
    if (seg[0] === 'bindings') {
      if (seg.length === 1 && req.method === 'POST') return send(res, 201, { binding, event: event('created', 0, binding.Version) }, requestId);
      if (seg.length === 2 && req.method === 'GET') return send(res, 200, binding, requestId);
      if (seg.length === 2 && req.method === 'PATCH') {
        const prev = binding.Version;
        Object.assign(binding, {
          ProviderAccountID: body.provider_account_id, PublicRouteKeyDigest: body.public_route_key_digest,
          AppID: body.app_id, SecretRef: body.secret_ref, Protocol: body.protocol ?? binding.Protocol,
        });
        bump(binding);
        return send(res, 200, { binding, event: event('updated', prev, binding.Version) }, requestId);
      }
      if (seg[2] === 'status') { const prev = binding.Version; binding.Status = body.next_status; bump(binding); return send(res, 200, { binding, event: event('status_changed', prev, binding.Version) }, requestId); }
    }
    throw 404;
  } catch {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ request_id: requestId, error: 'not_found' }));
  }
});

server.listen(8787, '127.0.0.1', () => console.log('mock admin api on 127.0.0.1:8787'));
