import type {
  App,
  AppEvent,
  BackendProfile,
  BindingEvent,
  ChannelBinding,
  DraftConfiguration,
  LifecycleStatus,
  ModelProfile,
  ProfileEvent,
  PublishResult,
  Revision,
  Tenant,
  TenantEvent,
} from './types';

export type ErrorCategory =
  | 'invalid_request'
  | 'unauthorized'
  | 'forbidden'
  | 'not_found'
  | 'conflict'
  | 'storage_unavailable'
  | 'audit_unavailable'
  | 'internal_error';

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly category: ErrorCategory | string,
    public readonly requestId?: string,
  ) {
    super(category);
    this.name = 'ApiError';
  }
}

export interface Connection {
  baseUrl: string;
}

export interface AdminPrincipal {
  subject_id: string;
  global: boolean;
  tenant_scopes: string[];
  can_create_tenant: boolean;
}

export interface ListResponse<T> {
  items: T[];
  next_cursor?: string;
  total?: number | null;
}

interface RequestResult<T> {
  data: T;
  requestId: string;
}

/**
 * AdminClient talks to the same-origin control-plane API using the HttpOnly
 * session cookie issued by /admin/auth/login.
 */
export class AdminClient {
  constructor(
    private readonly connection: Connection,
    private readonly onUnauthorized?: () => void,
  ) {}

  private async request<T>(method: string, path: string, body?: unknown): Promise<RequestResult<T>> {
    let response: Response;
    try {
      response = await fetch(`${this.connection.baseUrl}${path}`, {
        method,
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    } catch {
      throw new ApiError(0, 'network_error');
    }
    let payload: { request_id?: string; data?: T; error?: string } | null = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    if (!response.ok) {
      const category = payload?.error ?? 'internal_error';
      if (response.status === 401) {
        this.onUnauthorized?.();
      }
      throw new ApiError(response.status, category, payload?.request_id);
    }
    return { data: payload?.data as T, requestId: payload?.request_id ?? '' };
  }

  private get<T>(path: string) {
    return this.request<T>('GET', path);
  }

  private post<T>(path: string, body: unknown) {
    return this.request<T>('POST', path, body);
  }

  private patch<T>(path: string, body: unknown) {
    return this.request<T>('PATCH', path, body);
  }

  private list<T>(path: string, params: Record<string, string | number | undefined> = {}) {
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) if (value !== undefined && value !== '') query.set(key, String(value));
    return this.get<ListResponse<T>>(`${path}${query.toString() ? `?${query}` : ''}`);
  }

  login(username: string, password: string) {
    return this.post<AdminPrincipal>('/admin/auth/login', { username, password });
  }

  getSession() {
    return this.get<AdminPrincipal>('/admin/auth/session');
  }

  logout() {
    return this.post<{ logged_out: boolean }>('/admin/auth/logout', {});
  }

  getMe() { return this.get<AdminPrincipal>('/admin/v1/me'); }
  listTenants(params: Record<string, string | number | undefined> = {}) { return this.list<Tenant>('/admin/v1/tenants', params); }
  listApps(tenantId: string, params: Record<string, string | number | undefined> = {}) { return this.list<App>(this.tenantPath(tenantId, 'apps'), params); }
  listRevisions(tenantId: string, appId: string, params: Record<string, string | number | undefined> = {}) { return this.list<Revision>(this.tenantPath(tenantId, `apps/${encodeURIComponent(appId)}/revisions`), params); }
  listModels(tenantId: string, params: Record<string, string | number | undefined> = {}) { return this.list<ModelProfile>(this.tenantPath(tenantId, 'models'), params); }
  listBackends(tenantId: string, params: Record<string, string | number | undefined> = {}) { return this.list<BackendProfile>(this.tenantPath(tenantId, 'backends'), params); }
  listBindings(tenantId: string, params: Record<string, string | number | undefined> = {}) { return this.list<ChannelBinding>(this.tenantPath(tenantId, 'bindings'), params); }

  // --- Tenant -------------------------------------------------------------

  createTenant(body: Record<string, unknown>) {
    return this.post<Tenant>('/admin/v1/tenants', body);
  }

  getTenant(tenantId: string) {
    return this.get<Tenant>(`/admin/v1/tenants/${encodeURIComponent(tenantId)}`);
  }

  updateTenant(tenantId: string, body: Record<string, unknown>) {
    return this.patch<Tenant>(`/admin/v1/tenants/${encodeURIComponent(tenantId)}`, body);
  }

  transitionTenantStatus(tenantId: string, body: Record<string, unknown>) {
    return this.post<TenantEvent>(`/admin/v1/tenants/${encodeURIComponent(tenantId)}/status`, body);
  }

  // --- App & Revision -----------------------------------------------------

  private tenantPath(tenantId: string, suffix: string) {
    return `/admin/v1/tenants/${encodeURIComponent(tenantId)}/${suffix}`;
  }

  createApp(tenantId: string, body: Record<string, unknown>) {
    return this.post<App>(this.tenantPath(tenantId, 'apps'), body);
  }

  getApp(tenantId: string, appId: string) {
    return this.get<App>(this.tenantPath(tenantId, `apps/${encodeURIComponent(appId)}`));
  }

  updateApp(tenantId: string, appId: string, body: Record<string, unknown>) {
    return this.patch<App>(this.tenantPath(tenantId, `apps/${encodeURIComponent(appId)}`), body);
  }

  transitionAppStatus(tenantId: string, appId: string, body: Record<string, unknown>) {
    return this.post<AppEvent>(this.tenantPath(tenantId, `apps/${encodeURIComponent(appId)}/status`), body);
  }

  createDraft(tenantId: string, appId: string, body: Record<string, unknown>) {
    return this.post<Revision>(this.tenantPath(tenantId, `apps/${encodeURIComponent(appId)}/revisions`), body);
  }

  updateDraft(tenantId: string, appId: string, revision: number, body: Record<string, unknown>) {
    return this.patch<Revision>(
      this.tenantPath(tenantId, `apps/${encodeURIComponent(appId)}/revisions/${revision}`),
      body,
    );
  }

  publishDraft(tenantId: string, appId: string, revision: number, body: Record<string, unknown>) {
    return this.post<PublishResult>(
      this.tenantPath(tenantId, `apps/${encodeURIComponent(appId)}/revisions/${revision}/publish`),
      body,
    );
  }

  rollbackApp(tenantId: string, appId: string, body: Record<string, unknown>) {
    return this.post<AppEvent>(this.tenantPath(tenantId, `apps/${encodeURIComponent(appId)}/rollback`), body);
  }

  setCanary(tenantId: string, appId: string, body: Record<string, unknown>) {
    return this.post<AppEvent>(this.tenantPath(tenantId, `apps/${encodeURIComponent(appId)}/canary`), body);
  }

  // --- Model Profile ------------------------------------------------------

  createModel(tenantId: string, body: Record<string, unknown>) {
    return this.post<ProfileEvent<ModelProfile>>(this.tenantPath(tenantId, 'models'), body);
  }

  getModel(tenantId: string, profileId: string) {
    return this.get<ModelProfile>(this.tenantPath(tenantId, `models/${encodeURIComponent(profileId)}`));
  }

  updateModel(tenantId: string, profileId: string, body: Record<string, unknown>) {
    return this.patch<ProfileEvent<ModelProfile>>(
      this.tenantPath(tenantId, `models/${encodeURIComponent(profileId)}`),
      body,
    );
  }

  transitionModelStatus(tenantId: string, profileId: string, body: Record<string, unknown>) {
    return this.post<ProfileEvent<ModelProfile>>(
      this.tenantPath(tenantId, `models/${encodeURIComponent(profileId)}/status`),
      body,
    );
  }

  // --- Backend Profile ----------------------------------------------------

  createBackend(tenantId: string, body: Record<string, unknown>) {
    return this.post<ProfileEvent<BackendProfile>>(this.tenantPath(tenantId, 'backends'), body);
  }

  getBackend(tenantId: string, profileId: string) {
    return this.get<BackendProfile>(this.tenantPath(tenantId, `backends/${encodeURIComponent(profileId)}`));
  }

  updateBackend(tenantId: string, profileId: string, body: Record<string, unknown>) {
    return this.patch<ProfileEvent<BackendProfile>>(
      this.tenantPath(tenantId, `backends/${encodeURIComponent(profileId)}`),
      body,
    );
  }

  transitionBackendStatus(tenantId: string, profileId: string, body: Record<string, unknown>) {
    return this.post<ProfileEvent<BackendProfile>>(
      this.tenantPath(tenantId, `backends/${encodeURIComponent(profileId)}/status`),
      body,
    );
  }

  // --- Channel Binding ----------------------------------------------------

  createBinding(tenantId: string, body: Record<string, unknown>) {
    return this.post<BindingEvent>(this.tenantPath(tenantId, 'bindings'), body);
  }

  getBinding(tenantId: string, bindingId: string) {
    return this.get<ChannelBinding>(this.tenantPath(tenantId, `bindings/${encodeURIComponent(bindingId)}`));
  }

  updateBinding(tenantId: string, bindingId: string, body: Record<string, unknown>) {
    return this.patch<BindingEvent>(this.tenantPath(tenantId, `bindings/${encodeURIComponent(bindingId)}`), body);
  }

  transitionBindingStatus(tenantId: string, bindingId: string, body: Record<string, unknown>) {
    return this.post<BindingEvent>(
      this.tenantPath(tenantId, `bindings/${encodeURIComponent(bindingId)}/status`),
      body,
    );
  }
}

export type { DraftConfiguration, LifecycleStatus };
