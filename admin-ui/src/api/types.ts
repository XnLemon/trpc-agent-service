// Wire types matching the Admin HTTP API exactly as implemented in
// trpcservice/admin + domain packages. Response roots use Go default
// (PascalCase) JSON encoding; nested structs with json tags keep snake_case.
// See docs/docs/admin-web-ui.md for the full contract notes.

export type LifecycleStatus = 'draft' | 'active' | 'suspended' | 'disabled';
export type LogMaskingLevel = 'none' | 'basic' | 'strict';
export type ChannelKind = 'wecom' | 'telegram';
export type RevisionState = 'draft' | 'published';

export interface AuditEvent {
  EventType?: string;
  TenantID?: string;
  PreviousStatus?: string;
  CurrentStatus?: string;
  NextStatus?: string;
  ActorType?: string;
  ActorID?: string;
  Reason?: string;
  CorrelationID?: string;
  PreviousVersion?: number;
  NextVersion?: number;
  OccurredAt?: string;
  [key: string]: unknown;
}

export interface Tenant {
  TenantID: string;
  TenantKey: string;
  DisplayName: string;
  Status: LifecycleStatus;
  RateLimitRPM: number | null;
  MaxConcurrentExecutions: number | null;
  MonthlyTokenBudget: number | null;
  MonthlySpendLimitMinor: number | null;
  BillingCurrency: string;
  AuditRetentionDays: number;
  LogMaskingLevel: LogMaskingLevel;
  TraceSamplingRate: number;
  DefaultAgentAppID: string | null;
  DefaultBackendProfileID: string | null;
  Version: number;
  CreatedAt: string;
  UpdatedAt: string;
}

export interface App {
  TenantID: string;
  AppID: string;
  AppKey: string;
  DisplayName: string;
  Description: string;
  Status: LifecycleStatus;
  CurrentRevision: number | null;
  CanaryRevision: number | null;
  Version: number;
  CreatedAt: string;
  UpdatedAt: string;
}

export interface GenerationConfig {
  temperature?: number;
  top_p?: number;
  max_output_tokens?: number;
}

export interface RuntimePolicy {
  max_llm_calls: number;
  max_tool_calls: number;
  enable_parallel_tools: boolean;
  max_parallel_tools: number;
  execution_timeout_seconds: number;
}

export interface ToolAuthorization {
  tool_id: string;
  required: boolean;
}

export interface DraftConfiguration {
  description: string;
  instruction: string;
  globalInstruction: string;
  model_profile_id: string;
  generation: GenerationConfig;
  runtime: RuntimePolicy;
  tools: ToolAuthorization[];
}

export interface Revision {
  TenantID: string;
  AppID: string;
  Revision: number;
  State: RevisionState;
  DraftVersion: number;
  Kind: 'llm' | string;
  SchemaVersion: number;
  Description: string;
  Instruction: string;
  GlobalInstruction: string;
  ModelProfileID: string;
  Generation: GenerationConfig;
  Runtime: RuntimePolicy;
  Tools: ToolAuthorization[];
  ContentDigest: string;
  PublishedAt: string | null;
  CreatedAt: string;
  UpdatedAt: string;
}

export interface ModelConfiguration {
  provider: string;
  model: string;
  endpoint?: string;
  options?: Record<string, string>;
  secret_ref?: string;
  generation?: GenerationConfig;
}

export interface ModelProfile {
  TenantID: string;
  ProfileID: string;
  ProfileKey: string;
  DisplayName: string;
  Description: string;
  Status: LifecycleStatus;
  SchemaVersion: number;
  Configuration: ModelConfiguration;
  ContentDigest: string;
  Version: number;
  CreatedAt: string;
  UpdatedAt: string;
}

// NOTE: CapabilityBinding has no json tags on the server, so responses use
// PascalCase keys while requests may use snake_case (normalized server-side).
export interface CapabilityBinding {
  Capability: string;
  Provider: string;
  Endpoint: string;
  Options: Record<string, string> | null;
  SecretRef: string;
}

export interface BackendProfile {
  TenantID: string;
  ProfileID: string;
  ProfileKey: string;
  DisplayName: string;
  Description: string;
  Status: LifecycleStatus;
  SchemaVersion: number;
  Bindings: CapabilityBinding[];
  ContentDigest: string;
  Version: number;
  CreatedAt: string;
  UpdatedAt: string;
}

export interface WeComProtocolConfiguration {
  corp_id?: string;
  agent_id?: string;
  receive_id?: string;
}

export interface TelegramProtocolConfiguration {
  api_base_url?: string;
  webhook_path?: string;
}

export interface ProtocolConfiguration {
  wecom?: WeComProtocolConfiguration;
  telegram?: TelegramProtocolConfiguration;
}

export interface ChannelBinding {
  TenantID: string;
  BindingID: string;
  BindingKey: string;
  Channel: ChannelKind;
  ProviderAccountID: string;
  PublicRouteKeyDigest: string;
  AppID: string;
  SecretRef: string;
  Protocol: ProtocolConfiguration;
  Status: LifecycleStatus;
  Version: number;
  ConfigDigest: string;
  CreatedAt: string;
  UpdatedAt: string;
}

export interface WithEvent<T, K extends string> {
  [key: string]: unknown;
}

export interface TenantEvent {
  tenant: Tenant;
  event: AuditEvent;
}

export interface AppEvent {
  app: App;
  event: AuditEvent;
}

export interface PublishResult {
  app: App;
  revision: Revision;
  event: AuditEvent;
}

export interface ProfileEvent<T> {
  profile: T;
  event: AuditEvent;
}

export interface BindingEvent {
  binding: ChannelBinding;
  event: AuditEvent;
}
