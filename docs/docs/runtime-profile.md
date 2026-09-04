# Agent Runtime Profiles

Runtime Profiles are tenant-scoped, versioned descriptors for the Agent
implementation selected by a published App Revision. A profile stores only
references and non-sensitive configuration; secrets, live clients and Runner
instances are never persisted.

## Identity and publication

The stable identity is `(tenant_id, runtime_profile_id)`. `runtime_key` is
unique inside a tenant. A published revision captures the profile id, kind,
execution mode, implementation version and digest, configuration digest, and
governance mode. Updating a profile does not change an existing revision;
publish a new revision to adopt the update. Rollback selects the historical
revision and therefore restores its runtime identity.

## Built-in catalog

| Kind | Version | Mode | Governance | Capabilities |
| --- | --- | --- | --- | --- |
| `builtin-llm` | `v1` | `builtin` | `full` | `text`, `tool` |
| `builtin-chain` | `v1` | `builtin` | `full` | `text`, `composition` |
| `builtin-parallel` | `v1` | `builtin` | `full` | `text`, `composition` |
| `builtin-cycle` | `v1` | `builtin` | `full` | `text`, `composition` |
| `builtin-graph` | `v1` | `builtin` | `full` | `text`, `composition` |

Composition is declarative and child references are validated for unknown
nodes, duplicate edges and cycles before publication.

## Remote runtimes

Remote implementations use protocol `runtime.v1` and must implement `Run`,
ordered Event streaming, `Cancel`, `Health`, and `Close`. Requests include the
tenant and profile identity, revision, implementation digest, request id,
deadline, user/session ids and message. Endpoints are supplied by trusted
profile configuration, never by an inbound message. Event ids must be unique
within a stream; malformed or duplicate events terminate the stream.

`full` governance means model, tool, session, storage and secret boundaries
are injected by the platform. `perimeter` is for remote runtimes that access
their own dependencies; the platform governs identity, network, resources,
入口/出口 and audit but does not claim complete internal governance.
