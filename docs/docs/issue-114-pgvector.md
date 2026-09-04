# Issue #114: PostgreSQL/pgvector Knowledge Retrieval

The `pgvector` Backend Profile provider is the first durable knowledge
retrieval implementation. It binds one PostgreSQL pool to one trusted tenant
and validates the profile endpoint, schema, collection, embedding model,
embedding version, vector dimension, queue size, and worker count before a
runtime capability is materialized.

## Lifecycle

`Upsert` commits the source row first with `index_status = pending`. A bounded
provider-owned worker then uses the configured embedding boundary and marks the
same version `ready` only after a vector of the configured dimension is
available. Embedding failures become `failed` with a low-cardinality error
class; after the bounded attempt budget they become `dead_letter`. `Retry` and
`Reindex` replay the stable document key. `Delete` marks the
source `deleted` and clears its vector, so stale rows cannot be returned.

Provider construction requeues all durable pending rows, draining them through
the bounded queue so accepted writes resume indexing after a restart. Worker
updates are fenced by the source version, and `Close` cancels the owned worker
context before waiting for shutdown. A blocking embedder must honor context
cancellation to release its own resources.

The default embedder is deterministic and local, which keeps normal CI free of
external credentials. Deployments may inject an `Embedder` implementation, but
tenant document data never selects its endpoint or credentials. A model or
dimension mismatch fails closed as `ErrIncompatible`.

## Retrieval boundary

Every query carries the provider's immutable tenant ID. SQL restricts rows to
that tenant, `ready` status, non-deleted rows, and non-null vectors before the
provider applies trusted metadata and authorization filters. Only then are
cosine scores calculated, ties ordered by stable document/chunk IDs, and the
bounded limit applied. Results contain defensive metadata copies and never
return raw database or provider errors.

Migration `0017_runtime_pgvector_knowledge.up.sql` enables the PostgreSQL
`vector` extension and creates the tenant-composite source table. It is
append-only in migration history; changing model or dimension requires a new
profile version and an explicit reindex/migration plan.

The local Compose PostgreSQL service is the intended opt-in validation target.
Set a Backend Profile binding with `Provider: "pgvector"` and an endpoint such
as `postgresql://postgres:5432` plus the validated namespace options. The
runtime reuses the already authenticated control-plane pool; credentials stay
in the operator-managed DSN and Secret boundary.
