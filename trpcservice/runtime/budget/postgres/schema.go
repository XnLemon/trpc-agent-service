package postgres

import _ "embed"

// SchemaSQL contains the PostgreSQL budget ledger tables. Cross-package
// migration ordering and grants remain owned by the migration orchestrator.
//
//go:embed schema.sql
var SchemaSQL string
