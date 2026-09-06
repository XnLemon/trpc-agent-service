package postgres

import _ "embed"

// SchemaSQL contains the agent-app PostgreSQL base table schema.
//
// Cross-package constraints, mutation functions, triggers, grants, and later
// schema evolution remain migration-owned.
//
//go:embed schema.sql
var SchemaSQL string
