package mysql

import _ "embed"

// SchemaSQL contains the backend-profile MySQL base table schema.
//
// Cross-package constraints, mutation functions, triggers, grants, and later
// schema evolution remain migration-owned.
//
//go:embed schema.sql
var SchemaSQL string
