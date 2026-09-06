package postgres

import (
	"context"
	"database/sql"
	"fmt"

	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
)

import _ "embed"

// SchemaSQL contains shared PostgreSQL roles and validation helpers required
// by the domain-owned table schemas.
//
//go:embed schema.sql
var SchemaSQL string

// SchemaModule exposes the shared prerequisite as a normal schema module so
// the migration runner can execute it under its advisory lock.
func SchemaModule() sharedschema.Module {
	return sharedschema.Module{
		Name:   "postgres/common",
		Driver: sharedschema.DriverPostgres,
		SQL:    SchemaSQL,
	}
}

// InitDB initializes the shared PostgreSQL prerequisites.
func InitDB(ctx context.Context, db *sql.DB) error {
	if err := sharedschema.Apply(ctx, db, SchemaModule()); err != nil {
		return fmt.Errorf("postgres schema prerequisites: %w", err)
	}
	return nil
}

// InitModule initializes the shared prerequisites and then one domain module.
// It is the standalone entry point used by each PostgreSQL adapter package.
func InitModule(ctx context.Context, db *sql.DB, module sharedschema.Module) error {
	if err := InitDB(ctx, db); err != nil {
		return err
	}
	if err := sharedschema.Apply(ctx, db, module); err != nil {
		return fmt.Errorf("postgres schema module %s: %w", module.Name, err)
	}
	return nil
}
