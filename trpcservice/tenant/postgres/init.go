package postgres

import (
	"context"
	"database/sql"

	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
	commonpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/schema/postgres"
)

// SchemaTables lists the tables owned by this adapter.
var SchemaTables = []string{"tenant", "tenant_status_change_outbox", "tenant_configuration_outbox"}

// SchemaModule describes the tenant PostgreSQL schema and its dependencies.
func SchemaModule() sharedschema.Module {
	return sharedschema.Module{
		Name:         "tenant",
		Driver:       sharedschema.DriverPostgres,
		Dependencies: []string{commonpostgres.SchemaModule().Name},
		Tables:       append([]string(nil), SchemaTables...),
		SQL:          SchemaSQL,
	}
}

// InitDB initializes the tenant tables and indexes owned by this package.
func InitDB(ctx context.Context, db *sql.DB) error {
	return commonpostgres.InitModule(ctx, db, SchemaModule())
}

// VerifySchema verifies that the tenant-owned tables exist.
func VerifySchema(ctx context.Context, db *sql.DB) error {
	return sharedschema.Verify(ctx, db, sharedschema.DriverPostgres, SchemaTables)
}
