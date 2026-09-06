package postgres

import (
	"context"
	"database/sql"

	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
	commonpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/schema/postgres"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
)

// SchemaTables lists the tables owned by this adapter.
var SchemaTables = []string{"audit_event", "execution_audit_handoff"}

// SchemaModule describes the audit PostgreSQL schema and its dependencies.
func SchemaModule() sharedschema.Module {
	return sharedschema.Module{
		Name:         "audit",
		Driver:       sharedschema.DriverPostgres,
		Dependencies: []string{commonpostgres.SchemaModule().Name, tenantpostgres.SchemaModule().Name},
		Tables:       append([]string(nil), SchemaTables...),
		SQL:          SchemaSQL,
	}
}

// InitDB initializes the audit tables and indexes owned by this package.
func InitDB(ctx context.Context, db *sql.DB) error {
	return commonpostgres.InitModule(ctx, db, SchemaModule())
}

// VerifySchema verifies that the audit-owned tables exist.
func VerifySchema(ctx context.Context, db *sql.DB) error {
	return sharedschema.Verify(ctx, db, sharedschema.DriverPostgres, SchemaTables)
}
