package postgres

import (
	"context"
	"database/sql"

	modelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/model/postgres"
	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
	commonpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/schema/postgres"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
)

// SchemaTables lists the tables owned by this adapter.
var SchemaTables = []string{"agent_app", "agent_app_revision", "agent_app_revision_tool", "agent_app_change_outbox"}

// SchemaModule describes the agent-app PostgreSQL schema and its dependencies.
func SchemaModule() sharedschema.Module {
	return sharedschema.Module{
		Name:         "app",
		Driver:       sharedschema.DriverPostgres,
		Dependencies: []string{commonpostgres.SchemaModule().Name, tenantpostgres.SchemaModule().Name, modelpostgres.SchemaModule().Name},
		Tables:       append([]string(nil), SchemaTables...),
		SQL:          SchemaSQL,
	}
}

// InitDB initializes the agent-app tables and indexes owned by this package.
func InitDB(ctx context.Context, db *sql.DB) error {
	return commonpostgres.InitModule(ctx, db, SchemaModule())
}

// VerifySchema verifies that the agent-app-owned tables exist.
func VerifySchema(ctx context.Context, db *sql.DB) error {
	return sharedschema.Verify(ctx, db, sharedschema.DriverPostgres, SchemaTables)
}
