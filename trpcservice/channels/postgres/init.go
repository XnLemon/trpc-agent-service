package postgres

import (
	"context"
	"database/sql"

	apppostgres "github.com/XnLemon/trpc-agent-service/trpcservice/app/postgres"
	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
	commonpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/schema/postgres"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
)

// SchemaTables lists the tables owned by this adapter.
var SchemaTables = []string{"channel_binding", "channel_binding_change_outbox"}

// SchemaModule describes the channel PostgreSQL schema and its dependencies.
func SchemaModule() sharedschema.Module {
	return sharedschema.Module{
		Name:         "channels",
		Driver:       sharedschema.DriverPostgres,
		Dependencies: []string{commonpostgres.SchemaModule().Name, tenantpostgres.SchemaModule().Name, apppostgres.SchemaModule().Name},
		Tables:       append([]string(nil), SchemaTables...),
		SQL:          SchemaSQL,
	}
}

// InitDB initializes the channel tables and indexes owned by this package.
func InitDB(ctx context.Context, db *sql.DB) error {
	return commonpostgres.InitModule(ctx, db, SchemaModule())
}

// VerifySchema verifies that the channel-owned tables exist.
func VerifySchema(ctx context.Context, db *sql.DB) error {
	return sharedschema.Verify(ctx, db, sharedschema.DriverPostgres, SchemaTables)
}
