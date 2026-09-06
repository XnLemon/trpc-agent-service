package mysql

import (
	"context"
	"database/sql"

	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
)

// SchemaTables lists the tables owned by this adapter.
var SchemaTables = []string{"model_profile", "model_profile_change_outbox"}

// SchemaModule describes the model MySQL schema and its dependencies.
func SchemaModule() sharedschema.Module {
	return sharedschema.Module{
		Name:         "model",
		Driver:       sharedschema.DriverMySQL,
		Dependencies: []string{tenantmysql.SchemaModule().Name},
		Tables:       append([]string(nil), SchemaTables...),
		SQL:          SchemaSQL,
	}
}

// InitDB initializes the model tables and indexes owned by this package.
func InitDB(ctx context.Context, db *sql.DB) error {
	return sharedschema.Apply(ctx, db, SchemaModule())
}

// VerifySchema verifies that the model-owned tables exist.
func VerifySchema(ctx context.Context, db *sql.DB) error {
	return sharedschema.Verify(ctx, db, sharedschema.DriverMySQL, SchemaTables)
}
