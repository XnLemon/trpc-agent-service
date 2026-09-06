package mysql

import (
	"context"
	"database/sql"

	modelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/model/mysql"
	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
)

// SchemaTables lists the tables owned by this adapter.
var SchemaTables = []string{"agent_app", "agent_app_revision", "agent_app_revision_tool", "agent_app_change_outbox"}

// SchemaModule describes the agent-app MySQL schema and its dependencies.
func SchemaModule() sharedschema.Module {
	return sharedschema.Module{
		Name:         "app",
		Driver:       sharedschema.DriverMySQL,
		Dependencies: []string{tenantmysql.SchemaModule().Name, modelmysql.SchemaModule().Name},
		Tables:       append([]string(nil), SchemaTables...),
		SQL:          SchemaSQL,
	}
}

// InitDB initializes the agent-app tables and indexes owned by this package.
func InitDB(ctx context.Context, db *sql.DB) error {
	return sharedschema.Apply(ctx, db, SchemaModule())
}

// VerifySchema verifies that the agent-app-owned tables exist.
func VerifySchema(ctx context.Context, db *sql.DB) error {
	return sharedschema.Verify(ctx, db, sharedschema.DriverMySQL, SchemaTables)
}
