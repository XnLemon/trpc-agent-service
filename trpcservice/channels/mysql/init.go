package mysql

import (
	"context"
	"database/sql"

	appmysql "github.com/XnLemon/trpc-agent-service/trpcservice/app/mysql"
	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
)

// SchemaTables lists the tables owned by this adapter.
var SchemaTables = []string{"channel_binding", "channel_binding_change_outbox"}

// SchemaModule describes the channel MySQL schema and its dependencies.
func SchemaModule() sharedschema.Module {
	return sharedschema.Module{
		Name:         "channels",
		Driver:       sharedschema.DriverMySQL,
		Dependencies: []string{tenantmysql.SchemaModule().Name, appmysql.SchemaModule().Name},
		Tables:       append([]string(nil), SchemaTables...),
		SQL:          SchemaSQL,
	}
}

// InitDB initializes the channel tables and indexes owned by this package.
func InitDB(ctx context.Context, db *sql.DB) error {
	return sharedschema.Apply(ctx, db, SchemaModule())
}

// VerifySchema verifies that the channel-owned tables exist.
func VerifySchema(ctx context.Context, db *sql.DB) error {
	return sharedschema.Verify(ctx, db, sharedschema.DriverMySQL, SchemaTables)
}
