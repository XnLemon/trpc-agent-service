package postgres

import (
	"context"
	"database/sql"

	appostgres "github.com/XnLemon/trpc-agent-service/trpcservice/app/postgres"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
	commonpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/schema/postgres"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
)

// SchemaTables lists the tables owned by this adapter.
var SchemaTables = []string{
	"runtime_session", "message_event", "reply_outbox", "runtime_event_history",
	"runtime_reply_correlation", "runtime_memory", "runtime_summary", "runtime_knowledge",
	"runtime_artifact", "runtime_audit_log", "runtime_vector_index", "runtime_object",
	"runtime_attachment",
}

// SchemaModule describes the runtime-storage PostgreSQL schema and its dependencies.
func SchemaModule() sharedschema.Module {
	return sharedschema.Module{
		Name:         "runtime/storage",
		Driver:       sharedschema.DriverPostgres,
		Dependencies: []string{commonpostgres.SchemaModule().Name, tenantpostgres.SchemaModule().Name, channelpostgres.SchemaModule().Name, appostgres.SchemaModule().Name},
		Tables:       append([]string(nil), SchemaTables...),
		SQL:          SchemaSQL,
	}
}

// InitDB initializes the runtime-storage tables and indexes owned by this package.
func InitDB(ctx context.Context, db *sql.DB) error {
	return commonpostgres.InitModule(ctx, db, SchemaModule())
}

// VerifySchema verifies that the runtime-storage tables exist.
func VerifySchema(ctx context.Context, db *sql.DB) error {
	return sharedschema.Verify(ctx, db, sharedschema.DriverPostgres, SchemaTables)
}
