package postgres

import (
	"context"
	"database/sql"

	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
	commonpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/schema/postgres"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
)

// SchemaModule declares the budget tables and their control-plane
// dependencies.
func SchemaModule() sharedschema.Module {
	return sharedschema.Module{
		Name: "runtime-budget-postgres", Driver: sharedschema.DriverPostgres,
		Dependencies: []string{commonpostgres.SchemaModule().Name, tenantpostgres.SchemaModule().Name},
		Tables:       []string{"runtime_budget_ledger", "runtime_budget_reservation"}, SQL: SchemaSQL,
	}
}

// Init initializes the budget schema directly for package-level integrations.
func Init(ctx context.Context, db *sql.DB) error {
	return commonpostgres.InitModule(ctx, db, SchemaModule())
}
