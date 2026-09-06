package migrations

import (
	appmysql "github.com/XnLemon/trpc-agent-service/trpcservice/app/mysql"
	apppostgres "github.com/XnLemon/trpc-agent-service/trpcservice/app/postgres"
	auditpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/audit/postgres"
	backendmysql "github.com/XnLemon/trpc-agent-service/trpcservice/backend/mysql"
	backendpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/backend/postgres"
	channelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/channels/mysql"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	modelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/model/mysql"
	modelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/model/postgres"
	runtimebudgetpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget/postgres"
	runtimequeuepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/queue/postgres"
	runtimestoragepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	sharedschema "github.com/XnLemon/trpc-agent-service/trpcservice/schema"
	commonpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/schema/postgres"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
)

func allPostgresSchemaModules() []sharedschema.Module {
	return []sharedschema.Module{
		commonpostgres.SchemaModule(), tenantpostgres.SchemaModule(), modelpostgres.SchemaModule(),
		apppostgres.SchemaModule(), backendpostgres.SchemaModule(), channelpostgres.SchemaModule(),
		runtimestoragepostgres.SchemaModule(), runtimequeuepostgres.SchemaModule(), auditpostgres.SchemaModule(), runtimebudgetpostgres.SchemaModule(),
	}
}

func allMySQLSchemaModules() []sharedschema.Module {
	return []sharedschema.Module{
		tenantmysql.SchemaModule(), modelmysql.SchemaModule(), appmysql.SchemaModule(),
		backendmysql.SchemaModule(), channelmysql.SchemaModule(),
	}
}
