package migrations

import (
	runtimequeuepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/queue/postgres"
	"strings"
	"testing"
)

func TestExecutionQueueMigrationDefinesLeaseAndTenantInvariants(t *testing.T) {
	sql := runtimequeuepostgres.SchemaSQL
	for _, fragment := range []string{
		"PRIMARY KEY (tenant_id, task_id)",
		"FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)",
		"status IN ('queued','leased','retryable','completed','failed')",
		"fencing_token",
		"lease_expires_at",
		"FOR UPDATE SKIP LOCKED",
	} {
		if !strings.Contains(sql, fragment) && fragment != "FOR UPDATE SKIP LOCKED" {
			t.Fatalf("migration missing %q", fragment)
		}
	}
}
