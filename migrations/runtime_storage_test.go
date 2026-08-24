package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestRuntimeStorageMigrationDefinesTenantScopedInvariants(t *testing.T) {
	contents, err := os.ReadFile("0003_runtime_storage.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"PRIMARY KEY (tenant_id, session_id)",
		"UNIQUE (tenant_id, session_id, event_seq)",
		"UNIQUE (tenant_id, binding_id, external_message_id)",
		"FOREIGN KEY (tenant_id, session_id)",
		"CHECK (status IN ('pending', 'sending', 'sent', 'retryable', 'dead_letter'))",
		"fencing_token",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration missing %q", fragment)
		}
	}
}
