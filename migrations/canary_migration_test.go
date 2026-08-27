package migrations

import (
	"io/fs"
	"strings"
	"testing"
)

func TestCanaryMigrationLocksTenantAndAppTogether(t *testing.T) {
	contents, err := fs.ReadFile(Files, "0010_agent_app_canary.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "FOR UPDATE OF app, tenant") {
		t.Fatal("canary mutation must lock both tenant and app rows while checking tenant status")
	}
}
