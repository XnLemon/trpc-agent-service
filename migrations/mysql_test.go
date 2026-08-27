package migrations

import (
	"strings"
	"testing"
)

func TestSplitMySQLStatementsRespectsQuotedSemicolonsAndComments(t *testing.T) {
	script := `-- header;
CREATE TABLE example (value VARCHAR(32) DEFAULT 'a;b');
/* block; comment */
ALTER TABLE example ADD COLUMN quoted VARCHAR(32) DEFAULT "x;y";
`
	statements := splitMySQLStatements(script)
	if len(statements) != 2 {
		t.Fatalf("statement count = %d (%#v)", len(statements), statements)
	}
	if !strings.Contains(statements[0], "CREATE TABLE") || !strings.Contains(statements[0], "'a;b'") {
		t.Fatalf("first statement = %q", statements[0])
	}
	if !strings.Contains(statements[1], "ALTER TABLE") || !strings.Contains(statements[1], `"x;y"`) {
		t.Fatalf("second statement = %q", statements[1])
	}
}

func TestMySQLMigrationSetUsesBinaryIdentityAndRecoveryMarkers(t *testing.T) {
	files, err := orderedMySQLFiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].version != 1 {
		t.Fatalf("MySQL files = %#v", files)
	}
	script := files[0].statements
	joined := strings.Join(script, "\n")
	for _, fragment := range []string{"utf8mb4_bin", "active_provider_account_id", "channel_binding_candidate_idx", "ENGINE=InnoDB"} {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("migration is missing %q", fragment)
		}
	}
	if strings.Contains(joined, "REPEATABLE READ") || strings.Contains(joined, "SECURITY DEFINER") {
		t.Fatal("MySQL migration contains an unsupported transaction/function contract")
	}
}
