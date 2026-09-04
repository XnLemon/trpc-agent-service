package migrations

import (
	"os"
	"strings"
	"testing"
)

func TestRuntimePGVectorMigrationDefinesTenantScopedLifecycle(t *testing.T) {
	contents, err := os.ReadFile("0017_runtime_pgvector_knowledge.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"CREATE EXTENSION IF NOT EXISTS vector",
		"CREATE TABLE public.runtime_pgvector_documents",
		"tenant_id           TEXT NOT NULL",
		"collection          TEXT NOT NULL",
		"index_status        TEXT NOT NULL DEFAULT 'pending'",
		"'pending','ready','failed','dead_letter','deleted'",
		"CHECK (embedding IS NULL OR vector_dims(embedding) = embedding_dimension)",
		"PRIMARY KEY (tenant_id, collection, knowledge_id, document_id, chunk_id)",
		"REVOKE ALL ON TABLE public.runtime_pgvector_documents FROM PUBLIC",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("pgvector migration is missing %q", fragment)
		}
	}
}
