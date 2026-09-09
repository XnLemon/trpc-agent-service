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

func TestRuntimeSessionDeletionMigrationCascadesDependentFacts(t *testing.T) {
	contents, err := os.ReadFile("0004_runtime_session_delete_cascade.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"DROP CONSTRAINT message_event_tenant_id_session_id_fkey",
		"REFERENCES public.runtime_session(tenant_id, session_id) ON DELETE CASCADE",
		"DROP CONSTRAINT reply_outbox_tenant_id_event_id_fkey",
		"REFERENCES public.message_event(tenant_id, event_id) ON DELETE CASCADE",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration missing %q", fragment)
		}
	}
}

func TestRuntimeEventHistoryMigrationIsTenantScopedAndCascades(t *testing.T) {
	contents, err := os.ReadFile("0005_runtime_event_history.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"PRIMARY KEY (tenant_id, session_id, event_id)",
		"UNIQUE (tenant_id, session_id, history_seq)",
		"REFERENCES public.runtime_session(tenant_id, session_id) ON DELETE CASCADE",
		"payload     JSONB NOT NULL",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration missing %q", fragment)
		}
	}
	if strings.Contains(sql, "GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_event_history TO tenant_app_writer") {
		t.Fatal("runtime event history grants mutation to the runtime role")
	}
	if !strings.Contains(sql, "GRANT UPDATE (event_id) ON public.runtime_event_history TO tenant_app_writer") {
		t.Fatal("runtime event history is missing the narrow idempotent update grant")
	}
}

func TestRuntimeKnowledgeMigrationDefinesDurableTenantScopedDocuments(t *testing.T) {
	contents, err := os.ReadFile("0018_runtime_knowledge.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"CREATE TABLE public.runtime_knowledge_document",
		"REFERENCES public.tenant(tenant_id) ON DELETE CASCADE",
		"metadata        JSONB NOT NULL",
		"embedding       JSONB NOT NULL",
		"PRIMARY KEY (tenant_id, document_id)",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("knowledge migration missing %q", fragment)
		}
	}
}

func TestRuntimeKnowledgeAppScopeMigrationFailsClosedAndUsesCompositeIdentity(t *testing.T) {
	contents, err := os.ReadFile("0023_runtime_knowledge_app_scope.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"SET app_id = metadata ->> '_trpc_app_id'",
		"runtime_knowledge_document contains rows without app identity",
		"PRIMARY KEY (tenant_id, app_id, document_id)",
		"metadata ->> '_trpc_app_id' = app_id",
		"REFERENCES public.agent_app (tenant_id, app_id)",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("knowledge app migration missing %q", fragment)
		}
	}
	if strings.Contains(sql, "embedding") || strings.Contains(sql, "secret_ref") {
		t.Fatal("knowledge app migration must not add embedding or secret columns")
	}
}

func TestRuntimeKnowledgeVersionMigrationDefinesTenantAppManifests(t *testing.T) {
	contents, err := os.ReadFile("0021_runtime_knowledge_version.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"CREATE TABLE public.runtime_knowledge_version",
		"PRIMARY KEY (tenant_id, app_id, version)",
		"UNIQUE (tenant_id, app_id, content_digest)",
		"REFERENCES public.agent_app(tenant_id, app_id)",
		"GRANT SELECT, INSERT ON public.runtime_knowledge_version TO tenant_app_writer",
		"GRANT REFERENCES ON public.agent_app TO tenant_app_writer",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("knowledge version migration missing %q", fragment)
		}
	}
	if strings.Contains(sql, "embedding       ") || strings.Contains(sql, "secret_ref") {
		t.Fatal("knowledge version migration must not persist embeddings or secrets")
	}
}

func TestRuntimeToolInvocationMigrationDefinesDurableUnknownState(t *testing.T) {
	contents, err := os.ReadFile("0019_runtime_tool_invocation.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{
		"CREATE TABLE public.runtime_tool_invocation",
		"PRIMARY KEY (tenant_id, invocation_id)",
		"UNIQUE (tenant_id, event_id, tool_call_id)",
		"status IN ('prepared','dispatching','accepted','succeeded','failed','denied','unknown','manual')",
		"args_sha256",
		"REVOKE ALL ON TABLE public.runtime_tool_invocation FROM PUBLIC",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("tool invocation migration missing %q", fragment)
		}
	}
	if strings.Contains(sql, "arguments TEXT") || strings.Contains(sql, "result JSONB") {
		t.Fatal("tool invocation migration must not persist raw arguments or results")
	}
}

func TestRuntimeCapabilityMigrationContainsOnlyPlatformStorage(t *testing.T) {
	contents, err := os.ReadFile("0012_runtime_capabilities.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(contents)
	for _, fragment := range []string{"runtime_summary", "runtime_audit_log", "runtime_attachment_content"} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("migration missing %q", fragment)
		}
	}
	for _, obsolete := range []string{"runtime_memory", "runtime_knowledge", "runtime_artifact", "runtime_vector_index", "runtime_object"} {
		if strings.Contains(sql, obsolete) {
			t.Fatalf("migration retains obsolete Agent storage %q", obsolete)
		}
	}
}
