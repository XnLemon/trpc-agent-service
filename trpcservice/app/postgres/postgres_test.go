package postgres

import "testing"

func TestNewRepositoryCreatesAgentAdapter(t *testing.T) {
	if repository := NewRepository(nil); repository == nil {
		t.Fatal("agent PostgreSQL repository is nil")
	}
}
