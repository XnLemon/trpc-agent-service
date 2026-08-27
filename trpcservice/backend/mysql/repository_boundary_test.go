package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

func TestBackendRepositoryRejectsCancelledContextsBeforeStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewRepository(nil, nil)
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, _, err := r.Create(ctx, backend.CreateInput{}); return err }},
		{"get", func() error { _, err := r.Get(ctx, "tenant", "profile"); return err }},
		{"update", func() error { _, _, err := r.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{}); return err }},
		{"transition", func() error { _, _, err := r.TransitionStatus(ctx, backend.TransitionStatusInput{}); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestReplaceBackendBindingsClearsEmptyBindingSet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectExec("DELETE FROM backend_profile_binding WHERE tenant_id = \\? AND profile_id = \\?").
		WithArgs("tenant", "profile").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := replaceBackendBindings(context.Background(), db, backend.Profile{TenantID: "tenant", ProfileID: "profile"}); err != nil {
		t.Fatalf("replace empty bindings = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
