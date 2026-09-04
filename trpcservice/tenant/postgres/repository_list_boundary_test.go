package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

//nolint:gocyclo // Exercises every list failure and pagination branch.
func TestTenantRepositoryListFailureAndPaginationBranches(t *testing.T) {
	value, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "list-boundary", DisplayName: "List Boundary", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		setup   func(sqlmock.Sqlmock)
		cursor  string
		wantErr error
	}{
		{name: "invalid cursor", cursor: "bad"},
		{name: "negative cursor", cursor: "-1"},
		{name: "query error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID).WillReturnError(errors.New("query"))
		}, wantErr: ErrStorage},
		{name: "scan error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID).
				WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow(value.TenantID)).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "iteration error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID).
				WillReturnRows(testTenantRows(value).RowError(0, errors.New("iteration"))).RowsWillBeClosed()
		}, wantErr: ErrStorage},
		{name: "close error", setup: func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID).
				WillReturnRows(testTenantRows(value).CloseError(errors.New("close"))).RowsWillBeClosed()
		}, wantErr: ErrStorage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			if tc.setup != nil {
				tc.setup(mock)
			}
			_, _, callErr := NewRepository(db).List(ctx, []string{value.TenantID}, "", "", tc.cursor, 1)
			if tc.wantErr == nil {
				if callErr == nil {
					t.Fatal("invalid cursor was accepted")
				}
			} else if !errors.Is(callErr, tc.wantErr) {
				t.Fatalf("error = %v, want %v", callErr, tc.wantErr)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}

	second, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "list-boundary-secondary", DisplayName: "List Boundary Secondary", Status: tenant.StatusSuspended,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID).
		WillReturnRows(testTenantRows(value, second)).RowsWillBeClosed()
	items, next, err := NewRepository(db).List(ctx, []string{value.TenantID}, "", "", "", 1)
	if err != nil || len(items) != 1 || next != "1" {
		t.Fatalf("paginated tenant list = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(`SELECT .* FROM public\.tenant`).WithArgs(value.TenantID).
		WillReturnRows(testTenantRows(value)).RowsWillBeClosed()
	items, next, err = NewRepository(db).List(ctx, []string{value.TenantID}, "", "", "1", 1)
	if err != nil || items == nil || len(items) != 0 || next != "" {
		t.Fatalf("past-end tenant list = items=%+v next=%q err=%v", items, next, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewRepository(nil).List(ctx, []string{value.TenantID}, "", "", "", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil-storage List error = %v", err)
	}
}
