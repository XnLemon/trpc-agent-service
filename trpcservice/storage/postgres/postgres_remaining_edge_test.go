package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestPostgreSQLTenantReaderAndMetadataEdges(t *testing.T) {
	current := mockTenant(t)
	defaultApp := mockAppID
	defaultBackend := "bp_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	current.DefaultAgentAppID = &defaultApp
	current.DefaultBackendProfileID = &defaultBackend
	if err := current.Validate(); err != nil {
		t.Fatal(err)
	}
	decoded, err := scanTenant(scriptedRow{values: []any{
		current.TenantID, current.TenantKey, current.DisplayName, string(current.Status), nil,
		nil, nil, nil, current.BillingCurrency,
		current.AuditRetentionDays, string(current.LogMaskingLevel), current.TraceSamplingRate, defaultApp, defaultBackend,
		current.Version, current.CreatedAt, current.UpdatedAt,
	}})
	if err != nil || decoded.DefaultAgentAppID == nil || *decoded.DefaultAgentAppID != defaultApp || decoded.DefaultBackendProfileID == nil || *decoded.DefaultBackendProfileID != defaultBackend {
		t.Fatalf("tenant defaults = %+v, err=%v", decoded, err)
	}
	if _, err := scanTenantStatusEvent(scriptedRow{err: errors.New("status row failure")}); err == nil {
		t.Fatal("tenant status row error was ignored")
	}
	for name, metadata := range map[string]tenant.TransitionMetadata{
		"missing":     {ActorType: "admin", ActorID: "", Reason: "reason", CorrelationID: "correlation"},
		"long reason": {ActorType: "admin", ActorID: "user", Reason: string(make([]rune, 1001)), CorrelationID: "correlation"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateTenantMetadata(metadata); !errors.Is(err, tenant.ErrInvalid) {
				t.Fatalf("metadata error = %v", err)
			}
		})
	}
	if validTenantTransition(tenant.StatusDisabled, tenant.StatusActive) || validTenantTransition(tenant.StatusSuspended, tenant.StatusSuspended) {
		t.Fatal("terminal tenant transitions were accepted")
	}
}

func TestPostgreSQLRepositoryRemainingReaderBranches(t *testing.T) {
	ctx := context.Background()
	modelCatalog, backendCatalog := mockCatalogs(t)

	t.Run("tenant activity read failure", func(t *testing.T) {
		db, mock := newSQLMock(t)
		mock.ExpectQuery(".*").WithArgs(mockTenantID).WillReturnError(sqlmock.ErrCancelled)
		if err := assertTenantActive(ctx, db, mockTenantID); !errors.Is(err, ErrStorage) {
			t.Fatalf("tenant activity read error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("rollback requires current revision", func(t *testing.T) {
		noCurrent := mockApp(t, 1)
		noCurrent.Status = agent.StatusDraft
		noCurrent.CurrentRevision = nil
		if err := noCurrent.Validate(); err != nil {
			t.Fatal(err)
		}
		target := mockRevision(t, noCurrent, 1, true)
		db, mock := newSQLMock(t)
		repo := NewAgentRepository(db)
		mock.ExpectBegin()
		expectAgentApp(mock, noCurrent)
		expectAgentRevision(mock, target)
		mock.ExpectRollback()
		_, _, err := repo.Rollback(ctx, agent.RollbackInput{
			TenantID: noCurrent.TenantID, AppID: noCurrent.AppID, TargetRevision: target.Revision, ExpectedAppVersion: noCurrent.Version, Metadata: mockAgentMetadata(),
		})
		assertDomainFailure(t, err, agent.ErrInvalid, mock)
	})

	t.Run("agent metadata rejects incomplete and oversized values", func(t *testing.T) {
		if err := validateAgentMetadata(agent.ChangeMetadata{ActorType: "admin", ActorID: "", Reason: "reason", CorrelationID: "correlation"}); !errors.Is(err, agent.ErrInvalid) {
			t.Fatalf("incomplete agent metadata error = %v", err)
		}
		if err := validateAgentMetadata(agent.ChangeMetadata{ActorType: "admin", ActorID: "user", Reason: string(make([]rune, 1001)), CorrelationID: "correlation"}); !errors.Is(err, agent.ErrInvalid) {
			t.Fatalf("oversized agent metadata error = %v", err)
		}
	})

	t.Run("backend reader lock path", func(t *testing.T) {
		current := mockBackend(t, backendCatalog)
		db, mock := newSQLMock(t)
		expectBackendProfile(mock, current)
		stored, err := loadBackendProfile(ctx, db, backendCatalog, current.TenantID, current.ProfileID, true)
		if err != nil || stored.ProfileID != current.ProfileID {
			t.Fatalf("locked backend read = %+v, err=%v", stored, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("candidate binding no longer active", func(t *testing.T) {
		binding := mockBinding(t, mockAppID)
		binding.Status = channels.StatusSuspended
		if err := binding.Validate(); err != nil {
			t.Fatal(err)
		}
		candidate := validCandidate(binding.PublicRouteKeyDigest)
		candidate.BindingVersion = binding.Version
		candidate.ConfigDigest = binding.ConfigDigest
		db, mock := newSQLMock(t)
		repo := NewChannelRepository(db)
		repo.candidates[candidate.CandidateToken] = candidateRecord{tenantID: binding.TenantID, bindingID: binding.BindingID, context: candidate}
		mock.ExpectQuery(".*").WithArgs(binding.TenantID, binding.BindingID).WillReturnRows(mockBindingRows(binding))
		if _, err := repo.ConsumeCandidate(ctx, candidate); !errors.Is(err, channels.ErrCandidateUnavailable) {
			t.Fatalf("inactive candidate binding error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	future := time.Now().UTC().Add(time.Hour)
	if got := monotonicNow(future); got != future {
		t.Fatalf("monotonic timestamp regressed: %s", got)
	}
	if _, _, err := NewModelRepository(nil, modelCatalog).TransitionStatus(ctx, model.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("unavailable model repository error = %v", err)
	}
}

func TestPostgreSQLRepositoryDomainAndNotFoundBoundaries(t *testing.T) {
	ctx := context.Background()
	modelCatalog, backendCatalog := mockCatalogs(t)

	t.Run("constructors reject invalid creates before issuing SQL", func(t *testing.T) {
		db, mock := newSQLMock(t)
		if _, err := NewTenantRepository(db).Create(ctx, tenant.CreateInput{}); !errors.Is(err, tenant.ErrInvalid) {
			t.Fatalf("tenant create error = %v", err)
		}
		if _, err := NewAgentRepository(db).Create(ctx, agent.CreateInput{}); !errors.Is(err, agent.ErrInvalid) {
			t.Fatalf("agent create error = %v", err)
		}
		if _, _, err := NewBackendRepository(db, backendCatalog).Create(ctx, backend.CreateInput{}); !errors.Is(err, backend.ErrInvalid) {
			t.Fatalf("backend create error = %v", err)
		}
		if _, _, err := NewChannelRepository(db).Create(ctx, channels.CreateInput{}); !errors.Is(err, channels.ErrInvalid) {
			t.Fatalf("channel create error = %v", err)
		}
		if _, _, err := NewModelRepository(db, modelCatalog).Create(ctx, model.CreateInput{}); !errors.Is(err, model.ErrInvalid) {
			t.Fatalf("model create error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	backendID := mockBackend(t, backendCatalog).ProfileID
	bindingID := mockBinding(t, mockAppID).BindingID
	for name, get := range map[string]func(*sql.DB, sqlmock.Sqlmock) error{
		"tenant": func(db *sql.DB, mock sqlmock.Sqlmock) error {
			mock.ExpectQuery(".*").WithArgs(mockTenantID).WillReturnError(sql.ErrNoRows)
			_, err := NewTenantRepository(db).Get(ctx, mockTenantID)
			return err
		},
		"agent": func(db *sql.DB, mock sqlmock.Sqlmock) error {
			mock.ExpectQuery(".*").WithArgs(mockTenantID, mockAppID).WillReturnError(sql.ErrNoRows)
			_, err := NewAgentRepository(db).Get(ctx, mockTenantID, mockAppID)
			return err
		},
		"backend": func(db *sql.DB, mock sqlmock.Sqlmock) error {
			mock.ExpectQuery(".*").WithArgs(mockTenantID, backendID).WillReturnError(sql.ErrNoRows)
			_, err := NewBackendRepository(db, backendCatalog).Get(ctx, mockTenantID, backendID)
			return err
		},
		"channel": func(db *sql.DB, mock sqlmock.Sqlmock) error {
			mock.ExpectQuery(".*").WithArgs(mockTenantID, bindingID).WillReturnError(sql.ErrNoRows)
			_, err := NewChannelRepository(db).Get(ctx, mockTenantID, bindingID)
			return err
		},
		"model": func(db *sql.DB, mock sqlmock.Sqlmock) error {
			mock.ExpectQuery(".*").WithArgs(mockTenantID, mockModelID).WillReturnError(sql.ErrNoRows)
			_, err := NewModelRepository(db, modelCatalog).Get(ctx, mockTenantID, mockModelID)
			return err
		},
	} {
		t.Run(name+" not found", func(t *testing.T) {
			db, mock := newSQLMock(t)
			if err := get(db, mock); err == nil {
				t.Fatal("not-found row was accepted")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
