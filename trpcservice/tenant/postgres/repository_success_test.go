package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestTenantRepositoryWritesCompleteReadback(t *testing.T) {
	ctx := context.Background()
	created, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "repository-success", DisplayName: "Repository Success", Status: tenant.StatusActive,
		AuditRetentionDays: 90, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("update configuration", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		updated := created.Clone()
		updated.DisplayName = "Updated Repository"
		updated.Version++
		updated.UpdatedAt = updated.UpdatedAt.Add(time.Second)

		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"next_version"}).AddRow(updated.Version))
		mock.ExpectQuery(".*").WillReturnRows(testTenantRows(&updated))
		mock.ExpectCommit()

		result, err := NewRepository(db).UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{
			TenantID: created.TenantID, ExpectedVersion: created.Version, DisplayName: updated.DisplayName,
			AuditRetentionDays: updated.AuditRetentionDays, LogMaskingLevel: updated.LogMaskingLevel, TraceSamplingRate: updated.TraceSamplingRate,
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Version != updated.Version || result.DisplayName != updated.DisplayName {
			t.Fatalf("updated tenant = %+v", result)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("transition status", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		updated := created.Clone()
		updated.Status = tenant.StatusSuspended
		updated.Version++
		updated.UpdatedAt = updated.UpdatedAt.Add(time.Second)

		mock.ExpectBegin()
		mock.ExpectQuery(".*").WillReturnRows(testTenantRows(created))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(7)))
		mock.ExpectQuery(".*").WillReturnRows(testTenantRows(&updated))
		mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "previous_status", "next_status", "actor_type", "actor_id", "reason", "previous_version", "next_version", "occurred_at",
		}).AddRow(created.TenantID, string(created.Status), string(updated.Status), "test", "tenant", "coverage", created.Version, updated.Version, updated.UpdatedAt))
		mock.ExpectCommit()

		result, event, err := NewRepository(db).TransitionStatus(ctx, tenant.TransitionStatusInput{
			TenantID: created.TenantID, ExpectedVersion: created.Version, NextStatus: updated.Status,
			Metadata: tenant.TransitionMetadata{ActorType: "test", ActorID: "tenant", Reason: "coverage", CorrelationID: "tenant-success"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != updated.Status || event.NextStatus != updated.Status || event.NextVersion != updated.Version {
			t.Fatalf("transition result = tenant=%+v event=%+v", result, event)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func testTenantRows(value *tenant.Tenant) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"tenant_id", "tenant_key", "display_name", "status", "rate_limit_rpm", "max_concurrent_executions", "monthly_token_budget",
		"monthly_spend_limit_minor", "billing_currency", "audit_retention_days", "log_masking_level", "trace_sampling_rate", "default_agent_app_id",
		"default_backend_profile_id", "version", "created_at", "updated_at",
	}).AddRow(
		value.TenantID, value.TenantKey, value.DisplayName, string(value.Status), nil, nil, nil, nil, nil,
		value.AuditRetentionDays, string(value.LogMaskingLevel), value.TraceSamplingRate, nil, nil, value.Version, value.CreatedAt, value.UpdatedAt,
	)
}
