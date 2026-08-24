package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func TestPostgreSQLEventReadersPreserveOptionalFields(t *testing.T) {
	created, updated := validTimes()
	_ = created
	modelEvent, err := scanModelEvent(scriptedRow{values: []any{
		"updated", mockTenantID, mockModelID, "active", "suspended", "previous", "current", "admin", "user", "change", "model", int64(1), int64(2), updated,
	}})
	if err != nil || modelEvent.PreviousStatus != model.StatusActive || modelEvent.PreviousDigest != "previous" {
		t.Fatalf("model optional event fields = %+v, err=%v", modelEvent, err)
	}
	backendEvent, err := scanBackendEvent(scriptedRow{values: []any{
		"updated", mockTenantID, "bp_01ARZ3NDEKTSV4RRFFQ69G5FAW", "active", "suspended", "previous", "current", "admin", "user", "change", "backend", int64(1), int64(2), updated,
	}})
	if err != nil || backendEvent.PreviousStatus != backend.StatusActive || backendEvent.PreviousDigest != "previous" {
		t.Fatalf("backend optional event fields = %+v, err=%v", backendEvent, err)
	}
	channelEvent, err := scanChannelEvent(scriptedRow{values: []any{
		"updated", mockTenantID, "cb_01ARZ3NDEKTSV4RRFFQ69G5FAW", "active", "suspended", "previous", "current", "admin", "user", "change", "channel", int64(1), int64(2), updated,
	}})
	if err != nil || channelEvent.PreviousStatus != channels.StatusActive || channelEvent.PreviousDigest != "previous" {
		t.Fatalf("channel optional event fields = %+v, err=%v", channelEvent, err)
	}
	agentEvent, err := scanAgentEvent(scriptedRow{values: []any{
		"published", mockTenantID, mockAppID, "active", "active", int64(1), int64(2), "digest", "admin", "user", "change", "agent", int64(1), int64(2), updated,
	}})
	if err != nil || agentEvent.PreviousStatus != agent.StatusActive || agentEvent.PreviousRevision == nil || *agentEvent.PreviousRevision != 1 || agentEvent.CurrentRevision == nil || *agentEvent.CurrentRevision != 2 || agentEvent.ContentDigest != "digest" {
		t.Fatalf("agent optional event fields = %+v, err=%v", agentEvent, err)
	}
	for name, scan := range map[string]func() error{
		"model":   func() error { _, err := scanModelEvent(scriptedRow{err: errors.New("row error")}); return err },
		"backend": func() error { _, err := scanBackendEvent(scriptedRow{err: errors.New("row error")}); return err },
		"channel": func() error { _, err := scanChannelEvent(scriptedRow{err: errors.New("row error")}); return err },
		"agent":   func() error { _, err := scanAgentEvent(scriptedRow{err: errors.New("row error")}); return err },
	} {
		t.Run(name+" row error", func(t *testing.T) {
			if err := scan(); err == nil {
				t.Fatal("row error was ignored")
			}
		})
	}
}

func TestPostgreSQLReadersRejectCorruptRows(t *testing.T) {
	modelCatalog, backendCatalog := mockCatalogs(t)
	modelValue := mockModel(t, modelCatalog)
	options, generation, err := encodeModelJSON(modelValue.Configuration)
	if err != nil {
		t.Fatal(err)
	}
	modelRow := []any{modelValue.TenantID, modelValue.ProfileID, modelValue.ProfileKey, modelValue.DisplayName, modelValue.Description,
		string(modelValue.Status), modelValue.SchemaVersion, modelValue.Configuration.Provider, modelValue.Configuration.Model, modelValue.Configuration.Endpoint,
		options, modelValue.Configuration.SecretRef, generation, modelValue.ContentDigest, modelValue.Version, modelValue.CreatedAt, modelValue.UpdatedAt}
	modelRow[10] = []byte("not-json")
	if _, err := scanModelProfile(modelCatalog, scriptedRow{values: modelRow}); !errors.Is(err, ErrStorage) {
		t.Fatalf("corrupt model JSON error = %v", err)
	}

	binding := mockBinding(t, mockAppID)
	protocol, err := encodeProtocol(binding.Protocol)
	if err != nil {
		t.Fatal(err)
	}
	channelRow := []any{binding.TenantID, binding.BindingID, binding.BindingKey, string(binding.Channel), binding.ProviderAccountID,
		binding.PublicRouteKeyDigest, binding.AppID, binding.SecretRef, protocol, channels.SchemaVersionV1, string(binding.Status), binding.Version,
		binding.ConfigDigest, binding.CreatedAt, binding.UpdatedAt}
	channelRow[9] = channels.SchemaVersionV1 + 1
	if _, err := scanChannelBinding(scriptedRow{values: channelRow}); !errors.Is(err, ErrStorage) {
		t.Fatalf("unsupported channel schema error = %v", err)
	}
	channelRow[9], channelRow[8] = channels.SchemaVersionV1, []byte("not-json")
	if _, err := scanChannelBinding(scriptedRow{values: channelRow}); !errors.Is(err, ErrStorage) {
		t.Fatalf("corrupt channel protocol error = %v", err)
	}

	bindingRowError := errors.New("binding row error")
	for _, test := range []struct {
		name     string
		bindings *sqlmock.Rows
		want     error
	}{
		{name: "corrupt binding JSON", bindings: sqlmock.NewRows([]string{"capability", "provider", "endpoint", "options", "secret_ref"}).AddRow("session", "inmemory", "", []byte("not-json"), ""), want: ErrStorage},
		{name: "binding row error", bindings: sqlmock.NewRows([]string{"capability", "provider", "endpoint", "options", "secret_ref"}).AddRow("session", "inmemory", "", []byte("{}"), "").RowError(0, bindingRowError), want: bindingRowError},
		{name: "missing bindings", bindings: sqlmock.NewRows([]string{"capability", "provider", "endpoint", "options", "secret_ref"}), want: ErrStorage},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			current := mockBackend(t, backendCatalog)
			mock.ExpectQuery(".*").WithArgs(current.TenantID, current.ProfileID).WillReturnRows(mockBackendRootRows(current))
			mock.ExpectQuery(".*").WithArgs(current.TenantID, current.ProfileID).WillReturnRows(test.bindings)
			_, err := loadBackendProfile(context.Background(), db, backendCatalog, current.TenantID, current.ProfileID, false)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostgreSQLRevisionAndCandidateReadersFailClosed(t *testing.T) {
	ctx := context.Background()
	app := mockApp(t, 1)
	published := mockRevision(t, app, 1, true)
	t.Run("revision tools query failure", func(t *testing.T) {
		db, mock := newSQLMock(t)
		expectAgentRevisionRow(mock, published, nil, nil)
		mock.ExpectQuery(".*").WithArgs(published.TenantID, published.AppID, published.Revision).WillReturnError(errors.New("tools unavailable"))
		if _, err := loadAgentRevision(ctx, db, published.TenantID, published.AppID, published.Revision, false); err == nil {
			t.Fatal("tools query error was ignored")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("revision corrupt configuration", func(t *testing.T) {
		db, mock := newSQLMock(t)
		expectAgentRevisionRow(mock, published, []byte("not-json"), nil)
		if _, err := loadAgentRevision(ctx, db, published.TenantID, published.AppID, published.Revision, false); !errors.Is(err, ErrStorage) {
			t.Fatalf("corrupt revision error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("revision tool row failure", func(t *testing.T) {
		db, mock := newSQLMock(t)
		expectAgentRevisionRow(mock, published, nil, nil)
		mock.ExpectQuery(".*").WithArgs(published.TenantID, published.AppID, published.Revision).WillReturnRows(sqlmock.NewRows([]string{"tool_id", "required"}).AddRow("tool", true).RowError(0, errors.New("tool row error")))
		if _, err := loadAgentRevision(ctx, db, published.TenantID, published.AppID, published.Revision, false); err == nil {
			t.Fatal("tool row error was ignored")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "candidate-edge")
	if err != nil {
		t.Fatal(err)
	}
	candidate := validCandidate(routeDigest)
	db, mock := newSQLMock(t)
	repo := NewChannelRepository(db)
	repo.candidates[candidate.CandidateToken] = candidateRecord{tenantID: mockTenantID, bindingID: "cb_01ARZ3NDEKTSV4RRFFQ69G5FAW", context: candidate}
	changed := candidate.Clone()
	changed.ConfigDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := repo.ConsumeCandidate(ctx, changed); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("mismatched candidate error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock = newSQLMock(t)
	repo = NewChannelRepository(db)
	expired := candidate.Clone()
	expired.IssuedAt = expired.IssuedAt.Add(-2 * postgresCandidateTTL)
	expired.ExpiresAt = expired.IssuedAt.Add(postgresCandidateTTL)
	repo.candidates[expired.CandidateToken] = candidateRecord{tenantID: mockTenantID, bindingID: "cb_01ARZ3NDEKTSV4RRFFQ69G5FAW", context: expired}
	if _, err := repo.ConsumeCandidate(ctx, expired); !errors.Is(err, channels.ErrCandidateUnavailable) {
		t.Fatalf("expired candidate error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func expectAgentRevisionRow(mock sqlmock.Sqlmock, value *agent.Revision, generationOverride, runtimeOverride []byte) {
	generation, runtime, _, err := encodeAgentRevisionParts(*value)
	if err != nil {
		panic(err)
	}
	if generationOverride != nil {
		generation = generationOverride
	}
	if runtimeOverride != nil {
		runtime = runtimeOverride
	}
	var digest, publishedAt any
	if value.ContentDigest != "" {
		digest = value.ContentDigest
	}
	if value.PublishedAt != nil {
		publishedAt = *value.PublishedAt
	}
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.AppID, value.Revision).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "app_id", "revision", "state", "draft_version", "agent_kind", "schema_version", "description",
		"instruction", "global_instruction", "model_profile_id", "generation_config", "runtime_policy", "content_digest", "published_at", "created_at", "updated_at",
	}).AddRow(value.TenantID, value.AppID, value.Revision, string(value.State), value.DraftVersion, string(value.Kind),
		value.SchemaVersion, value.Description, value.Instruction, value.GlobalInstruction, value.ModelProfileID, generation,
		runtime, digest, publishedAt, value.CreatedAt, value.UpdatedAt))
}
