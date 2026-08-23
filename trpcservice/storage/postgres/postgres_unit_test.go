package postgres

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgreSQLCodecRoundTripsAndRejectsMalformedJSON(t *testing.T) {
	temperature := 0.25
	maxOutputTokens := 128
	configuration := model.Configuration{
		Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"},
		Generation: model.GenerationConfig{Temperature: &temperature, MaxOutputTokens: &maxOutputTokens},
	}
	options, generation, err := encodeModelJSON(configuration)
	if err != nil {
		t.Fatal(err)
	}
	var decoded model.Configuration
	decoded.Provider, decoded.Model = configuration.Provider, configuration.Model
	if err := decodeModelJSON(options, generation, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Options["mode"] != "safe" || decoded.Generation.MaxOutputTokens == nil || *decoded.Generation.MaxOutputTokens != maxOutputTokens {
		t.Fatalf("model JSON round trip = %+v", decoded)
	}

	bindings, err := encodeBackendBindings([]backend.CapabilityBinding{{
		Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "primary"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var decodedBindings []backendBindingJSON
	if err := decodeJSON(bindings, &decodedBindings); err != nil || len(decodedBindings) != 1 || decodedBindings[0].Options["namespace"] != "primary" {
		t.Fatalf("backend JSON round trip = %+v, err=%v", decodedBindings, err)
	}

	revision := agent.Revision{
		Generation: agent.GenerationConfig{Temperature: &temperature, MaxOutputTokens: &maxOutputTokens},
		Runtime:    agent.DefaultRuntimePolicy(), Tools: []agent.ToolAuthorization{{ToolID: "tool", Required: true}},
	}
	encodedGeneration, encodedRuntime, encodedTools, err := encodeAgentRevisionParts(revision)
	if err != nil {
		t.Fatal(err)
	}
	var decodedRevision agent.Revision
	if err := decodeAgentRevisionParts(encodedGeneration, encodedRuntime, &decodedRevision); err != nil {
		t.Fatal(err)
	}
	if decodedRevision.Generation.MaxOutputTokens == nil || decodedRevision.Runtime.MaxLLMCalls != revision.Runtime.MaxLLMCalls {
		t.Fatalf("agent JSON round trip = %+v", decodedRevision)
	}
	var decodedTools []agent.ToolAuthorization
	if err := decodeJSON(encodedTools, &decodedTools); err != nil || len(decodedTools) != 1 || decodedTools[0].ToolID != "tool" {
		t.Fatalf("agent tools JSON = %+v, err=%v", decodedTools, err)
	}

	protocol := channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{WebhookPath: "/telegram"}}
	encodedProtocol, err := encodeProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	var decodedProtocol channels.ProtocolConfiguration
	if err := decodeProtocol(encodedProtocol, &decodedProtocol); err != nil || decodedProtocol.Telegram == nil || decodedProtocol.Telegram.WebhookPath != "/telegram" {
		t.Fatalf("protocol JSON round trip = %+v, err=%v", decodedProtocol, err)
	}

	for _, malformed := range [][]byte{[]byte(`{"unknown": true}`), []byte(`{} {}`), []byte(`not-json`)} {
		var value map[string]string
		if err := decodeJSON(malformed, &value); !errors.Is(err, ErrStorage) {
			t.Fatalf("decodeJSON(%q) error = %v", malformed, err)
		}
	}
	var empty map[string]string
	if err := decodeJSON(nil, &empty); err != nil || empty == nil {
		t.Fatalf("empty JSON decode = %#v, err=%v", empty, err)
	}
}

func TestPostgreSQLDBHelpers(t *testing.T) {
	if got := normalizeDSN(" PostgreSQL+PSYCOPG://db.example/test "); got != "postgresql://db.example/test" {
		t.Fatalf("normalized psycopg DSN = %q", got)
	}
	if got := normalizeDSN("postgres://db.example/test"); got != "postgres://db.example/test" {
		t.Fatalf("normalized native DSN = %q", got)
	}
	if _, err := Open(context.Background(), "", Options{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("empty DSN error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(canceled, "postgres://unused", Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Open error = %v", err)
	}
	if err := Ping(context.Background(), nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil Ping error = %v", err)
	}
	if _, err := begin(context.Background(), nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil begin error = %v", err)
	}
	rollback(nil)
	if err := commit(context.Background(), nil); err == nil {
		t.Fatal("nil commit unexpectedly succeeded")
	}

	if got := mapDBError(context.Background(), sql.ErrNoRows, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid); !errors.Is(got, tenant.ErrNotFound) {
		t.Fatalf("not-found mapping = %v", got)
	}
	for code, want := range map[string]error{
		"23505": tenant.ErrDuplicateKey,
		"23503": tenant.ErrInvalid,
		"23514": tenant.ErrInvalid,
		"22P02": tenant.ErrInvalid,
		"22001": tenant.ErrInvalid,
		"40001": tenant.ErrConflict,
		"40P01": tenant.ErrConflict,
	} {
		got := mapDBError(context.Background(), &pgconn.PgError{Code: code}, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid)
		if !errors.Is(got, want) {
			t.Errorf("%s mapping = %v, want %v", code, got, want)
		}
	}
	if got := mapDBError(context.Background(), errors.New("driver failure"), tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid); !errors.Is(got, ErrStorage) {
		t.Fatalf("unknown mapping = %v", got)
	}
	canceled, cancel = context.WithCancel(context.Background())
	cancel()
	if got := mapDBError(canceled, errors.New("driver failure"), tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid); !errors.Is(got, context.Canceled) {
		t.Fatalf("context mapping = %v", got)
	}

	if value := nullableInt(sql.NullInt64{Int64: 4, Valid: true}); value == nil || *value != 4 {
		t.Fatalf("nullable int = %v", value)
	}
	if nullableInt(sql.NullInt64{}) != nil || nullableString(sql.NullString{}) != nil {
		t.Fatal("invalid nullable values became non-nil")
	}
}

func TestPostgreSQLRepositoriesRejectCanceledAndUnavailablePools(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldForbidden, SecretRefPolicy: model.FieldOptional})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden})
	if err != nil {
		t.Fatal(err)
	}
	metadata := tenant.TransitionMetadata{ActorType: "test", ActorID: "unit", Reason: "test", CorrelationID: "unit"}

	tenants := NewTenantRepository(nil)
	validTenant := tenant.CreateInput{TenantKey: "unit", DisplayName: "Unit", Status: tenant.StatusActive, AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1}
	if _, err := tenants.Create(context.Background(), validTenant); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant Create error = %v", err)
	}
	if _, err := tenants.Get(canceled, "tenant"); !errors.Is(err, context.Canceled) {
		t.Fatalf("tenant canceled Get error = %v", err)
	}
	if _, err := tenants.Get(context.Background(), "tenant"); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant Get error = %v", err)
	}
	if _, err := tenants.UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{TenantID: "tenant", DisplayName: "Unit", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1}); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant Update error = %v", err)
	}
	if _, _, err := tenants.TransitionStatus(context.Background(), tenant.TransitionStatusInput{TenantID: "tenant", NextStatus: tenant.StatusSuspended, Metadata: metadata}); !errors.Is(err, ErrStorage) {
		t.Fatalf("tenant Transition error = %v", err)
	}

	apps := NewAgentRepository(nil)
	if _, err := apps.Create(context.Background(), agent.CreateInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent Create error = %v", err)
	}
	if _, err := apps.Get(context.Background(), "tenant", "app"); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent Get error = %v", err)
	}
	if _, err := apps.UpdateMetadata(context.Background(), agent.UpdateMetadataInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent metadata error = %v", err)
	}
	if _, err := apps.CreateDraft(context.Background(), agent.CreateDraftInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent draft error = %v", err)
	}
	if _, err := apps.UpdateDraft(context.Background(), agent.UpdateDraftInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent draft update error = %v", err)
	}
	if _, err := apps.GetRevision(context.Background(), "tenant", "app", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent revision error = %v", err)
	}
	if _, _, _, err := apps.Publish(context.Background(), agent.PublishInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent publish error = %v", err)
	}
	if _, _, err := apps.Rollback(context.Background(), agent.RollbackInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent rollback error = %v", err)
	}
	if _, _, err := apps.TransitionStatus(context.Background(), agent.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent transition error = %v", err)
	}

	models := NewModelRepository(nil, modelCatalog)
	if _, _, err := models.Create(context.Background(), model.CreateInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("model Create error = %v", err)
	}
	if _, err := models.Get(context.Background(), "tenant", "profile"); !errors.Is(err, ErrStorage) {
		t.Fatalf("model Get error = %v", err)
	}
	if _, _, err := models.UpdateConfiguration(context.Background(), model.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("model Update error = %v", err)
	}
	if _, _, err := models.TransitionStatus(context.Background(), model.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("model Transition error = %v", err)
	}

	backends := NewBackendRepository(nil, backendCatalog)
	if _, _, err := backends.Create(context.Background(), backend.CreateInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("backend Create error = %v", err)
	}
	if _, err := backends.Get(context.Background(), "tenant", "profile"); !errors.Is(err, ErrStorage) {
		t.Fatalf("backend Get error = %v", err)
	}
	if _, _, err := backends.UpdateConfiguration(context.Background(), backend.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("backend Update error = %v", err)
	}
	if _, _, err := backends.TransitionStatus(context.Background(), backend.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("backend Transition error = %v", err)
	}

	channelRepo := NewChannelRepository(nil)
	if _, _, err := channelRepo.Create(context.Background(), channels.CreateInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel Create error = %v", err)
	}
	if _, err := channelRepo.Get(context.Background(), "tenant", "binding"); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel Get error = %v", err)
	}
	if _, _, err := channelRepo.UpdateConfiguration(context.Background(), channels.UpdateConfigurationInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel Update error = %v", err)
	}
	if _, _, err := channelRepo.TransitionStatus(context.Background(), channels.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel Transition error = %v", err)
	}
	if _, err := channelRepo.LookupCandidates(context.Background(), channels.ChannelTelegram, strings.Repeat("a", 64)); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel Lookup error = %v", err)
	}
	if _, err := channelRepo.ConsumeCandidate(context.Background(), channels.CandidateBindingContext{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("channel Consume error = %v", err)
	}
}

type scriptedRow struct {
	values []any
	err    error
}

func (row scriptedRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(dest) != len(row.values) {
		return errors.New("scan destination count mismatch")
	}
	for index, value := range row.values {
		if err := assignScanValue(dest[index], value); err != nil {
			return err
		}
	}
	return nil
}

func assignScanValue(destination, value any) error {
	switch target := destination.(type) {
	case *sql.NullInt64:
		if value == nil {
			target.Valid = false
			return nil
		}
		target.Int64, target.Valid = value.(int64), true
	case *sql.NullString:
		if value == nil {
			target.Valid = false
			return nil
		}
		target.String, target.Valid = value.(string), true
	case *sql.NullTime:
		if value == nil {
			target.Valid = false
			return nil
		}
		target.Time, target.Valid = value.(time.Time), true
	case *[]byte:
		if value == nil {
			*target = nil
			return nil
		}
		*target = append([]byte(nil), value.([]byte)...)
	default:
		left := reflect.ValueOf(destination)
		if left.Kind() != reflect.Pointer || left.IsNil() {
			return errors.New("scan destination is not a pointer")
		}
		right := reflect.ValueOf(value)
		if !right.IsValid() || !right.Type().AssignableTo(left.Elem().Type()) {
			return errors.New("scan value type mismatch")
		}
		left.Elem().Set(right)
	}
	return nil
}

func validTimes() (time.Time, time.Time) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return created, created.Add(time.Minute)
}

func TestPostgreSQLScanHelpers(t *testing.T) {
	created, updated := validTimes()
	rootID := "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	appID := "app_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	profileID := "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	bindingID := "cb_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	digest := strings.Repeat("a", 64)

	value, err := scanTenant(scriptedRow{values: []any{rootID, "tenant", "Tenant", "active", int64(10), int64(5), int64(100), int64(200), "USD", 30, "basic", float64(1), nil, nil, int64(1), created, updated}})
	if err != nil || value.TenantID != rootID || value.BillingCurrency != "USD" {
		t.Fatalf("tenant scan = %+v, err=%v", value, err)
	}
	if _, err := scanTenant(scriptedRow{err: errors.New("row failure")}); err == nil {
		t.Fatal("tenant scan failure was ignored")
	}
	statusEvent, err := scanTenantStatusEvent(scriptedRow{values: []any{rootID, "active", "suspended", "admin", "user", "pause", int64(1), int64(2), updated}})
	if err != nil || statusEvent.NextStatus != tenant.StatusSuspended {
		t.Fatalf("tenant event scan = %+v, err=%v", statusEvent, err)
	}

	catalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional, EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional, Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}}})
	if err != nil {
		t.Fatal(err)
	}
	expectedModel, err := model.NewProfile(model.CreateInput{
		TenantID: rootID, ProfileKey: "primary", DisplayName: "Model", Status: model.StatusActive,
		Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	modelValue, err := scanModelProfile(catalog, scriptedRow{values: []any{rootID, profileID, "primary", "Model", "", "active", 1, "public", "chat", "", []byte(`{"mode":"safe"}`), "", []byte(`{}`), expectedModel.ContentDigest, int64(1), created, updated}})
	if err != nil || modelValue.Configuration.Options["mode"] != "safe" {
		t.Fatalf("model scan = %+v, err=%v", modelValue, err)
	}
	modelEvent, err := scanModelEvent(scriptedRow{values: []any{"created", rootID, profileID, nil, "active", nil, digest, "admin", "user", "create", "corr", int64(0), int64(1), updated}})
	if err != nil || modelEvent.EventType != model.EventCreated {
		t.Fatalf("model event scan = %+v, err=%v", modelEvent, err)
	}

	backendEvent, err := scanBackendEvent(scriptedRow{values: []any{"created", rootID, profileID, nil, "active", nil, digest, "admin", "user", "create", "corr", int64(0), int64(1), updated}})
	if err != nil || backendEvent.EventType != backend.EventCreated {
		t.Fatalf("backend event scan = %+v, err=%v", backendEvent, err)
	}
	protocol := []byte(`{"telegram":{}}`)
	expectedBinding, err := channels.NewBinding(channels.CreateInput{
		TenantID: rootID, BindingKey: "primary", Channel: channels.ChannelTelegram,
		ProviderAccountID: "account", PublicRouteKeyDigest: digest, AppID: appID,
		SecretRef: "secret://binding", Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{}}, Status: channels.StatusDraft,
	})
	if err != nil {
		t.Fatal(err)
	}
	bindingValue, err := scanChannelBinding(scriptedRow{values: []any{rootID, bindingID, "primary", "telegram", "account", digest, appID, "secret://binding", protocol, 1, "draft", int64(1), expectedBinding.ConfigDigest, created, updated}})
	if err != nil || bindingValue.BindingID != bindingID || bindingValue.Protocol.Telegram == nil {
		t.Fatalf("channel scan = %+v, err=%v", bindingValue, err)
	}
	channelEvent, err := scanChannelEvent(scriptedRow{values: []any{"created", rootID, bindingID, nil, "draft", nil, digest, "admin", "user", "create", "corr", int64(0), int64(1), updated}})
	if err != nil || channelEvent.EventType != channels.EventCreated {
		t.Fatalf("channel event scan = %+v, err=%v", channelEvent, err)
	}
	agentEvent, err := scanAgentEvent(scriptedRow{values: []any{"published", rootID, appID, nil, "active", nil, int64(1), digest, "admin", "user", "publish", "corr", int64(1), int64(2), updated}})
	if err != nil || agentEvent.EventType != agent.ChangePublished {
		t.Fatalf("agent event scan = %+v, err=%v", agentEvent, err)
	}
}
