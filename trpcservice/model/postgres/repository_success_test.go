package postgres

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func TestModelRepositoryGetDecodesStoredProfile(t *testing.T) {
	catalog, err := model.NewProviderCatalog(model.ProviderSpec{
		Provider: "public", Models: []string{"chat"}, EndpointPolicy: model.FieldOptional,
		EndpointSchemes: []string{"https"}, EndpointHosts: []string{"example.test"}, SecretRefPolicy: model.FieldOptional,
		Options: map[string]model.OptionSpec{"mode": {Kind: model.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := model.NewProfile(model.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", ProfileKey: "primary", DisplayName: "Primary", Status: model.StatusActive,
		SchemaVersion: model.SchemaVersionV1, Configuration: model.Configuration{Provider: "public", Model: "chat", Options: map[string]string{"mode": "safe"}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	options, generation, err := encodeModelJSON(profile.Configuration)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(".*").WithArgs(profile.TenantID, profile.ProfileID).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "profile_id", "profile_key", "display_name", "description", "status", "schema_version", "provider", "model", "endpoint",
		"options", "secret_ref", "generation", "content_digest", "version", "created_at", "updated_at",
	}).AddRow(
		profile.TenantID, profile.ProfileID, profile.ProfileKey, profile.DisplayName, profile.Description, string(profile.Status), profile.SchemaVersion,
		profile.Configuration.Provider, profile.Configuration.Model, profile.Configuration.Endpoint, options, profile.Configuration.SecretRef, generation,
		profile.ContentDigest, profile.Version, profile.CreatedAt, profile.UpdatedAt,
	))

	stored, err := NewRepository(db, catalog).Get(context.Background(), profile.TenantID, profile.ProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ProfileID != profile.ProfileID || stored.Configuration.Options["mode"] != "safe" {
		t.Fatalf("stored model profile = %+v", stored)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
