package model

import (
	"errors"
	"math"
	"testing"
	"time"
)

const (
	modelTestTenantOne = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestNewProfileNormalizesAndDefensivelyCopies(t *testing.T) {
	catalog := modelTestCatalog(t)
	temperature := 0.2
	maxTokens := 128
	options := map[string]string{"mode": " SAFE "}
	profile, err := NewProfile(CreateInput{
		TenantID: modelTestTenantOne, ProfileKey: " Primary ", DisplayName: " Primary model ",
		Description: " Shared deterministic model ", Configuration: Configuration{
			Provider: "FAKE", Model: "DETERMINISTIC", Options: options,
			Generation: GenerationConfig{Temperature: &temperature, MaxOutputTokens: &maxTokens},
		},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if profile.ProfileKey != "primary" || profile.Configuration.Provider != "fake" || profile.Configuration.Model != "deterministic" || profile.Configuration.Options["mode"] != "safe" {
		t.Fatalf("profile was not normalized: %+v", profile)
	}
	if profile.Status != StatusActive || profile.SchemaVersion != SchemaVersionV1 || profile.Version != 1 {
		t.Fatalf("profile defaults = status %q schema %d version %d", profile.Status, profile.SchemaVersion, profile.Version)
	}
	if profile.CreatedAt.IsZero() || !profile.CreatedAt.Equal(profile.UpdatedAt) || profile.CreatedAt.Location() != time.UTC {
		t.Fatalf("timestamps are not initialized in UTC: created=%v updated=%v", profile.CreatedAt, profile.UpdatedAt)
	}
	if err := profile.Validate(catalog); err != nil {
		t.Fatal(err)
	}

	options["mode"] = "fast"
	temperature = 1.5
	maxTokens = 999
	if profile.Configuration.Options["mode"] != "safe" || *profile.Configuration.Generation.Temperature != 0.2 || *profile.Configuration.Generation.MaxOutputTokens != 128 {
		t.Fatal("NewProfile retained mutable caller configuration")
	}
	clone := profile.Clone()
	clone.Configuration.Options["mode"] = "fast"
	*clone.Configuration.Generation.Temperature = 1.1
	if profile.Configuration.Options["mode"] != "safe" || *profile.Configuration.Generation.Temperature != 0.2 {
		t.Fatal("Profile.Clone leaked nested configuration mutation")
	}
}

func TestProfileSchemaRejectsUnknownAndCredentialBearingConfiguration(t *testing.T) {
	catalog := modelTestCatalog(t)
	tests := []struct {
		name  string
		input Configuration
	}{
		{name: "unknown provider", input: Configuration{Provider: "unknown", Model: "chat"}},
		{name: "unknown model", input: Configuration{Provider: "public", Model: "unknown"}},
		{name: "unknown option", input: Configuration{Provider: "fake", Model: "deterministic", Options: map[string]string{"unknown": "value"}}},
		{name: "userinfo endpoint", input: Configuration{Provider: "public", Model: "chat", Endpoint: "https://user:password@example.test/v1"}},
		{name: "query endpoint", input: Configuration{Provider: "public", Model: "chat", Endpoint: "https://example.test/v1?api_key=password"}},
		{name: "fragment endpoint", input: Configuration{Provider: "public", Model: "chat", Endpoint: "https://example.test/v1#fragment"}},
		{name: "wrong endpoint scheme", input: Configuration{Provider: "public", Model: "chat", Endpoint: "http://example.test/v1"}},
		{name: "malformed endpoint host", input: Configuration{Provider: "public", Model: "chat", Endpoint: "https://example..test/v1"}},
		{name: "invalid endpoint port", input: Configuration{Provider: "public", Model: "chat", Endpoint: "https://example.test:0/v1"}},
		{name: "invalid generation", input: Configuration{Provider: "fake", Model: "deterministic", Generation: GenerationConfig{Temperature: float64Pointer(math.NaN())}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewProfile(CreateInput{TenantID: modelTestTenantOne, ProfileKey: test.name, DisplayName: "Model", Configuration: test.input}, catalog); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}
}

func TestProviderCatalogRejectsSensitiveOptionSchemas(t *testing.T) {
	_, err := NewProviderCatalog(ProviderSpec{
		Provider: "unsafe", Models: []string{"chat"}, EndpointPolicy: FieldForbidden,
		SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"api_key": {Kind: OptionString}},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("sensitive option schema error = %v", err)
	}
}

func TestProfileStateTransitionsAndSchemaPolicies(t *testing.T) {
	catalog := modelTestCatalog(t)
	if _, err := NewProfile(CreateInput{
		TenantID: modelTestTenantOne, ProfileKey: "required", DisplayName: "Required",
		Configuration: Configuration{Provider: "secured", Model: "chat"},
	}, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing required secret error = %v", err)
	}
	if _, err := NewProfile(CreateInput{
		TenantID: modelTestTenantOne, ProfileKey: "forbidden", DisplayName: "Forbidden",
		Configuration: Configuration{Provider: "fake", Model: "deterministic", SecretRef: "secret://tenant/model"},
	}, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forbidden secret error = %v", err)
	}
	profile, err := NewProfile(CreateInput{
		TenantID: modelTestTenantOne, ProfileKey: "lifecycle", DisplayName: "Lifecycle",
		Configuration: Configuration{Provider: "fake", Model: "deterministic"},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !profile.CanTransitionTo(StatusSuspended) || profile.CanTransitionTo(StatusActive) || profile.CanTransitionTo(StatusDisabled) == false {
		t.Fatal("unexpected active lifecycle transitions")
	}
	profile.Status = StatusSuspended
	if !profile.CanTransitionTo(StatusActive) || !profile.CanTransitionTo(StatusDisabled) {
		t.Fatal("unexpected suspended lifecycle transitions")
	}
	profile.Status = StatusDisabled
	if profile.CanAcceptExecution() || profile.CanTransitionTo(StatusActive) {
		t.Fatal("disabled Profile remained executable or resumable")
	}
}

func modelTestCatalog(t *testing.T) *ProviderCatalog {
	t.Helper()
	defaultMode := "safe"
	catalog, err := NewProviderCatalog(
		ProviderSpec{
			Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: FieldForbidden,
			SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{
				"mode": {Kind: OptionEnum, DefaultValue: &defaultMode, AllowedValues: []string{"fast", "safe"}},
			},
		},
		ProviderSpec{Provider: "public", Models: []string{"chat"}, EndpointPolicy: FieldOptional, EndpointSchemes: []string{"https"}, SecretRefPolicy: FieldOptional},
		ProviderSpec{Provider: "secured", Models: []string{"chat"}, EndpointPolicy: FieldRequired, EndpointSchemes: []string{"https"}, SecretRefPolicy: FieldRequired},
	)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func float64Pointer(value float64) *float64 { return &value }
