package backend

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
)

const testTenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestNewProfileNormalizesConfiguration(t *testing.T) {
	catalog := newTestCatalog(t)
	inputOptions := map[string]string{
		"database":  " agent ",
		"pool_size": "010",
		"read_only": "TRUE",
		"ssl_mode":  " REQUIRE ",
	}
	profile, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: " Primary-Data ",
		DisplayName: " Primary data ", Description: " Shared stores ",
		Bindings: []CapabilityBinding{
			{Capability: CapabilityMemory, Provider: "inmemory", Options: map[string]string{"namespace": " durable "}},
			{
				Capability: CapabilitySession, Provider: "POSTGRES",
				Endpoint: " POSTGRES://DB.EXAMPLE.COM:5432 ", Options: inputOptions,
				SecretRef: " secret://tenant/database ",
			},
		},
	}, catalog)
	if err != nil {
		t.Fatalf("NewProfile() error = %v", err)
	}
	if profile.ProfileKey != "primary-data" || profile.DisplayName != "Primary data" || profile.Description != "Shared stores" {
		t.Fatalf("Profile metadata was not normalized: %#v", profile)
	}
	if profile.Status != StatusActive || profile.SchemaVersion != 1 || profile.Version != 1 {
		t.Fatalf("Profile defaults = status %q schema %d version %d", profile.Status, profile.SchemaVersion, profile.Version)
	}
	if matched := regexp.MustCompile(`^bp_[0-7][0-9A-HJKMNP-TV-Z]{25}$`).MatchString(profile.ProfileID); !matched {
		t.Fatalf("ProfileID = %q", profile.ProfileID)
	}
	if profile.CreatedAt.IsZero() || !profile.CreatedAt.Equal(profile.UpdatedAt) || profile.CreatedAt.Location() != time.UTC {
		t.Fatalf("timestamps are not initialized in UTC: created=%v updated=%v", profile.CreatedAt, profile.UpdatedAt)
	}
	if len(profile.Bindings) != 2 || profile.Bindings[0].Capability != CapabilitySession || profile.Bindings[1].Capability != CapabilityMemory {
		t.Fatalf("Bindings were not canonically sorted: %#v", profile.Bindings)
	}
	session := profile.Bindings[0]
	if session.Provider != "postgres" || session.Endpoint != "postgres://db.example.com:5432" || session.SecretRef != "secret://tenant/database" {
		t.Fatalf("Session binding was not normalized: %#v", session)
	}
	wantOptions := map[string]string{
		"database": "agent", "pool_size": "10", "read_only": "true", "ssl_mode": "require",
	}
	if !stringMapsEqual(session.Options, wantOptions) {
		t.Fatalf("Session options = %#v, want %#v", session.Options, wantOptions)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(profile.ContentDigest) {
		t.Fatalf("ContentDigest = %q", profile.ContentDigest)
	}
	if err := profile.Validate(catalog); err != nil {
		t.Fatalf("Profile.Validate() error = %v", err)
	}

	inputOptions["database"] = "mutated"
	if profile.Bindings[0].Options["database"] != "agent" {
		t.Fatal("NewProfile retained the caller's option map")
	}
}

func TestProfileLifecycleAndSessionInvariant(t *testing.T) {
	catalog := newTestCatalog(t)
	memoryOnly := []CapabilityBinding{{Capability: CapabilityMemory, Provider: "inmemory"}}

	if _, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "active", DisplayName: "Active", Bindings: memoryOnly,
	}, catalog); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "session") {
		t.Fatalf("active Profile without Session error = %v", err)
	}
	suspended, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "suspended", DisplayName: "Suspended",
		Status: StatusSuspended, Bindings: memoryOnly,
	}, catalog)
	if err != nil {
		t.Fatalf("suspended Profile without Session error = %v", err)
	}
	if suspended.CanAcceptExecution() {
		t.Fatal("suspended Profile accepted execution")
	}
	if !suspended.CanTransitionTo(StatusActive) || !suspended.CanTransitionTo(StatusDisabled) || suspended.CanTransitionTo(StatusSuspended) {
		t.Fatalf("unexpected suspended transitions")
	}
	active := newTestProfile(t, catalog)
	if !active.CanAcceptExecution() || !active.CanTransitionTo(StatusSuspended) || !active.CanTransitionTo(StatusDisabled) || active.CanTransitionTo(StatusActive) {
		t.Fatalf("unexpected active lifecycle behavior")
	}
	active.Status = StatusDisabled
	if active.CanAcceptExecution() || active.CanTransitionTo(StatusActive) || active.CanTransitionTo(StatusSuspended) {
		t.Fatalf("disabled Profile is not terminal")
	}
	if err := active.Validate(catalog); err != nil {
		t.Fatalf("disabled retained Profile should validate: %v", err)
	}
	if _, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "disabled", DisplayName: "Disabled",
		Status: StatusDisabled, Bindings: sessionBinding(),
	}, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("creating disabled Profile error = %v", err)
	}
}

func TestProfileDigestIsCanonicalAndSemantic(t *testing.T) {
	catalog := newTestCatalog(t)
	first, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "first", DisplayName: "First",
		Bindings: append([]CapabilityBinding{
			{Capability: CapabilityMemory, Provider: "inmemory", Options: map[string]string{"namespace": "memory"}},
		}, sessionBinding()...),
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "second", DisplayName: "Different display",
		Bindings: []CapabilityBinding{
			sessionBinding()[0],
			{Capability: CapabilityMemory, Provider: "inmemory", Options: map[string]string{"namespace": "memory"}},
		},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if first.ContentDigest != second.ContentDigest {
		t.Fatalf("equivalent configurations have different digests: %s != %s", first.ContentDigest, second.ContentDigest)
	}
	changed := sessionBinding()
	changed[0].SecretRef = "secret://tenant/database-next"
	third, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "third", DisplayName: "First", Bindings: changed,
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if first.ContentDigest == third.ContentDigest {
		t.Fatal("changing SecretRef did not change the content digest")
	}
}

func TestProfileCloneAndValidateDetectMutation(t *testing.T) {
	catalog := newTestCatalog(t)
	profile := newTestProfile(t, catalog)
	clone := profile.Clone()
	clone.Bindings[0].Options["database"] = "other"
	if profile.Bindings[0].Options["database"] != "agent" {
		t.Fatal("Profile.Clone leaked the options map")
	}

	mutated := profile.Clone()
	mutated.Bindings[0].Options["database"] = "other"
	if err := mutated.Validate(catalog); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("mutated Profile validation error = %v", err)
	}
	mutated = profile.Clone()
	mutated.Bindings[0], mutated.Bindings[1] = mutated.Bindings[1], mutated.Bindings[0]
	if err := mutated.Validate(catalog); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "normalized") {
		t.Fatalf("unsorted Profile validation error = %v", err)
	}
}

func TestNewProfileBoundaryValidation(t *testing.T) {
	catalog := newTestCatalog(t)
	valid := CreateInput{
		TenantID: testTenantID, ProfileKey: "primary", DisplayName: "Primary", Bindings: sessionBinding(),
	}
	tests := []struct {
		name   string
		mutate func(*CreateInput)
	}{
		{name: "tenant ID", mutate: func(input *CreateInput) { input.TenantID = "t_bad" }},
		{name: "profile key", mutate: func(input *CreateInput) { input.ProfileKey = "1bad" }},
		{name: "display name", mutate: func(input *CreateInput) { input.DisplayName = " " }},
		{name: "description", mutate: func(input *CreateInput) { input.Description = strings.Repeat("x", 2001) }},
		{name: "schema", mutate: func(input *CreateInput) { input.SchemaVersion = 2 }},
		{name: "empty bindings", mutate: func(input *CreateInput) { input.Status = StatusSuspended; input.Bindings = nil }},
		{name: "unknown status", mutate: func(input *CreateInput) { input.Status = "unknown" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := valid
			input.Bindings = cloneBindings(valid.Bindings)
			test.mutate(&input)
			if _, err := NewProfile(input, catalog); !errors.Is(err, ErrInvalid) {
				t.Fatalf("NewProfile() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestProfileValidateRejectsCorruptRoot(t *testing.T) {
	catalog := newTestCatalog(t)
	profile := newTestProfile(t, catalog)
	tests := []struct {
		name   string
		mutate func(*Profile)
	}{
		{name: "profile ID", mutate: func(profile *Profile) { profile.ProfileID = "bp_bad" }},
		{name: "key normalization", mutate: func(profile *Profile) { profile.ProfileKey = "Primary" }},
		{name: "metadata normalization", mutate: func(profile *Profile) { profile.DisplayName += " " }},
		{name: "status", mutate: func(profile *Profile) { profile.Status = "unknown" }},
		{name: "version", mutate: func(profile *Profile) { profile.Version = 0 }},
		{name: "created time", mutate: func(profile *Profile) { profile.CreatedAt = time.Time{} }},
		{name: "time order", mutate: func(profile *Profile) { profile.UpdatedAt = profile.CreatedAt.Add(-time.Second) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			corrupt := profile.Clone()
			test.mutate(&corrupt)
			if err := corrupt.Validate(catalog); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestProviderCatalogRejectsInvalidSchemas(t *testing.T) {
	defaultValue := "10"
	minimum, maximum := int64(1), int64(5)
	valid := ProviderSpec{
		Provider: "postgres", Capabilities: []Capability{CapabilitySession},
		EndpointPolicy: FieldRequired, EndpointSchemes: []string{"postgres"},
		SecretRefPolicy: FieldRequired,
	}
	tests := []struct {
		name  string
		specs []ProviderSpec
	}{
		{name: "empty catalog"},
		{name: "provider name", specs: []ProviderSpec{{Provider: "Postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden}}},
		{name: "empty capabilities", specs: []ProviderSpec{{Provider: "postgres", EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden}}},
		{name: "unknown capability", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{"unknown"}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden}}},
		{name: "duplicate capability", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession, CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden}}},
		{name: "field policy", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, SecretRefPolicy: FieldForbidden}}},
		{name: "forbidden schemes", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, EndpointSchemes: []string{"postgres"}, SecretRefPolicy: FieldForbidden}}},
		{name: "missing schemes", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldRequired, SecretRefPolicy: FieldForbidden}}},
		{name: "sensitive option", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"password": {Kind: OptionString}}}}},
		{name: "required default", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"pool": {Kind: OptionInteger, Required: true, DefaultValue: &defaultValue}}}}},
		{name: "enum values", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"mode": {Kind: OptionEnum}}}}},
		{name: "integer bounds", specs: []ProviderSpec{{Provider: "postgres", Capabilities: []Capability{CapabilitySession}, EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden, Options: map[string]OptionSpec{"pool": {Kind: OptionInteger, MinInteger: &maximum, MaxInteger: &minimum}}}}},
		{name: "duplicate registration", specs: []ProviderSpec{valid, valid}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewProviderCatalog(test.specs...); !errors.Is(err, ErrInvalid) {
				t.Fatalf("NewProviderCatalog() error = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestProviderCatalogRejectsInvalidBindingsWithoutLeakingValues(t *testing.T) {
	catalog := newTestCatalog(t)
	secretValue := "do-not-leak-password"
	valid := sessionBinding()[0]
	tests := []struct {
		name   string
		mutate func(*CapabilityBinding)
	}{
		{name: "unknown capability", mutate: func(binding *CapabilityBinding) { binding.Capability = "unknown" }},
		{name: "unknown provider", mutate: func(binding *CapabilityBinding) { binding.Provider = "mysql" }},
		{name: "endpoint required", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "" }},
		{name: "endpoint scheme", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "https://db.example.com" }},
		{name: "endpoint user info", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://user:password@db.example.com" }},
		{name: "endpoint query", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://db.example.com?token=" + secretValue }},
		{name: "endpoint fragment", mutate: func(binding *CapabilityBinding) { binding.Endpoint = "postgres://db.example.com#secret" }},
		{name: "secret required", mutate: func(binding *CapabilityBinding) { binding.SecretRef = "" }},
		{name: "secret grammar", mutate: func(binding *CapabilityBinding) { binding.SecretRef = "secret ref with spaces" }},
		{name: "unknown option", mutate: func(binding *CapabilityBinding) { binding.Options["unknown"] = secretValue }},
		{name: "sensitive option", mutate: func(binding *CapabilityBinding) { binding.Options["api_key"] = secretValue }},
		{name: "required option", mutate: func(binding *CapabilityBinding) { delete(binding.Options, "database") }},
		{name: "integer", mutate: func(binding *CapabilityBinding) { binding.Options["pool_size"] = "many" }},
		{name: "integer maximum", mutate: func(binding *CapabilityBinding) { binding.Options["pool_size"] = "101" }},
		{name: "boolean", mutate: func(binding *CapabilityBinding) { binding.Options["read_only"] = "sometimes" }},
		{name: "enum", mutate: func(binding *CapabilityBinding) { binding.Options["ssl_mode"] = "unsafe" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binding := valid.Clone()
			test.mutate(&binding)
			_, err := catalog.NormalizeBindings([]CapabilityBinding{binding})
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("NormalizeBindings() error = %v, want ErrInvalid", err)
			}
			if strings.Contains(err.Error(), secretValue) || strings.Contains(err.Error(), "user:password") {
				t.Fatalf("error leaked configuration value: %v", err)
			}
		})
	}

	duplicate := []CapabilityBinding{valid.Clone(), valid.Clone()}
	if _, err := catalog.NormalizeBindings(duplicate); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate capability error = %v", err)
	}
	forbidden := CapabilityBinding{Capability: CapabilityMemory, Provider: "inmemory", Endpoint: "https://example.com"}
	if _, err := catalog.NormalizeBindings([]CapabilityBinding{forbidden}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forbidden endpoint error = %v", err)
	}
	forbidden = CapabilityBinding{Capability: CapabilityMemory, Provider: "inmemory", SecretRef: "secret://memory"}
	if _, err := catalog.NormalizeBindings([]CapabilityBinding{forbidden}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("forbidden SecretRef error = %v", err)
	}
}

func TestProviderCatalogDefensivelyCopiesSpecs(t *testing.T) {
	defaultPool := "10"
	allowed := []string{"disable", "require"}
	options := map[string]OptionSpec{
		"database":  {Kind: OptionString, Required: true},
		"pool_size": {Kind: OptionInteger, DefaultValue: &defaultPool},
		"ssl_mode":  {Kind: OptionEnum, DefaultValue: stringPointer("require"), AllowedValues: allowed},
	}
	spec := ProviderSpec{
		Provider: "postgres", Capabilities: []Capability{CapabilitySession},
		EndpointPolicy: FieldRequired, EndpointSchemes: []string{"postgres"},
		SecretRefPolicy: FieldRequired, Options: options,
	}
	catalog, err := NewProviderCatalog(spec)
	if err != nil {
		t.Fatal(err)
	}
	defaultPool = "999"
	allowed[1] = "mutated"
	delete(options, "database")
	spec.EndpointSchemes[0] = "https"

	binding := CapabilityBinding{
		Capability: CapabilitySession, Provider: "postgres", Endpoint: "postgres://db.example.com",
		Options: map[string]string{"database": "agent"}, SecretRef: "secret://db",
	}
	normalized, err := catalog.NormalizeBindings([]CapabilityBinding{binding})
	if err != nil {
		t.Fatalf("NormalizeBindings() after caller mutation error = %v", err)
	}
	if normalized[0].Options["pool_size"] != "10" || normalized[0].Options["ssl_mode"] != "require" {
		t.Fatalf("catalog retained caller-owned schema data: %#v", normalized[0].Options)
	}
}

func newTestCatalog(t *testing.T) *ProviderCatalog {
	t.Helper()
	minimumPool, maximumPool := int64(1), int64(100)
	catalog, err := NewProviderCatalog(
		ProviderSpec{
			Provider: "inmemory",
			Capabilities: []Capability{
				CapabilitySession, CapabilityMemory, CapabilityKnowledge, CapabilityArtifact, CapabilityAudit,
			},
			EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden,
			Options: map[string]OptionSpec{"namespace": {Kind: OptionString}},
		},
		ProviderSpec{
			Provider: "postgres", Capabilities: []Capability{CapabilitySession, CapabilityMemory, CapabilityAudit},
			EndpointPolicy: FieldRequired, EndpointSchemes: []string{"postgres"}, SecretRefPolicy: FieldRequired,
			Options: map[string]OptionSpec{
				"database":  {Kind: OptionString, Required: true},
				"pool_size": {Kind: OptionInteger, DefaultValue: stringPointer("10"), MinInteger: &minimumPool, MaxInteger: &maximumPool},
				"read_only": {Kind: OptionBoolean, DefaultValue: stringPointer("false")},
				"ssl_mode":  {Kind: OptionEnum, DefaultValue: stringPointer("require"), AllowedValues: []string{"disable", "require", "verify-full"}},
			},
		},
		ProviderSpec{
			Provider: "qdrant", Capabilities: []Capability{CapabilityKnowledge},
			EndpointPolicy: FieldRequired, EndpointSchemes: []string{"https"}, SecretRefPolicy: FieldOptional,
			Options: map[string]OptionSpec{"collection": {Kind: OptionString, Required: true}},
		},
	)
	if err != nil {
		t.Fatalf("NewProviderCatalog() error = %v", err)
	}
	return catalog
}

func sessionBinding() []CapabilityBinding {
	return []CapabilityBinding{{
		Capability: CapabilitySession, Provider: "postgres", Endpoint: "postgres://db.example.com:5432",
		Options: map[string]string{"database": "agent"}, SecretRef: "secret://tenant/database",
	}}
}

func newTestProfile(t *testing.T, catalog *ProviderCatalog) *Profile {
	t.Helper()
	profile, err := NewProfile(CreateInput{
		TenantID: testTenantID, ProfileKey: "primary", DisplayName: "Primary",
		Bindings: append(sessionBinding(), CapabilityBinding{
			Capability: CapabilityMemory, Provider: "inmemory", Options: map[string]string{"namespace": "memory"},
		}),
	}, catalog)
	if err != nil {
		t.Fatalf("NewProfile() error = %v", err)
	}
	return profile
}

func stringPointer(value string) *string { return &value }
