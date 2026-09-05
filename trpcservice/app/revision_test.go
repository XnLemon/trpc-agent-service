package app

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

const validAppID = "app_01J1K9ZQTVE4PAWF1TSB2WMHNP"

func float64Pointer(value float64) *float64 { return &value }
func intPointer(value int) *int             { return &value }

func validDraftConfiguration() DraftConfiguration {
	return DraftConfiguration{
		Description:       " Example revision ",
		Instruction:       "Answer the user accurately.",
		GlobalInstruction: "Follow tenant policy.",
		ModelProfileID:    " model-primary ",
		Generation: GenerationConfig{
			Temperature:     float64Pointer(0.2),
			TopP:            float64Pointer(0.9),
			MaxOutputTokens: intPointer(2048),
		},
		Tools: []ToolAuthorization{
			{ToolID: " search ", Required: true},
			{ToolID: "calculator"},
		},
	}
}

func validRevisionInput() CreateRevisionInput {
	return CreateRevisionInput{
		TenantID:      validTenantID,
		AppID:         validAppID,
		Revision:      1,
		Configuration: validDraftConfiguration(),
	}
}

func TestNewRevisionNormalizesAndMaterializesDefaults(t *testing.T) {
	revision, err := NewRevision(validRevisionInput())
	if err != nil {
		t.Fatal(err)
	}
	if revision.Kind != KindLLM || revision.SchemaVersion != SchemaVersionV1 || revision.State != RevisionStateDraft || revision.DraftVersion != 1 {
		t.Fatalf("unexpected revision identity: %+v", revision)
	}
	if revision.Description != "Example revision" || revision.ModelProfileID != "model-primary" {
		t.Fatalf("configuration was not normalized: %+v", revision)
	}
	if revision.Runtime != DefaultRuntimePolicy() {
		t.Fatalf("runtime defaults were not materialized: %+v", revision.Runtime)
	}
	if len(revision.Tools) != 2 || revision.Tools[0].ToolID != "calculator" || revision.Tools[1].ToolID != "search" {
		t.Fatalf("tool allowlist was not normalized deterministically: %+v", revision.Tools)
	}
	if revision.ContentDigest != "" || revision.PublishedAt != nil || revision.CreatedAt.IsZero() || !revision.CreatedAt.Equal(revision.UpdatedAt) {
		t.Fatalf("unexpected draft bookkeeping: %+v", revision)
	}
	if err := revision.Validate(); err != nil {
		t.Fatalf("new revision must validate: %v", err)
	}
}

func TestNewRevisionRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*CreateRevisionInput)
	}{
		{name: "tenant id", mutate: func(input *CreateRevisionInput) { input.TenantID = "bad" }},
		{name: "app id", mutate: func(input *CreateRevisionInput) { input.AppID = "bad" }},
		{name: "revision", mutate: func(input *CreateRevisionInput) { input.Revision = 0 }},
		{name: "kind", mutate: func(input *CreateRevisionInput) { input.Kind = Kind("graph") }},
		{name: "schema", mutate: func(input *CreateRevisionInput) { input.SchemaVersion = 2 }},
		{name: "blank instruction", mutate: func(input *CreateRevisionInput) { input.Configuration.Instruction = " " }},
		{name: "long instruction", mutate: func(input *CreateRevisionInput) {
			input.Configuration.Instruction = strings.Repeat("界", maxInstructionRunes+1)
		}},
		{name: "long global instruction", mutate: func(input *CreateRevisionInput) {
			input.Configuration.GlobalInstruction = strings.Repeat("界", maxInstructionRunes+1)
		}},
		{name: "blank model", mutate: func(input *CreateRevisionInput) { input.Configuration.ModelProfileID = " " }},
		{name: "long model", mutate: func(input *CreateRevisionInput) {
			input.Configuration.ModelProfileID = strings.Repeat("m", maxReferenceRunes+1)
		}},
		{name: "temperature", mutate: func(input *CreateRevisionInput) { input.Configuration.Generation.Temperature = float64Pointer(2.1) }},
		{name: "temperature nan", mutate: func(input *CreateRevisionInput) {
			input.Configuration.Generation.Temperature = float64Pointer(math.NaN())
		}},
		{name: "top p", mutate: func(input *CreateRevisionInput) { input.Configuration.Generation.TopP = float64Pointer(0) }},
		{name: "tokens", mutate: func(input *CreateRevisionInput) { input.Configuration.Generation.MaxOutputTokens = intPointer(0) }},
		{name: "runtime LLM calls", mutate: func(input *CreateRevisionInput) {
			input.Configuration.Runtime = DefaultRuntimePolicy()
			input.Configuration.Runtime.MaxLLMCalls = 0
		}},
		{name: "runtime tool calls", mutate: func(input *CreateRevisionInput) {
			input.Configuration.Runtime = DefaultRuntimePolicy()
			input.Configuration.Runtime.MaxToolCalls = -1
		}},
		{name: "runtime parallel", mutate: func(input *CreateRevisionInput) {
			input.Configuration.Runtime = DefaultRuntimePolicy()
			input.Configuration.Runtime.MaxParallelTools = 2
		}},
		{name: "runtime timeout", mutate: func(input *CreateRevisionInput) {
			input.Configuration.Runtime = DefaultRuntimePolicy()
			input.Configuration.Runtime.ExecutionTimeoutSeconds = 3601
		}},
		{name: "blank tool", mutate: func(input *CreateRevisionInput) { input.Configuration.Tools = []ToolAuthorization{{ToolID: " "}} }},
		{name: "long tool", mutate: func(input *CreateRevisionInput) {
			input.Configuration.Tools = []ToolAuthorization{{ToolID: strings.Repeat("t", maxReferenceRunes+1)}}
		}},
		{name: "duplicate tool", mutate: func(input *CreateRevisionInput) {
			input.Configuration.Tools = []ToolAuthorization{{ToolID: "search"}, {ToolID: " search "}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validRevisionInput()
			test.mutate(&input)
			if _, err := NewRevision(input); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}
}

func TestRevisionCloneAndConfigurationAreDeepCopies(t *testing.T) {
	revision, err := NewRevision(validRevisionInput())
	if err != nil {
		t.Fatal(err)
	}
	clone := revision.Clone()
	*clone.Generation.Temperature = 1.7
	clone.Tools[0].ToolID = "mutated"
	if *revision.Generation.Temperature != 0.2 || revision.Tools[0].ToolID != "calculator" {
		t.Fatal("revision clone leaked pointer or slice state")
	}
	configuration := revision.Configuration()
	*configuration.Generation.MaxOutputTokens = 1
	configuration.Tools[0].Required = !configuration.Tools[0].Required
	if *revision.Generation.MaxOutputTokens != 2048 || revision.Tools[0].Required == configuration.Tools[0].Required {
		t.Fatal("configuration accessor leaked mutable state")
	}
}

func TestContentDigestIsDeterministicAndSensitiveToBehavior(t *testing.T) {
	first, err := NewRevision(validRevisionInput())
	if err != nil {
		t.Fatal(err)
	}
	firstDigest := assertEquivalentExecutionContentDigest(t, first)
	assertBehavioralChangeAltersDigest(t, first, firstDigest)
	assertEmptyToolAllowlistsHaveEquivalentDigests(t)
	assertSignedZeroTemperaturesHaveEquivalentDigests(t, first)
}

func assertEquivalentExecutionContentDigest(t *testing.T, first *Revision) string {
	t.Helper()
	input := validRevisionInput()
	input.Revision = 99
	input.Configuration.Tools[0], input.Configuration.Tools[1] = input.Configuration.Tools[1], input.Configuration.Tools[0]
	second, err := NewRevision(input)
	if err != nil {
		t.Fatal(err)
	}
	firstDigest, err := first.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := second.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if len(firstDigest) != 64 || firstDigest != secondDigest {
		t.Fatalf("equivalent execution content must have one digest: %q %q", firstDigest, secondDigest)
	}
	return firstDigest
}

func assertBehavioralChangeAltersDigest(t *testing.T, first *Revision, firstDigest string) {
	t.Helper()
	changed := first.Clone()
	changed.Instruction = "Use a different behavior."
	changedDigest, err := changed.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == firstDigest {
		t.Fatal("behavioral configuration change must alter digest")
	}
}

func assertEmptyToolAllowlistsHaveEquivalentDigests(t *testing.T) {
	t.Helper()
	nilToolsInput := validRevisionInput()
	nilToolsInput.Configuration.Tools = nil
	nilTools, err := NewRevision(nilToolsInput)
	if err != nil {
		t.Fatal(err)
	}
	emptyToolsInput := validRevisionInput()
	emptyToolsInput.Configuration.Tools = []ToolAuthorization{}
	emptyTools, err := NewRevision(emptyToolsInput)
	if err != nil {
		t.Fatal(err)
	}
	if nilTools.Tools == nil || emptyTools.Tools == nil {
		t.Fatal("empty tool allowlists must use the canonical non-nil representation")
	}
	nilDigest, err := nilTools.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	emptyDigest, err := emptyTools.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if nilDigest != emptyDigest {
		t.Fatalf("equivalent empty tool allowlists must have one digest: %q %q", nilDigest, emptyDigest)
	}
	assertDirectEmptyToolRepresentationsHaveEquivalentDigests(t, emptyTools)
}

func assertDirectEmptyToolRepresentationsHaveEquivalentDigests(t *testing.T, emptyTools *Revision) {
	t.Helper()
	directNil := emptyTools.Clone()
	directNil.Tools = nil
	directEmpty := emptyTools.Clone()
	directEmpty.Tools = []ToolAuthorization{}
	directNilDigest, err := directNil.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	directEmptyDigest, err := directEmpty.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if directNilDigest != directEmptyDigest {
		t.Fatalf("digest must normalize external empty tool representations: %q %q", directNilDigest, directEmptyDigest)
	}
}

func assertSignedZeroTemperaturesHaveEquivalentDigests(t *testing.T, first *Revision) {
	t.Helper()
	positiveZero := first.Clone()
	positiveZero.Generation.Temperature = float64Pointer(0)
	negativeZero := first.Clone()
	negativeZero.Generation.Temperature = float64Pointer(math.Copysign(0, -1))
	positiveZeroDigest, err := positiveZero.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	negativeZeroDigest, err := negativeZero.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if positiveZeroDigest != negativeZeroDigest {
		t.Fatalf("equivalent signed zero temperatures must have one digest: %q %q", positiveZeroDigest, negativeZeroDigest)
	}
}

func TestPublishFreezesContentAndDetectsMutation(t *testing.T) {
	draft, err := NewRevision(validRevisionInput())
	if err != nil {
		t.Fatal(err)
	}
	publishedAt := draft.CreatedAt.Add(time.Second)
	published, err := draft.Publish(publishedAt)
	if err != nil {
		t.Fatal(err)
	}
	if draft.State != RevisionStateDraft || draft.ContentDigest != "" || draft.PublishedAt != nil {
		t.Fatal("publishing mutated the source draft")
	}
	if published.State != RevisionStatePublished || published.PublishedAt == nil || !published.PublishedAt.Equal(publishedAt) || len(published.ContentDigest) != 64 {
		t.Fatalf("unexpected publication state: %+v", published)
	}
	if err := published.Validate(); err != nil {
		t.Fatalf("published revision must validate: %v", err)
	}
	if _, err := published.Publish(publishedAt.Add(time.Second)); !errors.Is(err, ErrImmutableRevision) {
		t.Fatalf("expected immutable publication rejection, got %v", err)
	}
	mutated := published.Clone()
	mutated.Tools[0].Required = !mutated.Tools[0].Required
	if !errors.Is(mutated.Validate(), ErrInvalid) {
		t.Fatal("published content mutation must invalidate digest")
	}
}

func TestRevisionValidateRejectsMalformedPublicationState(t *testing.T) {
	draft, err := NewRevision(validRevisionInput())
	if err != nil {
		t.Fatal(err)
	}
	now := draft.CreatedAt.Add(time.Second)
	tests := []struct {
		name   string
		mutate func(*Revision)
	}{
		{name: "draft version", mutate: func(revision *Revision) { revision.DraftVersion = 0 }},
		{name: "unnormalized model", mutate: func(revision *Revision) { revision.ModelProfileID = " padded " }},
		{name: "unsorted tools", mutate: func(revision *Revision) { revision.Tools[0], revision.Tools[1] = revision.Tools[1], revision.Tools[0] }},
		{name: "unknown state", mutate: func(revision *Revision) { revision.State = RevisionState("unknown") }},
		{name: "draft digest", mutate: func(revision *Revision) { revision.ContentDigest = strings.Repeat("0", 64) }},
		{name: "draft published time", mutate: func(revision *Revision) { revision.PublishedAt = &now }},
		{name: "zero timestamp", mutate: func(revision *Revision) { revision.UpdatedAt = time.Time{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := draft.Clone()
			test.mutate(&invalid)
			if !errors.Is(invalid.Validate(), ErrInvalid) {
				t.Fatalf("expected invalid revision rejection: %+v", invalid)
			}
		})
	}

	if _, err := draft.Publish(time.Time{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected zero publication time rejection, got %v", err)
	}
	if _, err := draft.Publish(draft.CreatedAt.Add(-time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected publication ordering rejection, got %v", err)
	}
	edited := draft.Clone()
	edited.UpdatedAt = draft.CreatedAt.Add(2 * time.Second)
	if _, err := edited.Publish(draft.CreatedAt.Add(time.Second)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected publication before the latest draft update to be rejected, got %v", err)
	}
	if _, err := edited.Publish(edited.UpdatedAt); err != nil {
		t.Fatalf("expected publication at the latest draft update boundary to succeed, got %v", err)
	}
	corrupted := draft.Clone()
	corrupted.ContentDigest = strings.Repeat("0", 64)
	if _, err := corrupted.Publish(corrupted.UpdatedAt); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected malformed draft receiver to be rejected before publication, got %v", err)
	}
	published, err := draft.Publish(now)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*Revision)
	}{
		{name: "publication after update", mutate: func(revision *Revision) {
			later := revision.PublishedAt.Add(time.Second)
			revision.PublishedAt = &later
		}},
		{name: "update after publication", mutate: func(revision *Revision) { revision.UpdatedAt = revision.UpdatedAt.Add(time.Second) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := published.Clone()
			test.mutate(&invalid)
			if !errors.Is(invalid.Validate(), ErrInvalid) {
				t.Fatalf("expected mismatched publication bookkeeping rejection: %+v", invalid)
			}
		})
	}
}
