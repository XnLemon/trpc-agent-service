package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// RevisionState distinguishes mutable draft content from immutable published
// content.
type RevisionState string

const (
	// RevisionStateDraft identifies a mutable revision.
	RevisionStateDraft RevisionState = "draft"
	// RevisionStatePublished identifies immutable executable content.
	RevisionStatePublished RevisionState = "published"
)

// Kind identifies the tRPC-Agent-Go Agent configuration schema.
type Kind string

const (
	// KindLLM is the only executable kind supported by schema version 1.
	KindLLM Kind = "llm"
)

const (
	// SchemaVersionV1 is the initial LLMAgent configuration schema.
	SchemaVersionV1     = 1
	maxInstructionRunes = 65536
	maxReferenceRunes   = 256
)

// GenerationConfig is the provider-neutral subset materialized by the first
// Agent App schema. Nil fields preserve provider defaults.
type GenerationConfig struct {
	Temperature     *float64 `json:"temperature,omitempty"`
	TopP            *float64 `json:"top_p,omitempty"`
	MaxOutputTokens *int     `json:"max_output_tokens,omitempty"`
}

// RuntimePolicy contains bounded execution controls captured by a published
// revision. It does not carry contexts, timers, or runtime clients.
type RuntimePolicy struct {
	MaxLLMCalls             int  `json:"max_llm_calls"`
	MaxToolCalls            int  `json:"max_tool_calls"`
	EnableParallelTools     bool `json:"enable_parallel_tools"`
	MaxParallelTools        int  `json:"max_parallel_tools"`
	ExecutionTimeoutSeconds int  `json:"execution_timeout_seconds"`
}

// DefaultRuntimePolicy returns the materialized schema-v1 execution defaults.
func DefaultRuntimePolicy() RuntimePolicy {
	return RuntimePolicy{
		MaxLLMCalls:             16,
		MaxToolCalls:            64,
		MaxParallelTools:        1,
		ExecutionTimeoutSeconds: 300,
	}
}

// ToolAuthorization is one deny-by-default tool allowlist entry.
type ToolAuthorization struct {
	ToolID   string `json:"tool_id"`
	Required bool   `json:"required"`
}

// DraftConfiguration is the complete executable content of one revision.
type DraftConfiguration struct {
	Description       string
	Instruction       string
	GlobalInstruction string
	ModelProfileID    string
	Generation        GenerationConfig
	Runtime           RuntimePolicy
	Tools             []ToolAuthorization
}

// Revision is one tenant-scoped version of an Agent App definition.
// Published revisions are immutable and content-addressed.
type Revision struct {
	TenantID          string
	AppID             string
	Revision          int64
	State             RevisionState
	DraftVersion      int64
	Kind              Kind
	SchemaVersion     int
	Description       string
	Instruction       string
	GlobalInstruction string
	ModelProfileID    string
	Generation        GenerationConfig
	Runtime           RuntimePolicy
	Tools             []ToolAuthorization
	ContentDigest     string
	PublishedAt       *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// CreateRevisionInput contains trusted identity and caller-selected content for
// a new draft. A Repository allocates the revision number inside the App scope.
type CreateRevisionInput struct {
	TenantID      string
	AppID         string
	Revision      int64
	Kind          Kind
	SchemaVersion int
	Configuration DraftConfiguration
}

// NewRevision validates and constructs a new draft revision.
func NewRevision(input CreateRevisionInput) (*Revision, error) {
	if err := validateTenantID(input.TenantID); err != nil {
		return nil, err
	}
	if err := validateAppID(input.AppID); err != nil {
		return nil, err
	}
	if input.Revision < 1 {
		return nil, fmt.Errorf("%w: revision must be positive", ErrInvalid)
	}
	kind := input.Kind
	if kind == "" {
		kind = KindLLM
	}
	schemaVersion := input.SchemaVersion
	if schemaVersion == 0 {
		schemaVersion = SchemaVersionV1
	}
	configuration, err := normalizeDraftConfiguration(input.Configuration)
	if err != nil {
		return nil, err
	}
	if err := validateRevisionDefinition(kind, schemaVersion, configuration); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	revision := &Revision{
		TenantID:          input.TenantID,
		AppID:             input.AppID,
		Revision:          input.Revision,
		State:             RevisionStateDraft,
		DraftVersion:      1,
		Kind:              kind,
		SchemaVersion:     schemaVersion,
		Description:       configuration.Description,
		Instruction:       configuration.Instruction,
		GlobalInstruction: configuration.GlobalInstruction,
		ModelProfileID:    configuration.ModelProfileID,
		Generation:        cloneGenerationConfig(configuration.Generation),
		Runtime:           configuration.Runtime,
		Tools:             cloneTools(configuration.Tools),
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	return revision, nil
}

// Clone returns a deep copy suitable for repository and execution boundaries.
func (r Revision) Clone() Revision {
	clone := r
	clone.Generation = cloneGenerationConfig(r.Generation)
	clone.Tools = cloneTools(r.Tools)
	clone.PublishedAt = cloneTime(r.PublishedAt)
	return clone
}

// Configuration returns a deep copy of the revision's executable definition.
func (r Revision) Configuration() DraftConfiguration {
	return DraftConfiguration{
		Description:       r.Description,
		Instruction:       r.Instruction,
		GlobalInstruction: r.GlobalInstruction,
		ModelProfileID:    r.ModelProfileID,
		Generation:        cloneGenerationConfig(r.Generation),
		Runtime:           r.Runtime,
		Tools:             cloneTools(r.Tools),
	}
}

// Validate checks identity, schema, configuration, publication state, digest,
// version, and timestamp invariants.
func (r Revision) Validate() error {
	if err := validateTenantID(r.TenantID); err != nil {
		return err
	}
	if err := validateAppID(r.AppID); err != nil {
		return err
	}
	if r.Revision < 1 || r.DraftVersion < 1 {
		return fmt.Errorf("%w: revision and draft version must be positive", ErrInvalid)
	}
	configuration := r.Configuration()
	normalized, err := normalizeDraftConfiguration(configuration)
	if err != nil {
		return err
	}
	if !sameDraftConfiguration(configuration, normalized) {
		return fmt.Errorf("%w: revision configuration must be normalized", ErrInvalid)
	}
	if err := validateRevisionDefinition(r.Kind, r.SchemaVersion, configuration); err != nil {
		return err
	}
	if r.CreatedAt.IsZero() || r.UpdatedAt.IsZero() || r.UpdatedAt.Before(r.CreatedAt) {
		return fmt.Errorf("%w: revision timestamps must be initialized and ordered", ErrInvalid)
	}
	switch r.State {
	case RevisionStateDraft:
		if r.ContentDigest != "" || r.PublishedAt != nil {
			return fmt.Errorf("%w: draft revision cannot contain publication metadata", ErrInvalid)
		}
	case RevisionStatePublished:
		if r.PublishedAt == nil || r.PublishedAt.IsZero() || r.PublishedAt.Before(r.CreatedAt) {
			return fmt.Errorf("%w: published revision requires an ordered publication time", ErrInvalid)
		}
		digest, err := r.ComputeContentDigest()
		if err != nil {
			return err
		}
		if r.ContentDigest != digest {
			return fmt.Errorf("%w: published revision content digest mismatch", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown revision state %q", ErrInvalid, r.State)
	}
	return nil
}

// ComputeContentDigest returns the deterministic SHA-256 digest of executable
// content. Publication metadata and draft bookkeeping are excluded.
func (r Revision) ComputeContentDigest() (string, error) {
	configuration := r.Configuration()
	if err := validateRevisionDefinition(r.Kind, r.SchemaVersion, configuration); err != nil {
		return "", err
	}
	payload := struct {
		Kind              Kind                `json:"kind"`
		SchemaVersion     int                 `json:"schema_version"`
		Description       string              `json:"description"`
		Instruction       string              `json:"instruction"`
		GlobalInstruction string              `json:"global_instruction"`
		ModelProfileID    string              `json:"model_profile_id"`
		Generation        GenerationConfig    `json:"generation"`
		Runtime           RuntimePolicy       `json:"runtime"`
		Tools             []ToolAuthorization `json:"tools"`
	}{
		Kind:              r.Kind,
		SchemaVersion:     r.SchemaVersion,
		Description:       r.Description,
		Instruction:       r.Instruction,
		GlobalInstruction: r.GlobalInstruction,
		ModelProfileID:    r.ModelProfileID,
		Generation:        cloneGenerationConfig(r.Generation),
		Runtime:           r.Runtime,
		Tools:             cloneTools(r.Tools),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode agent app revision digest: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// Publish returns an immutable published copy. The source draft is unchanged.
func (r Revision) Publish(at time.Time) (Revision, error) {
	if r.State != RevisionStateDraft {
		return Revision{}, ErrImmutableRevision
	}
	if err := r.Validate(); err != nil {
		return Revision{}, err
	}
	if at.IsZero() || at.Before(r.UpdatedAt) {
		return Revision{}, fmt.Errorf("%w: publication time must follow the latest draft update", ErrInvalid)
	}
	published := r.Clone()
	digest, err := published.ComputeContentDigest()
	if err != nil {
		return Revision{}, err
	}
	at = at.UTC()
	published.State = RevisionStatePublished
	published.ContentDigest = digest
	published.PublishedAt = &at
	published.UpdatedAt = at
	if err := published.Validate(); err != nil {
		return Revision{}, err
	}
	return published, nil
}

func normalizeDraftConfiguration(configuration DraftConfiguration) (DraftConfiguration, error) {
	normalized := configuration
	normalized.Description = strings.TrimSpace(configuration.Description)
	normalized.ModelProfileID = strings.TrimSpace(configuration.ModelProfileID)
	normalized.Generation = cloneGenerationConfig(configuration.Generation)
	if normalized.Runtime == (RuntimePolicy{}) {
		normalized.Runtime = DefaultRuntimePolicy()
	}
	tools, err := normalizeTools(configuration.Tools)
	if err != nil {
		return DraftConfiguration{}, err
	}
	normalized.Tools = tools
	return normalized, nil
}

func validateRevisionDefinition(kind Kind, schemaVersion int, configuration DraftConfiguration) error {
	if kind != KindLLM || schemaVersion != SchemaVersionV1 {
		return fmt.Errorf("%w: unsupported agent kind %q or schema version %d", ErrInvalid, kind, schemaVersion)
	}
	if len([]rune(configuration.Description)) > 2000 {
		return fmt.Errorf("%w: revision description must contain at most 2000 characters", ErrInvalid)
	}
	if n := len([]rune(strings.TrimSpace(configuration.Instruction))); n < 1 || n > maxInstructionRunes {
		return fmt.Errorf("%w: instruction must contain 1-%d characters", ErrInvalid, maxInstructionRunes)
	}
	if len([]rune(configuration.GlobalInstruction)) > maxInstructionRunes {
		return fmt.Errorf("%w: global instruction must contain at most %d characters", ErrInvalid, maxInstructionRunes)
	}
	if n := len([]rune(configuration.ModelProfileID)); n < 1 || n > maxReferenceRunes {
		return fmt.Errorf("%w: model profile id must contain 1-%d characters", ErrInvalid, maxReferenceRunes)
	}
	if err := validateGenerationConfig(configuration.Generation); err != nil {
		return err
	}
	if err := validateRuntimePolicy(configuration.Runtime); err != nil {
		return err
	}
	if _, err := normalizeTools(configuration.Tools); err != nil {
		return err
	}
	return nil
}

func validateGenerationConfig(configuration GenerationConfig) error {
	if configuration.Temperature != nil && (math.IsNaN(*configuration.Temperature) || math.IsInf(*configuration.Temperature, 0) || *configuration.Temperature < 0 || *configuration.Temperature > 2) {
		return fmt.Errorf("%w: temperature must be finite and between 0 and 2", ErrInvalid)
	}
	if configuration.TopP != nil && (math.IsNaN(*configuration.TopP) || math.IsInf(*configuration.TopP, 0) || *configuration.TopP <= 0 || *configuration.TopP > 1) {
		return fmt.Errorf("%w: top p must be finite and in (0,1]", ErrInvalid)
	}
	if configuration.MaxOutputTokens != nil && (*configuration.MaxOutputTokens < 1 || *configuration.MaxOutputTokens > 1_000_000) {
		return fmt.Errorf("%w: max output tokens must be between 1 and 1000000", ErrInvalid)
	}
	return nil
}

func validateRuntimePolicy(policy RuntimePolicy) error {
	if policy.MaxLLMCalls < 1 || policy.MaxLLMCalls > 100 {
		return fmt.Errorf("%w: max LLM calls must be between 1 and 100", ErrInvalid)
	}
	if policy.MaxToolCalls < 0 || policy.MaxToolCalls > 1000 {
		return fmt.Errorf("%w: max tool calls must be between 0 and 1000", ErrInvalid)
	}
	if policy.MaxParallelTools < 1 || policy.MaxParallelTools > 64 {
		return fmt.Errorf("%w: max parallel tools must be between 1 and 64", ErrInvalid)
	}
	if !policy.EnableParallelTools && policy.MaxParallelTools != 1 {
		return fmt.Errorf("%w: serial tool execution requires max parallel tools of 1", ErrInvalid)
	}
	if policy.ExecutionTimeoutSeconds < 1 || policy.ExecutionTimeoutSeconds > 3600 {
		return fmt.Errorf("%w: execution timeout must be between 1 and 3600 seconds", ErrInvalid)
	}
	return nil
}

func normalizeTools(tools []ToolAuthorization) ([]ToolAuthorization, error) {
	normalized := cloneTools(tools)
	seen := make(map[string]struct{}, len(normalized))
	for i := range normalized {
		normalized[i].ToolID = strings.TrimSpace(normalized[i].ToolID)
		if n := len([]rune(normalized[i].ToolID)); n < 1 || n > maxReferenceRunes {
			return nil, fmt.Errorf("%w: tool id must contain 1-%d characters", ErrInvalid, maxReferenceRunes)
		}
		if _, exists := seen[normalized[i].ToolID]; exists {
			return nil, fmt.Errorf("%w: duplicate tool authorization %q", ErrInvalid, normalized[i].ToolID)
		}
		seen[normalized[i].ToolID] = struct{}{}
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].ToolID < normalized[j].ToolID })
	return normalized, nil
}

func sameDraftConfiguration(left, right DraftConfiguration) bool {
	if left.Description != right.Description || left.Instruction != right.Instruction || left.GlobalInstruction != right.GlobalInstruction || left.ModelProfileID != right.ModelProfileID || left.Runtime != right.Runtime {
		return false
	}
	if !sameGenerationConfig(left.Generation, right.Generation) || len(left.Tools) != len(right.Tools) {
		return false
	}
	for i := range left.Tools {
		if left.Tools[i] != right.Tools[i] {
			return false
		}
	}
	return true
}

func sameGenerationConfig(left, right GenerationConfig) bool {
	return sameFloat64(left.Temperature, right.Temperature) && sameFloat64(left.TopP, right.TopP) && sameInt(left.MaxOutputTokens, right.MaxOutputTokens)
}

func sameFloat64(left, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameInt(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func cloneGenerationConfig(configuration GenerationConfig) GenerationConfig {
	clone := configuration
	if configuration.Temperature != nil {
		value := *configuration.Temperature
		clone.Temperature = &value
	}
	if configuration.TopP != nil {
		value := *configuration.TopP
		clone.TopP = &value
	}
	if configuration.MaxOutputTokens != nil {
		value := *configuration.MaxOutputTokens
		clone.MaxOutputTokens = &value
	}
	return clone
}

func cloneTools(tools []ToolAuthorization) []ToolAuthorization {
	if tools == nil {
		return nil
	}
	return append([]ToolAuthorization(nil), tools...)
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}
