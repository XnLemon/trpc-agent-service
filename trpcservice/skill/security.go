// Package skill defines the platform security boundary around upstream Agent
// Skills. The upstream package intentionally only describes how to read and
// execute a skill; this package binds that capability to one immutable
// tenant/App/Revision scope and to an authenticated content manifest.
package skill

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/jsonstrict"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	upstreamskill "trpc.group/trpc-go/trpc-agent-go/skill"
)

var (
	// ErrInvalid reports malformed skill security metadata.
	ErrInvalid = errors.New("invalid skill security")
	// ErrScopeViolation reports an attempt to use a skill outside its sealed
	// tenant/App/Revision boundary.
	ErrScopeViolation = errors.New("skill scope violation")
	// ErrSkillNotAllowed reports a skill that is not in the published allowlist.
	ErrSkillNotAllowed = errors.New("skill is not authorized")
	// ErrUntrustedSource reports a repository without a configured trust
	// identity.
	ErrUntrustedSource = errors.New("skill source is not trusted")
	// ErrManifestUnavailable reports a repository that cannot attest the
	// version and content selected by a published Revision.
	ErrManifestUnavailable = errors.New("skill manifest is unavailable")
	// ErrVersionMismatch reports a changed or unexpected skill version.
	ErrVersionMismatch = errors.New("skill version mismatch")
	// ErrDigestMismatch reports changed skill content.
	ErrDigestMismatch = errors.New("skill content digest mismatch")
	// ErrSignatureInvalid reports a missing or invalid source signature.
	ErrSignatureInvalid = errors.New("skill signature is invalid")
	// ErrExecutionDenied reports a skill execution request that is not
	// explicitly authorized by its immutable policy.
	ErrExecutionDenied = errors.New("skill execution is denied")
	// ErrResourceLimit reports a skill execution request over its budget.
	ErrResourceLimit = errors.New("skill resource limit exceeded")
)

const (
	// ManifestFile is the optional per-skill attestation file used by the
	// filesystem adapter. It is deliberately excluded from the content digest
	// to avoid a signature/digest cycle.
	ManifestFile = ".skill-manifest.json"

	skillSignatureVersion = "trpc-agent-skill/v1"

	maxSkillReferenceRunes = 256
	maxSkillVersionRunes   = 128
	maxSkillSourceRunes    = 128
	maxSkillSignatureBytes = 4096
	maxSkillManifestBytes  = 64 << 10
	maxSkillFiles          = 4096
	maxSkillFileBytes      = 16 << 20
	maxSkillTotalBytes     = 256 << 20

	defaultSkillDurationSeconds = 300
	maxSkillDurationSeconds     = 3600
	defaultSkillOutputBytes     = 64 << 10
	maxSkillOutputBytes         = 64 << 20
	defaultSkillWorkspaceBytes  = 64 << 20
	maxSkillWorkspaceBytes      = 256 << 20
	defaultSkillInputBytes      = 8 << 20
	maxSkillInputBytes          = 16 << 20
	defaultSkillOutputFiles     = 64
	maxSkillOutputFiles         = 1024
	defaultSkillConcurrentRuns  = 1
	maxSkillConcurrentRuns      = 32
)

// Scope is the immutable platform identity to which a skill is bound.
type Scope struct {
	TenantID string
	AppID    string
	Revision int64
}

// Validate checks the generic form of a scope. The App ID's canonical app_
// ULID shape is checked by the app package at the control-plane boundary; this
// package deliberately does not import app to avoid a package cycle.
func (scope Scope) Validate() error {
	if scope.Revision < 1 || !validIdentity(scope.TenantID) || !validIdentity(scope.AppID) {
		return ErrInvalid
	}
	return nil
}

// ExecutionPolicy is the deny-by-default execution contract for one skill.
// The upstream runner currently receives KnowledgeOnly skill tooling from the
// platform. These fields are nevertheless part of the sealed contract so a
// future executor cannot silently choose unlimited shell access.
type ExecutionPolicy struct {
	Enabled            bool     `json:"enabled,omitempty"`
	AllowedCommands    []string `json:"allowed_commands,omitempty"`
	DeniedCommands     []string `json:"denied_commands,omitempty"`
	MaxDurationSeconds int      `json:"max_duration_seconds,omitempty"`
	MaxOutputBytes     int64    `json:"max_output_bytes,omitempty"`
	MaxWorkspaceBytes  int64    `json:"max_workspace_bytes,omitempty"`
	MaxInputBytes      int64    `json:"max_input_bytes,omitempty"`
	MaxOutputFiles     int      `json:"max_output_files,omitempty"`
	MaxConcurrentRuns  int      `json:"max_concurrent_runs,omitempty"`
}

// DefaultExecutionPolicy returns bounded, disabled-by-default execution
// settings.
func DefaultExecutionPolicy() ExecutionPolicy {
	return ExecutionPolicy{
		MaxDurationSeconds: defaultSkillDurationSeconds,
		MaxOutputBytes:     defaultSkillOutputBytes,
		MaxWorkspaceBytes:  defaultSkillWorkspaceBytes,
		MaxInputBytes:      defaultSkillInputBytes,
		MaxOutputFiles:     defaultSkillOutputFiles,
		MaxConcurrentRuns:  defaultSkillConcurrentRuns,
	}
}

// Normalize validates and canonicalizes an execution policy.
func (policy ExecutionPolicy) Normalize() (ExecutionPolicy, error) {
	normalized := policy
	var err error
	normalized.AllowedCommands, err = normalizeCommands(policy.AllowedCommands)
	if err != nil {
		return ExecutionPolicy{}, err
	}
	normalized.DeniedCommands, err = normalizeCommands(policy.DeniedCommands)
	if err != nil {
		return ExecutionPolicy{}, err
	}
	if !policy.Enabled && (len(normalized.AllowedCommands) > 0 || len(normalized.DeniedCommands) > 0) {
		return ExecutionPolicy{}, fmt.Errorf("%w: command permissions require enabled execution", ErrInvalid)
	}
	if policy.MaxDurationSeconds == 0 {
		normalized.MaxDurationSeconds = defaultSkillDurationSeconds
	}
	if policy.MaxOutputBytes == 0 {
		normalized.MaxOutputBytes = defaultSkillOutputBytes
	}
	if policy.MaxWorkspaceBytes == 0 {
		normalized.MaxWorkspaceBytes = defaultSkillWorkspaceBytes
	}
	if policy.MaxInputBytes == 0 {
		normalized.MaxInputBytes = defaultSkillInputBytes
	}
	if policy.MaxOutputFiles == 0 {
		normalized.MaxOutputFiles = defaultSkillOutputFiles
	}
	if policy.MaxConcurrentRuns == 0 {
		normalized.MaxConcurrentRuns = defaultSkillConcurrentRuns
	}
	if normalized.MaxDurationSeconds < 1 || normalized.MaxDurationSeconds > maxSkillDurationSeconds ||
		normalized.MaxOutputBytes < 1 || normalized.MaxOutputBytes > maxSkillOutputBytes ||
		normalized.MaxWorkspaceBytes < 1 || normalized.MaxWorkspaceBytes > maxSkillWorkspaceBytes ||
		normalized.MaxInputBytes < 1 || normalized.MaxInputBytes > maxSkillInputBytes ||
		normalized.MaxOutputFiles < 1 || normalized.MaxOutputFiles > maxSkillOutputFiles ||
		normalized.MaxConcurrentRuns < 1 || normalized.MaxConcurrentRuns > maxSkillConcurrentRuns {
		return ExecutionPolicy{}, fmt.Errorf("%w: skill execution resource limits are outside bounds", ErrInvalid)
	}
	for _, denied := range normalized.DeniedCommands {
		for _, allowed := range normalized.AllowedCommands {
			if denied == allowed {
				return ExecutionPolicy{}, fmt.Errorf("%w: command %q is both allowed and denied", ErrInvalid, denied)
			}
		}
	}
	return normalized, nil
}

// Validate checks the policy without exposing provider-specific errors.
func (policy ExecutionPolicy) Validate() error {
	_, err := policy.Normalize()
	return err
}

// ExecutionRequest contains measured execution inputs for policy enforcement.
type ExecutionRequest struct {
	Command        string
	InputBytes     int64
	WorkspaceBytes int64
	OutputBytes    int64
	OutputFiles    int
	Duration       time.Duration
}

// Authorize checks command permission and all configured resource limits.
func (policy ExecutionPolicy) Authorize(request ExecutionRequest) error {
	normalized, err := policy.Normalize()
	if err != nil {
		return err
	}
	if !normalized.Enabled {
		return ErrExecutionDenied
	}
	if request.InputBytes < 0 || request.WorkspaceBytes < 0 || request.OutputBytes < 0 || request.OutputFiles < 0 || request.Duration < 0 {
		return ErrInvalid
	}
	if request.InputBytes > normalized.MaxInputBytes || request.WorkspaceBytes > normalized.MaxWorkspaceBytes || request.OutputBytes > normalized.MaxOutputBytes || request.OutputFiles > normalized.MaxOutputFiles || request.Duration > time.Duration(normalized.MaxDurationSeconds)*time.Second {
		return ErrResourceLimit
	}
	command, err := commandName(request.Command)
	if err != nil {
		return ErrExecutionDenied
	}
	for _, denied := range normalized.DeniedCommands {
		if command == denied {
			return ErrExecutionDenied
		}
	}
	if len(normalized.AllowedCommands) == 0 {
		return nil
	}
	for _, allowed := range normalized.AllowedCommands {
		if command == allowed {
			return nil
		}
	}
	return ErrExecutionDenied
}

// Authorization pins one named skill to an immutable source/version/content
// identity and records its execution policy.
type Authorization struct {
	Name           string          `json:"name"`
	Version        string          `json:"version"`
	ContentDigest  string          `json:"content_digest"`
	Source         string          `json:"source"`
	Signature      string          `json:"signature,omitempty"`
	SignatureKeyID string          `json:"signature_key_id,omitempty"`
	Execution      ExecutionPolicy `json:"execution,omitempty"`
}

// Normalize validates and canonicalizes a published authorization.
func (authorization Authorization) Normalize() (Authorization, error) {
	normalized := authorization
	normalized.Name = strings.TrimSpace(authorization.Name)
	normalized.Version = strings.TrimSpace(authorization.Version)
	normalized.ContentDigest = strings.TrimSpace(authorization.ContentDigest)
	normalized.Source = strings.ToLower(strings.TrimSpace(authorization.Source))
	normalized.Signature = strings.TrimSpace(authorization.Signature)
	normalized.SignatureKeyID = strings.TrimSpace(authorization.SignatureKeyID)
	if !validSkillReference(normalized.Name, maxSkillReferenceRunes) || !validSkillReference(normalized.Version, maxSkillVersionRunes) || !validSkillReference(normalized.Source, maxSkillSourceRunes) || normalized.ContentDigest != strings.ToLower(normalized.ContentDigest) || !validSHA256(normalized.ContentDigest) {
		return Authorization{}, fmt.Errorf("%w: skill authorization identity is invalid", ErrInvalid)
	}
	if normalized.SignatureKeyID != "" && !validSkillReference(normalized.SignatureKeyID, maxSkillReferenceRunes) {
		return Authorization{}, fmt.Errorf("%w: skill signature key id is invalid", ErrInvalid)
	}
	if normalized.Signature != "" {
		signature, ok := decodeSignature(normalized.Signature)
		if !ok || len(signature) != ed25519.SignatureSize || len(normalized.Signature) > maxSkillSignatureBytes {
			return Authorization{}, fmt.Errorf("%w: skill signature is invalid", ErrInvalid)
		}
	}
	var err error
	normalized.Execution, err = authorization.Execution.Normalize()
	if err != nil {
		return Authorization{}, err
	}
	return normalized, nil
}

// Validate checks the authorization without exposing provider details.
func (authorization Authorization) Validate() error {
	_, err := authorization.Normalize()
	return err
}

// Clone returns a defensive authorization copy.
func (authorization Authorization) Clone() Authorization {
	clone := authorization
	clone.Execution.AllowedCommands = append([]string(nil), authorization.Execution.AllowedCommands...)
	clone.Execution.DeniedCommands = append([]string(nil), authorization.Execution.DeniedCommands...)
	return clone
}

// Equal compares normalized authorization content.
func (authorization Authorization) Equal(other Authorization) bool {
	left, leftErr := authorization.Normalize()
	right, rightErr := other.Normalize()
	if leftErr != nil || rightErr != nil {
		return false
	}
	return left.Name == right.Name && left.Version == right.Version && left.ContentDigest == right.ContentDigest && left.Source == right.Source && left.Signature == right.Signature && left.SignatureKeyID == right.SignatureKeyID && sameExecutionPolicy(left.Execution, right.Execution)
}

// Manifest is the source attestation returned by a trusted repository.
type Manifest struct {
	Name           string `json:"name"`
	Version        string `json:"version"`
	ContentDigest  string `json:"content_digest"`
	Source         string `json:"source"`
	Signature      string `json:"signature,omitempty"`
	SignatureKeyID string `json:"signature_key_id,omitempty"`
}

func (manifest Manifest) Normalize() (Manifest, error) {
	authorization, err := (Authorization{
		Name: manifest.Name, Version: manifest.Version, ContentDigest: manifest.ContentDigest,
		Source: manifest.Source, Signature: manifest.Signature, SignatureKeyID: manifest.SignatureKeyID,
	}).Normalize()
	if err != nil {
		return Manifest{}, err
	}
	return Manifest{
		Name: authorization.Name, Version: authorization.Version, ContentDigest: authorization.ContentDigest,
		Source: authorization.Source, Signature: authorization.Signature, SignatureKeyID: authorization.SignatureKeyID,
	}, nil
}

// Manifest returns the expected source manifest for an authorization.
func (authorization Authorization) Manifest() Manifest {
	return Manifest{
		Name: authorization.Name, Version: authorization.Version, ContentDigest: authorization.ContentDigest,
		Source: authorization.Source, Signature: authorization.Signature, SignatureKeyID: authorization.SignatureKeyID,
	}
}

// SignaturePayload is the canonical bytes signed by skill publishers.
func (manifest Manifest) SignaturePayload(scope Scope) []byte {
	payload := struct {
		Version       string `json:"protocol"`
		TenantID      string `json:"tenant_id"`
		AppID         string `json:"app_id"`
		Revision      int64  `json:"revision"`
		Name          string `json:"name"`
		SkillVersion  string `json:"skill_version"`
		ContentDigest string `json:"content_digest"`
		Source        string `json:"source"`
		SignatureKey  string `json:"signature_key_id,omitempty"`
	}{
		Version: skillSignatureVersion, TenantID: scope.TenantID, AppID: scope.AppID,
		Revision: scope.Revision, Name: manifest.Name, SkillVersion: manifest.Version,
		ContentDigest: manifest.ContentDigest, Source: manifest.Source, SignatureKey: manifest.SignatureKeyID,
	}
	encoded, _ := json.Marshal(payload)
	return encoded
}

// SignaturePayload returns the canonical bytes signed by skill publishers.
func (authorization Authorization) SignaturePayload(scope Scope) []byte {
	return authorization.Manifest().SignaturePayload(scope)
}

// SignatureVerifier can implement a platform key registry or an external
// signature service. It is called only after source and manifest identity
// checks have passed.
type SignatureVerifier func(context.Context, Scope, Manifest) error

// TrustPolicy describes which repository sources and signing keys are trusted.
type TrustPolicy struct {
	TrustedSources   []string
	PublicKeys       map[string]ed25519.PublicKey
	RequireSignature bool
	// AllowUnsigned must be explicitly enabled for a trusted source that does
	// not publish Ed25519 signatures. The zero value therefore fails closed.
	AllowUnsigned bool
	Verifier      SignatureVerifier
}

// Normalize validates and defensively copies trust configuration.
func (policy TrustPolicy) Normalize() (TrustPolicy, error) {
	normalized := policy
	var err error
	normalized.TrustedSources, err = normalizeReferences(policy.TrustedSources, maxSkillSourceRunes, true)
	if err != nil || len(normalized.TrustedSources) == 0 {
		return TrustPolicy{}, fmt.Errorf("%w: at least one trusted skill source is required", ErrInvalid)
	}
	normalized.PublicKeys = make(map[string]ed25519.PublicKey, len(policy.PublicKeys))
	for keyID, key := range policy.PublicKeys {
		keyID = strings.TrimSpace(keyID)
		if !validSkillReference(keyID, maxSkillReferenceRunes) || len(key) != ed25519.PublicKeySize {
			return TrustPolicy{}, fmt.Errorf("%w: invalid skill public key", ErrInvalid)
		}
		normalized.PublicKeys[keyID] = append(ed25519.PublicKey(nil), key...)
	}
	return normalized, nil
}

// Verify checks source identity and, when configured, the Ed25519 signature.
func (policy TrustPolicy) Verify(ctx context.Context, scope Scope, manifest Manifest) error {
	if nilvalue.Is(ctx) || nilvalue.ContextErr(ctx) != nil {
		return ErrScopeViolation
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	normalizedPolicy, err := policy.Normalize()
	if err != nil {
		return err
	}
	normalizedManifest, err := manifest.Normalize()
	if err != nil {
		return err
	}
	trusted := false
	for _, source := range normalizedPolicy.TrustedSources {
		if source == normalizedManifest.Source {
			trusted = true
			break
		}
	}
	if !trusted {
		return ErrUntrustedSource
	}
	if normalizedManifest.Signature == "" {
		if normalizedPolicy.RequireSignature || !normalizedPolicy.AllowUnsigned {
			return ErrSignatureInvalid
		}
		return nil
	}
	if normalizedPolicy.Verifier != nil {
		if !callSignatureVerifier(normalizedPolicy.Verifier, ctx, scope, normalizedManifest) {
			return ErrSignatureInvalid
		}
		return nil
	}
	key, ok := selectPublicKey(normalizedPolicy.PublicKeys, normalizedManifest.SignatureKeyID)
	if !ok {
		return ErrSignatureInvalid
	}
	signature, ok := decodeSignature(normalizedManifest.Signature)
	if !ok || !ed25519.Verify(key, normalizedManifest.SignaturePayload(scope), signature) {
		return ErrSignatureInvalid
	}
	return nil
}

// SourceRepository identifies the configured trust source of a repository.
type SourceRepository interface {
	SkillSource() string
}

// ManifestRepository attests the immutable version/digest selected by name.
type ManifestRepository interface {
	SkillManifest(context.Context, string) (Manifest, error)
}

// ManifestResolver resolves a manifest from a repository-specific source.
type ManifestResolver func(context.Context, string) (Manifest, error)

// AttestedRepository decorates an upstream repository with a trusted source
// identity and manifest resolver. The decorator itself does not authorize
// skills; SecureRepository performs the tenant/App/Revision and allowlist
// checks before exposing it to an Agent.
type AttestedRepository struct {
	base     upstreamskill.Repository
	source   string
	resolver ManifestResolver
}

var _ upstreamskill.Repository = (*AttestedRepository)(nil)
var _ SourceRepository = (*AttestedRepository)(nil)
var _ ManifestRepository = (*AttestedRepository)(nil)

// NewAttestedRepository creates a source-tagged repository. The source tag is
// meaningful only when the constructor is called by trusted platform code.
func NewAttestedRepository(base upstreamskill.Repository, source string, resolver ManifestResolver) (*AttestedRepository, error) {
	if nilvalue.Is(base) || !validSkillReference(strings.TrimSpace(source), maxSkillSourceRunes) || resolver == nil {
		return nil, fmt.Errorf("%w: attested skill repository is incomplete", ErrInvalid)
	}
	return &AttestedRepository{base: base, source: strings.ToLower(strings.TrimSpace(source)), resolver: resolver}, nil
}

// NewFilesystemAttestedRepository reads ManifestFile from each skill folder.
func NewFilesystemAttestedRepository(base upstreamskill.Repository, source string) (*AttestedRepository, error) {
	return NewAttestedRepository(base, source, func(ctx context.Context, name string) (Manifest, error) {
		if nilvalue.Is(ctx) || nilvalue.ContextErr(ctx) != nil {
			return Manifest{}, ErrScopeViolation
		}
		path, err := safeRepositoryPath(base, name)
		if err != nil || !pathWithinRepositoryRoots(base, path) {
			return Manifest{}, ErrManifestUnavailable
		}
		return readFilesystemManifest(path, name)
	})
}

// SkillSource implements SourceRepository.
func (repository *AttestedRepository) SkillSource() string {
	if repository == nil {
		return ""
	}
	return repository.source
}

// SkillManifest implements ManifestRepository.
func (repository *AttestedRepository) SkillManifest(ctx context.Context, name string) (manifest Manifest, err error) {
	if repository == nil || repository.resolver == nil || nilvalue.Is(ctx) {
		return Manifest{}, ErrManifestUnavailable
	}
	defer func() {
		if recover() != nil {
			manifest = Manifest{}
			err = ErrManifestUnavailable
		}
	}()
	manifest, err = repository.resolver(ctx, name)
	if err != nil {
		return Manifest{}, ErrManifestUnavailable
	}
	return manifest, nil
}

func (repository *AttestedRepository) Summaries() []upstreamskill.Summary {
	if repository == nil || nilvalue.Is(repository.base) {
		return nil
	}
	return safeSummaries(repository.base)
}

func (repository *AttestedRepository) Get(name string) (*upstreamskill.Skill, error) {
	if repository == nil || nilvalue.Is(repository.base) {
		return nil, ErrManifestUnavailable
	}
	return safeRepositoryGet(repository.base, name)
}

func (repository *AttestedRepository) Path(name string) (string, error) {
	if repository == nil || nilvalue.Is(repository.base) {
		return "", ErrManifestUnavailable
	}
	return safeRepositoryPath(repository.base, name)
}

func (repository *AttestedRepository) Roots() []string {
	if repository == nil || nilvalue.Is(repository.base) {
		return nil
	}
	rooted, ok := repository.base.(upstreamskill.RootedRepository)
	if !ok || nilvalue.Is(rooted) {
		return nil
	}
	return safeRepositoryRoots(rooted)
}

// SecureRepository exposes only skills that match the immutable allowlist and
// a trusted, signed content manifest.
type SecureRepository struct {
	base      upstreamskill.Repository
	scope     Scope
	trust     TrustPolicy
	byName    map[string]Authorization
	names     []string
	manifests ManifestRepository
	source    SourceRepository
}

var _ upstreamskill.Repository = (*SecureRepository)(nil)
var _ upstreamskill.ContextRepository = (*SecureRepository)(nil)
var _ upstreamskill.RootedRepository = (*SecureRepository)(nil)

// NewSecureRepository validates and binds a repository to one published scope.
func NewSecureRepository(base upstreamskill.Repository, scope Scope, authorizations []Authorization, trust TrustPolicy) (*SecureRepository, error) {
	if nilvalue.Is(base) {
		return nil, ErrUntrustedSource
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	normalizedTrust, err := trust.Normalize()
	if err != nil {
		return nil, err
	}
	if len(authorizations) == 0 {
		return nil, fmt.Errorf("%w: skill authorization is required", ErrSkillNotAllowed)
	}
	byName := make(map[string]Authorization, len(authorizations))
	for _, authorization := range authorizations {
		normalized, normalizeErr := authorization.Normalize()
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		if _, exists := byName[normalized.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate skill %q", ErrInvalid, normalized.Name)
		}
		byName[normalized.Name] = normalized
	}
	source, sourceOK := base.(SourceRepository)
	manifests, manifestOK := base.(ManifestRepository)
	if !sourceOK || nilvalue.Is(source) || !manifestOK || nilvalue.Is(manifests) {
		return nil, ErrUntrustedSource
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	return &SecureRepository{base: base, scope: scope, trust: normalizedTrust, byName: byName, names: names, manifests: manifests, source: source}, nil
}

// Summaries returns only allowlisted names that the source advertises.
func (repository *SecureRepository) Summaries() []upstreamskill.Summary {
	if repository == nil || nilvalue.Is(repository.base) {
		return nil
	}
	available := make(map[string]upstreamskill.Summary)
	for _, summary := range safeSummaries(repository.base) {
		if _, allowed := repository.byName[summary.Name]; allowed && validSkillReference(summary.Name, maxSkillReferenceRunes) {
			available[summary.Name] = summary
		}
	}
	result := make([]upstreamskill.Summary, 0, len(repository.names))
	for _, name := range repository.names {
		if summary, ok := available[name]; ok {
			result = append(result, summary)
		}
	}
	return result
}

// Get returns a verified skill. The background context is only used by direct
// callers; normal Agent loading uses GetForContext and supplies the sealed
// request scope.
func (repository *SecureRepository) Get(name string) (*upstreamskill.Skill, error) {
	return repository.get(context.Background(), name)
}

// Path returns a path only after the skill content has been verified.
func (repository *SecureRepository) Path(name string) (string, error) {
	if _, err := repository.get(context.Background(), name); err != nil {
		return "", err
	}
	return repository.safePath(name)
}

// Roots forwards trusted root hints without exposing an untrusted repository
// when the delegate does not implement the rooted contract.
func (repository *SecureRepository) Roots() []string {
	if repository == nil || nilvalue.Is(repository.base) {
		return nil
	}
	rooted, ok := repository.base.(upstreamskill.RootedRepository)
	if !ok || nilvalue.Is(rooted) {
		return nil
	}
	return safeRepositoryRoots(rooted)
}

// SummariesForContext enforces the bound context before returning summaries.
func (repository *SecureRepository) SummariesForContext(ctx context.Context) []upstreamskill.Summary {
	if !repository.contextAllowed(ctx) {
		return nil
	}
	return repository.Summaries()
}

// GetForContext verifies the immutable scope and then the skill attestation.
func (repository *SecureRepository) GetForContext(ctx context.Context, name string) (*upstreamskill.Skill, error) {
	if !repository.contextAllowed(ctx) {
		return nil, ErrScopeViolation
	}
	return repository.get(ctx, name)
}

// PathForContext verifies the immutable scope before returning a staged path.
func (repository *SecureRepository) PathForContext(ctx context.Context, name string) (string, error) {
	if !repository.contextAllowed(ctx) {
		return "", ErrScopeViolation
	}
	if _, err := repository.get(ctx, name); err != nil {
		return "", err
	}
	return repository.safePath(name)
}

// SkillRunEnv forwards only bounded, syntactically safe environment data from
// a trusted repository.
func (repository *SecureRepository) SkillRunEnv(ctx context.Context, name string) (map[string]string, error) {
	if !repository.contextAllowed(ctx) {
		return nil, ErrScopeViolation
	}
	if _, err := repository.get(ctx, name); err != nil {
		return nil, err
	}
	provider, ok := repository.base.(interface {
		SkillRunEnv(context.Context, string) (map[string]string, error)
	})
	if !ok || nilvalue.Is(provider) {
		return nil, nil
	}
	values, err := safeSkillRunEnv(provider, ctx, name)
	if err != nil {
		return nil, ErrUntrustedSource
	}
	if !validSkillEnvironment(values) {
		return nil, ErrUntrustedSource
	}
	return cloneEnvironment(values), nil
}

func (repository *SecureRepository) safePath(name string) (string, error) {
	path, err := safeRepositoryPath(repository.base, name)
	if err != nil || strings.TrimSpace(path) == "" {
		return "", ErrUntrustedSource
	}
	if _, rooted := repository.base.(upstreamskill.RootedRepository); rooted && !pathWithinRepositoryRoots(repository.base, path) {
		return "", ErrUntrustedSource
	}
	return path, nil
}

func (repository *SecureRepository) contextAllowed(ctx context.Context) bool {
	if repository == nil || nilvalue.Is(ctx) || nilvalue.ContextErr(ctx) != nil {
		return false
	}
	scope, ok := ScopeFromContext(ctx)
	return ok && scope == repository.scope
}

func (repository *SecureRepository) get(ctx context.Context, name string) (*upstreamskill.Skill, error) {
	if repository == nil || nilvalue.Is(repository.base) {
		return nil, ErrUntrustedSource
	}
	name = strings.TrimSpace(name)
	authorization, ok := repository.byName[name]
	if !ok {
		return nil, ErrSkillNotAllowed
	}
	resolved, err := safeRepositoryGet(repository.base, name)
	if err != nil || resolved == nil {
		return nil, ErrManifestUnavailable
	}
	if resolved.Summary.Name != name {
		return nil, ErrManifestUnavailable
	}
	manifest, err := safeSkillManifest(repository.manifests, ctx, name)
	if err != nil {
		return nil, ErrManifestUnavailable
	}
	normalizedManifest, err := manifest.Normalize()
	if err != nil {
		return nil, ErrManifestUnavailable
	}
	if normalizedManifest.Name != authorization.Name {
		return nil, ErrSkillNotAllowed
	}
	if normalizedManifest.Version != authorization.Version {
		return nil, ErrVersionMismatch
	}
	if normalizedManifest.Source != authorization.Source {
		return nil, ErrUntrustedSource
	}
	if normalizedManifest.ContentDigest != authorization.ContentDigest {
		return nil, ErrDigestMismatch
	}
	if normalizedManifest.Signature != authorization.Signature || normalizedManifest.SignatureKeyID != authorization.SignatureKeyID {
		return nil, ErrSignatureInvalid
	}
	if err := repository.trust.Verify(ctx, repository.scope, normalizedManifest); err != nil {
		return nil, err
	}
	actualDigest, err := digestRepositorySkill(repository.base, name, resolved)
	if err != nil || actualDigest != authorization.ContentDigest {
		return nil, ErrDigestMismatch
	}
	return cloneSkill(resolved), nil
}

// SecureRepositoryProvider is a reusable provider boundary for callers that
// want the security wrapper without the Agent-specific metadata adapter.
type SecureRepositoryProvider struct {
	delegate upstreamskill.RepositoryProvider
	scope    Scope
	auth     []Authorization
	trust    TrustPolicy
}

var _ upstreamskill.RepositoryProvider = (*SecureRepositoryProvider)(nil)

// NewSecureRepositoryProvider binds a provider to a sealed scope.
func NewSecureRepositoryProvider(delegate upstreamskill.RepositoryProvider, scope Scope, authorizations []Authorization, trust TrustPolicy) (*SecureRepositoryProvider, error) {
	if nilvalue.Is(delegate) {
		return nil, ErrUntrustedSource
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if _, err := trust.Normalize(); err != nil {
		return nil, err
	}
	cloned := make([]Authorization, len(authorizations))
	for index, authorization := range authorizations {
		cloned[index] = authorization.Clone()
	}
	return &SecureRepositoryProvider{delegate: delegate, scope: scope, auth: cloned, trust: trust}, nil
}

// Repository resolves and wraps one repository after checking the requested
// upstream App scope and the platform scope carried in context.
func (provider *SecureRepositoryProvider) Repository(ctx context.Context, requested upstreamskill.SkillScope) (repository upstreamskill.Repository, err error) {
	if provider == nil || nilvalue.Is(ctx) || nilvalue.ContextErr(ctx) != nil || nilvalue.Is(provider.delegate) {
		return nil, ErrScopeViolation
	}
	if !provider.contextAllowed(ctx) || requested.AppName != provider.scope.AppID || requested.UserID != "" {
		return nil, ErrScopeViolation
	}
	defer func() {
		if recover() != nil {
			repository = nil
			err = ErrUntrustedSource
		}
	}()
	resolved, resolveErr := provider.delegate.Repository(ctx, requested)
	if resolveErr != nil || nilvalue.Is(resolved) {
		return nil, ErrUntrustedSource
	}
	return NewSecureRepository(resolved, provider.scope, provider.auth, provider.trust)
}

func (provider *SecureRepositoryProvider) contextAllowed(ctx context.Context) bool {
	scope, ok := ScopeFromContext(ctx)
	return ok && scope == provider.scope
}

// WithScope attaches the fixed Skill scope used by context-aware repository
// methods. Agent Runner code installs it after authenticating the request.
func WithScope(ctx context.Context, scope Scope) context.Context {
	if nilvalue.Is(ctx) || scope.Validate() != nil {
		return nil
	}
	return context.WithValue(ctx, skillScopeContextKey{}, scope)
}

// ScopeFromContext returns the validated fixed Skill scope.
func ScopeFromContext(ctx context.Context) (Scope, bool) {
	if nilvalue.Is(ctx) {
		return Scope{}, false
	}
	raw, err := nilvalue.ContextValue(ctx, skillScopeContextKey{})
	if err != nil {
		return Scope{}, false
	}
	scope, ok := raw.(Scope)
	return scope, ok && scope.Validate() == nil
}

type skillScopeContextKey struct{}

type commandList = []string

func normalizeCommands(values []string) ([]string, error) {
	if len(values) > 128 {
		return nil, fmt.Errorf("%w: too many skill commands", ErrInvalid)
	}
	result := make(commandList, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || !validCommand(value) {
			return nil, fmt.Errorf("%w: invalid skill command", ErrInvalid)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("%w: duplicate skill command", ErrInvalid)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func normalizeReferences(values []string, max int, lower bool) ([]string, error) {
	if len(values) > 128 {
		return nil, fmt.Errorf("%w: too many skill references", ErrInvalid)
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if lower {
			value = strings.ToLower(value)
		}
		if !validSkillReference(value, max) {
			return nil, fmt.Errorf("%w: invalid skill reference", ErrInvalid)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("%w: duplicate skill reference", ErrInvalid)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

func validSkillReference(value string, max int) bool {
	if value == "" || len([]rune(value)) > max || !utf8.ValidString(value) || value == "." || value == ".." || strings.Contains(value, "..") {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' || character == '+' {
			continue
		}
		return false
	}
	return true
}

func validCommand(value string) bool {
	if value == "" || len([]rune(value)) > 256 || !utf8.ValidString(value) || strings.ContainsAny(value, " \t\r\n;&|<>`$(){}[]") {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func commandName(command string) (string, error) {
	command = strings.TrimSpace(command)
	if command == "" || strings.ContainsAny(command, "\r\n;&|<>`$(){}[]") {
		return "", ErrExecutionDenied
	}
	fields := strings.Fields(command)
	if len(fields) == 0 || !utf8.ValidString(fields[0]) {
		return "", ErrExecutionDenied
	}
	return fields[0], nil
}

func validIdentity(value string) bool {
	return value != "" && len([]rune(value)) <= maxSkillReferenceRunes && utf8.ValidString(value) && value == strings.TrimSpace(value) && !strings.Contains(value, "://") && strings.IndexFunc(value, unicode.IsControl) < 0
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func decodeSignature(value string) ([]byte, bool) {
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		decoded, err := encoding.DecodeString(value)
		if err == nil {
			return decoded, true
		}
	}
	return nil, false
}

func selectPublicKey(keys map[string]ed25519.PublicKey, keyID string) (ed25519.PublicKey, bool) {
	if keyID != "" {
		key, ok := keys[keyID]
		return key, ok
	}
	if len(keys) != 1 {
		return nil, false
	}
	for _, key := range keys {
		return key, true
	}
	return nil, false
}

func callSignatureVerifier(verifier SignatureVerifier, ctx context.Context, scope Scope, manifest Manifest) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return verifier(ctx, scope, manifest) == nil
}

func sameExecutionPolicy(left, right ExecutionPolicy) bool {
	left, leftErr := left.Normalize()
	right, rightErr := right.Normalize()
	if leftErr != nil || rightErr != nil {
		return false
	}
	return left.Enabled == right.Enabled && left.MaxDurationSeconds == right.MaxDurationSeconds && left.MaxOutputBytes == right.MaxOutputBytes && left.MaxWorkspaceBytes == right.MaxWorkspaceBytes && left.MaxInputBytes == right.MaxInputBytes && left.MaxOutputFiles == right.MaxOutputFiles && left.MaxConcurrentRuns == right.MaxConcurrentRuns && sameStrings(left.AllowedCommands, right.AllowedCommands) && sameStrings(left.DeniedCommands, right.DeniedCommands)
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func safeSummaries(repository upstreamskill.Repository) (summaries []upstreamskill.Summary) {
	defer func() {
		if recover() != nil {
			summaries = nil
		}
	}()
	return repository.Summaries()
}

func safeRepositoryGet(repository upstreamskill.Repository, name string) (value *upstreamskill.Skill, err error) {
	defer func() {
		if recover() != nil {
			value = nil
			err = ErrManifestUnavailable
		}
	}()
	return repository.Get(name)
}

func safeRepositoryPath(repository upstreamskill.Repository, name string) (path string, err error) {
	defer func() {
		if recover() != nil {
			path = ""
			err = ErrManifestUnavailable
		}
	}()
	return repository.Path(name)
}

func safeRepositoryRoots(repository upstreamskill.RootedRepository) (roots []string) {
	defer func() {
		if recover() != nil {
			roots = nil
		}
	}()
	return repository.Roots()
}

func safeSkillManifest(repository ManifestRepository, ctx context.Context, name string) (manifest Manifest, err error) {
	defer func() {
		if recover() != nil {
			manifest = Manifest{}
			err = ErrManifestUnavailable
		}
	}()
	return repository.SkillManifest(ctx, name)
}

func safeSkillRunEnv(provider interface {
	SkillRunEnv(context.Context, string) (map[string]string, error)
}, ctx context.Context, name string) (values map[string]string, err error) {
	defer func() {
		if recover() != nil {
			values = nil
			err = ErrUntrustedSource
		}
	}()
	return provider.SkillRunEnv(ctx, name)
}

func validSkillEnvironment(values map[string]string) bool {
	if len(values) > 128 {
		return false
	}
	for key, value := range values {
		if len(key) == 0 || len(key) > 128 || len(value) > 4096 || !utf8.ValidString(key) || !utf8.ValidString(value) || strings.IndexFunc(key, unicode.IsControl) >= 0 || strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return false
		}
		for index, character := range key {
			if !(character == '_' || character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || index > 0 && character >= '0' && character <= '9') {
				return false
			}
		}
		upper := strings.ToUpper(key)
		if upper == "PATH" || upper == "HOME" || strings.HasSuffix(upper, "TOKEN") || strings.HasSuffix(upper, "SECRET") || strings.HasSuffix(upper, "PASSWORD") || strings.HasPrefix(upper, "LD_") || strings.HasPrefix(upper, "DYLD_") {
			return false
		}
	}
	return true
}

func cloneEnvironment(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func cloneSkill(value *upstreamskill.Skill) *upstreamskill.Skill {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Docs = append([]upstreamskill.Doc(nil), value.Docs...)
	return &clone
}

func digestRepositorySkill(repository upstreamskill.Repository, name string, resolved *upstreamskill.Skill) (string, error) {
	if path, err := safeRepositoryPath(repository, name); err == nil && strings.TrimSpace(path) != "" {
		if _, rooted := repository.(upstreamskill.RootedRepository); rooted && !pathWithinRepositoryRoots(repository, path) {
			return "", ErrUntrustedSource
		}
		return DigestSkillDirectory(path)
	}
	return DigestSkill(resolved)
}

// DigestSkill computes a stable digest for a repository that does not expose a
// filesystem path. Filesystem repositories should use DigestSkillDirectory so
// scripts and auxiliary files are covered as well.
func DigestSkill(value *upstreamskill.Skill) (string, error) {
	if value == nil || !utf8.ValidString(value.Summary.Name) || !utf8.ValidString(value.Summary.Description) || !utf8.ValidString(value.Body) {
		return "", ErrInvalid
	}
	docs := make([]upstreamskill.Doc, len(value.Docs))
	copy(docs, value.Docs)
	sort.Slice(docs, func(i, j int) bool { return docs[i].Path < docs[j].Path })
	seen := make(map[string]struct{}, len(docs))
	for _, doc := range docs {
		if !validDocumentPath(doc.Path) || !utf8.ValidString(doc.Content) {
			return "", ErrInvalid
		}
		if _, exists := seen[doc.Path]; exists {
			return "", ErrInvalid
		}
		seen[doc.Path] = struct{}{}
	}
	payload := struct {
		Name        string              `json:"name"`
		Description string              `json:"description"`
		Body        string              `json:"body"`
		Docs        []upstreamskill.Doc `json:"docs"`
	}{Name: value.Summary.Name, Description: value.Summary.Description, Body: value.Body, Docs: docs}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", ErrInvalid
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// DigestSkillDirectory computes a stable digest over all regular files in a
// skill directory. Symlinks and special files are rejected to prevent a
// manifest from attesting one tree while execution reads another.
func DigestSkillDirectory(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" || strings.IndexFunc(root, func(r rune) bool { return r == '\x00' || unicode.IsControl(r) }) >= 0 {
		return "", ErrInvalid
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", ErrInvalid
	}
	info, err := os.Stat(resolvedRoot)
	if err != nil || !info.IsDir() {
		return "", ErrInvalid
	}
	type fileEntry struct {
		rel  string
		path string
		mode uint32
		size int64
	}
	files := make([]fileEntry, 0)
	var total int64
	err = filepath.WalkDir(resolvedRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry == nil {
			return ErrInvalid
		}
		if path == resolvedRoot {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return ErrInvalid
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Name() == ManifestFile {
			return nil
		}
		fileInfo, infoErr := entry.Info()
		if infoErr != nil || !fileInfo.Mode().IsRegular() || fileInfo.Size() < 0 || fileInfo.Size() > maxSkillFileBytes {
			return ErrInvalid
		}
		if len(files) >= maxSkillFiles || total > maxSkillTotalBytes-fileInfo.Size() {
			return ErrResourceLimit
		}
		relative, relErr := filepath.Rel(resolvedRoot, path)
		if relErr != nil || !validDocumentPath(filepath.ToSlash(relative)) {
			return ErrInvalid
		}
		files = append(files, fileEntry{rel: filepath.ToSlash(relative), path: path, mode: uint32(fileInfo.Mode().Perm()), size: fileInfo.Size()})
		total += fileInfo.Size()
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })
	hash := sha256.New()
	for _, file := range files {
		if _, err := fmt.Fprintf(hash, "%d:%s:%d:%d\n", len(file.rel), file.rel, file.mode, file.size); err != nil {
			return "", ErrInvalid
		}
		opened, openErr := os.Open(file.path)
		if openErr != nil {
			return "", ErrInvalid
		}
		_, copyErr := io.CopyN(hash, opened, file.size)
		closeErr := opened.Close()
		if copyErr != nil || closeErr != nil {
			return "", ErrInvalid
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validDocumentPath(value string) bool {
	value = filepath.ToSlash(strings.TrimSpace(value))
	if value == "" || strings.HasPrefix(value, "/") || strings.Contains(value, "\\") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func pathWithinRepositoryRoots(repository upstreamskill.Repository, path string) bool {
	rooted, ok := repository.(upstreamskill.RootedRepository)
	if !ok || nilvalue.Is(rooted) {
		return false
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	resolvedPath, err = filepath.Abs(filepath.Clean(resolvedPath))
	if err != nil {
		return false
	}
	for _, root := range safeRepositoryRoots(rooted) {
		resolvedRoot, rootErr := filepath.EvalSymlinks(root)
		if rootErr != nil {
			continue
		}
		resolvedRoot, rootErr = filepath.Abs(filepath.Clean(resolvedRoot))
		if rootErr != nil {
			continue
		}
		relative, relErr := filepath.Rel(resolvedRoot, resolvedPath)
		if relErr == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator)) && relative != "." {
			return true
		}
	}
	return false
}

func readFilesystemManifest(root, name string) (Manifest, error) {
	if strings.TrimSpace(root) == "" {
		return Manifest{}, ErrManifestUnavailable
	}
	path := filepath.Join(root, ManifestFile)
	file, err := os.Open(path)
	if err != nil {
		return Manifest{}, ErrManifestUnavailable
	}
	defer file.Close()
	limited := io.LimitReader(file, maxSkillManifestBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil || len(data) > maxSkillManifestBytes || jsonstrict.Validate(data, true) != nil {
		return Manifest{}, ErrManifestUnavailable
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, ErrManifestUnavailable
	}
	if manifest.Name != name {
		return Manifest{}, ErrManifestUnavailable
	}
	return manifest.Normalize()
}
