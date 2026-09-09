package skill

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	upstreamskill "trpc.group/trpc-go/trpc-agent-go/skill"
)

type securityTestRepository struct {
	skills map[string]*upstreamskill.Skill
}

func (repository *securityTestRepository) Summaries() []upstreamskill.Summary {
	result := make([]upstreamskill.Summary, 0, len(repository.skills))
	for _, value := range repository.skills {
		result = append(result, value.Summary)
	}
	return result
}

func (repository *securityTestRepository) Get(name string) (*upstreamskill.Skill, error) {
	value, ok := repository.skills[name]
	if !ok {
		return nil, errors.New("not found")
	}
	return cloneSkill(value), nil
}

func (repository *securityTestRepository) Path(string) (string, error) {
	return "", errors.New("logical repository has no filesystem path")
}

func TestSecureRepositoryPinsScopeManifestDigestAndSignature(t *testing.T) {
	value := &upstreamskill.Skill{Summary: upstreamskill.Summary{Name: "demo", Description: "trusted"}, Body: "Use the trusted workflow.", Docs: []upstreamskill.Doc{{Path: "guide.md", Content: "guide"}}}
	digest, err := DigestSkill(value)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: "tenant", AppID: "app", Revision: 7}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	authorization := Authorization{Name: "demo", Version: "1.2.3", ContentDigest: digest, Source: "test"}
	signature := ed25519.Sign(privateKey, authorization.SignaturePayload(scope))
	authorization.Signature = base64.RawStdEncoding.EncodeToString(signature)
	manifest := authorization.Manifest()
	base := &securityTestRepository{skills: map[string]*upstreamskill.Skill{"demo": value}}
	attested, err := NewAttestedRepository(base, "test", func(context.Context, string) (Manifest, error) { return manifest, nil })
	if err != nil {
		t.Fatal(err)
	}
	secure, err := NewSecureRepository(attested, scope, []Authorization{authorization}, TrustPolicy{
		TrustedSources: []string{"test"}, PublicKeys: map[string]ed25519.PublicKey{"publisher": publicKey}, RequireSignature: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithScope(context.Background(), scope)
	resolved, err := secure.GetForContext(ctx, "demo")
	if err != nil || resolved == nil || resolved.Body != value.Body {
		t.Fatalf("verified skill = %#v, err = %v", resolved, err)
	}
	if _, err := secure.GetForContext(WithScope(context.Background(), Scope{TenantID: "other", AppID: "app", Revision: 7}), "demo"); !errors.Is(err, ErrScopeViolation) {
		t.Fatalf("cross-scope read error = %v", err)
	}
	if _, err := secure.GetForContext(ctx, "other"); !errors.Is(err, ErrSkillNotAllowed) {
		t.Fatalf("unallowlisted read error = %v", err)
	}
}

func TestSecureRepositoryRejectsChangedManifestAndUntrustedSource(t *testing.T) {
	value := &upstreamskill.Skill{Summary: upstreamskill.Summary{Name: "demo"}, Body: "body"}
	digest, err := DigestSkill(value)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: "tenant", AppID: "app", Revision: 1}
	authorization := Authorization{Name: "demo", Version: "1", ContentDigest: digest, Source: "test"}
	base := &securityTestRepository{skills: map[string]*upstreamskill.Skill{"demo": value}}
	attested, err := NewAttestedRepository(base, "test", func(context.Context, string) (Manifest, error) {
		return Manifest{Name: "demo", Version: "2", ContentDigest: digest, Source: "test"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	secure, err := NewSecureRepository(attested, scope, []Authorization{authorization}, TrustPolicy{TrustedSources: []string{"test"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := secure.GetForContext(WithScope(context.Background(), scope), "demo"); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("version mismatch error = %v", err)
	}
	if _, err := NewSecureRepository(base, scope, []Authorization{authorization}, TrustPolicy{TrustedSources: []string{"test"}}); !errors.Is(err, ErrUntrustedSource) {
		t.Fatalf("unattested source error = %v", err)
	}
}

func TestExecutionPolicyIsDisabledAndBounded(t *testing.T) {
	policy := DefaultExecutionPolicy()
	if err := policy.Authorize(ExecutionRequest{Command: "echo hi"}); !errors.Is(err, ErrExecutionDenied) {
		t.Fatalf("disabled execution error = %v", err)
	}
	policy.Enabled = true
	policy.AllowedCommands = []string{"python"}
	if err := policy.Authorize(ExecutionRequest{Command: "python -c pass", Duration: time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := policy.Authorize(ExecutionRequest{Command: "sh -c pass"}); !errors.Is(err, ErrExecutionDenied) {
		t.Fatalf("command permission error = %v", err)
	}
	if err := policy.Authorize(ExecutionRequest{Command: "python", Duration: 10 * time.Minute}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("duration limit error = %v", err)
	}
}
