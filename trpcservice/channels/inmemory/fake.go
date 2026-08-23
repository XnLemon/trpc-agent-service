package inmemory

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

// These aliases keep the offline fake's existing package path while the
// proof-bearing implementation remains inside channels, where it can mint a
// VerifiedBinding only after successful verification.
type FakeResolverOptions = channels.FakeResolverOptions
type FakeCandidateResolver = channels.FakeCandidateResolver

const (
	DefaultFakeMaxClockSkew = channels.DefaultFakeMaxClockSkew
	DefaultFakeMaxHandles   = channels.DefaultFakeMaxHandles
)

func NewFakeCandidateResolver(repo *InMemoryRepository, secrets map[channels.SecretScope]string, options ...FakeResolverOptions) *FakeCandidateResolver {
	return channels.NewFakeCandidateResolver(repo, secrets, options...)
}

func NewFakeResolver(repo *InMemoryRepository, secrets map[channels.SecretScope]string, options ...FakeResolverOptions) *FakeCandidateResolver {
	return NewFakeCandidateResolver(repo, secrets, options...)
}

func SignFakeRequest(secret string, request channels.VerificationRequest) string {
	return channels.SignFakeRequest(secret, request)
}

// validFakeText is retained for package-local edge tests of the offline fake.
func validFakeText(value string, maxLength int) bool {
	return value != "" && len([]rune(value)) <= maxLength && !strings.ContainsFunc(value, unicode.IsControl)
}

func validLowerHexDigest(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
