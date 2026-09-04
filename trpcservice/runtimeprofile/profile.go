// Package runtimeprofile owns tenant-scoped Agent Runtime descriptors and
// trusted in-process factory registration.
package runtimeprofile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
)

var (
	ErrInvalid     = errors.New("invalid runtime profile")
	ErrNotFound    = errors.New("runtime profile not found")
	ErrConflict    = errors.New("runtime profile version conflict")
	ErrDuplicate   = errors.New("runtime profile key already exists")
	ErrUnavailable = errors.New("runtime unavailable")
)

// RuntimeProfile is a secret-free, versioned tenant runtime descriptor.
type RuntimeProfile struct {
	TenantID             string
	ProfileID            string
	RuntimeKey           string
	RuntimeKind          string
	ExecutionMode        string
	ImplementationRef    string
	Version              int64
	RuntimeVersion       string
	SchemaVersion        int
	ImplementationDigest string
	ConfigDigest         string
	Config               map[string]any
	Capabilities         []string
	GovernanceMode       string
	SecretRef            string
	Status               string
}

// Validate enforces the persisted descriptor contract.
func (p RuntimeProfile) Validate() error {
	if strings.TrimSpace(p.TenantID) == "" || strings.TrimSpace(p.ProfileID) == "" || strings.TrimSpace(p.RuntimeKey) == "" || strings.TrimSpace(p.RuntimeKind) == "" || strings.TrimSpace(p.ExecutionMode) == "" || strings.TrimSpace(p.ImplementationRef) == "" || p.Version < 1 || p.SchemaVersion < 1 || strings.TrimSpace(p.RuntimeVersion) == "" || strings.TrimSpace(p.ImplementationDigest) == "" || strings.TrimSpace(p.ConfigDigest) == "" {
		return ErrInvalid
	}
	if p.ExecutionMode != "builtin" && p.ExecutionMode != "in_process" && p.ExecutionMode != "remote" {
		return fmt.Errorf("%w: execution mode", ErrInvalid)
	}
	if p.GovernanceMode != "full" && p.GovernanceMode != "perimeter" {
		return fmt.Errorf("%w: governance mode", ErrInvalid)
	}
	if p.Status != "active" && p.Status != "disabled" && p.Status != "draft" {
		return fmt.Errorf("%w: status", ErrInvalid)
	}
	if p.SecretRef != "" && strings.ContainsAny(p.SecretRef, "\r\n") {
		return ErrInvalid
	}
	return nil
}

// ComputeConfigDigest returns a deterministic digest of non-secret config and capabilities.
func (p RuntimeProfile) ComputeConfigDigest() (string, error) {
	v := struct {
		Config       map[string]any `json:"config"`
		Capabilities []string       `json:"capabilities"`
	}{p.Config, append([]string(nil), p.Capabilities...)}
	sort.Strings(v.Capabilities)
	b, e := json.Marshal(v)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// Repository is the tenant-scoped Runtime Profile control-plane contract.
type Repository interface {
	Create(context.Context, RuntimeProfile) (RuntimeProfile, error)
	Get(context.Context, string, string) (RuntimeProfile, error)
	Update(context.Context, RuntimeProfile, int64) (RuntimeProfile, error)
	SetStatus(context.Context, string, string, int64, string) (RuntimeProfile, error)
	List(context.Context, string) ([]RuntimeProfile, error)
}

type scope struct{ tenant, id string }

// InMemoryRepository is a deterministic repository for tests and local operation.
type InMemoryRepository struct {
	mu     sync.RWMutex
	values map[scope]RuntimeProfile
	keys   map[string]scope
}

func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{values: map[scope]RuntimeProfile{}, keys: map[string]scope{}}
}
func (r *InMemoryRepository) Create(ctx context.Context, p RuntimeProfile) (RuntimeProfile, error) {
	if err := ctxErr(ctx); err != nil {
		return RuntimeProfile{}, err
	}
	if err := p.Validate(); err != nil {
		return RuntimeProfile{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := scope{p.TenantID, p.ProfileID}
	if _, ok := r.values[s]; ok {
		return RuntimeProfile{}, ErrDuplicate
	}
	k := p.TenantID + "\x00" + p.RuntimeKey
	if _, ok := r.keys[k]; ok {
		return RuntimeProfile{}, ErrDuplicate
	}
	p.Config = cloneMap(p.Config)
	p.Capabilities = append([]string(nil), p.Capabilities...)
	r.values[s] = p
	r.keys[k] = s
	return clone(p), nil
}
func (r *InMemoryRepository) Get(ctx context.Context, t, id string) (RuntimeProfile, error) {
	if err := ctxErr(ctx); err != nil {
		return RuntimeProfile{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.values[scope{t, id}]
	if !ok {
		return RuntimeProfile{}, ErrNotFound
	}
	return clone(p), nil
}
func (r *InMemoryRepository) Update(ctx context.Context, p RuntimeProfile, expected int64) (RuntimeProfile, error) {
	if err := ctxErr(ctx); err != nil {
		return RuntimeProfile{}, err
	}
	if err := p.Validate(); err != nil {
		return RuntimeProfile{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := scope{p.TenantID, p.ProfileID}
	old, ok := r.values[s]
	if !ok {
		return RuntimeProfile{}, ErrNotFound
	}
	if old.Version != expected {
		return RuntimeProfile{}, ErrConflict
	}
	if old.RuntimeKey != p.RuntimeKey {
		return RuntimeProfile{}, ErrInvalid
	}
	p.Version = expected + 1
	r.values[s] = clone(p)
	return clone(p), nil
}
func (r *InMemoryRepository) SetStatus(ctx context.Context, t, id string, expected int64, status string) (RuntimeProfile, error) {
	p, e := r.Get(ctx, t, id)
	if e != nil {
		return RuntimeProfile{}, e
	}
	p.Status = status
	return r.Update(ctx, p, expected)
}
func (r *InMemoryRepository) List(ctx context.Context, t string) ([]RuntimeProfile, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := []RuntimeProfile{}
	for s, p := range r.values {
		if s.tenant == t {
			out = append(out, clone(p))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RuntimeKey < out[j].RuntimeKey })
	return out, nil
}
func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	return ctx.Err()
}
func clone(p RuntimeProfile) RuntimeProfile {
	p.Config = cloneMap(p.Config)
	p.Capabilities = append([]string(nil), p.Capabilities...)
	return p
}
func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	o := make(map[string]any, len(m))
	for k, v := range m {
		o[k] = v
	}
	return o
}

// Factory builds a Runner from a fixed execution plan.
type Factory func(context.Context, runtime.ExecutionPlan) (trpcrunner.Runner, error)

// RuntimeFactoryRegistry maps trusted implementation keys to factories.
type RuntimeFactoryRegistry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

func NewFactoryRegistry() *RuntimeFactoryRegistry {
	return &RuntimeFactoryRegistry{factories: map[string]Factory{}}
}
func (r *RuntimeFactoryRegistry) Register(key string, f Factory) error {
	if r == nil || strings.TrimSpace(key) == "" || f == nil {
		return ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.factories[key]; ok {
		return ErrDuplicate
	}
	r.factories[key] = f
	return nil
}
func (r *RuntimeFactoryRegistry) Resolve(key string) (Factory, error) {
	if r == nil {
		return nil, ErrUnavailable
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.factories[key]
	if !ok {
		return nil, ErrUnavailable
	}
	return f, nil
}
