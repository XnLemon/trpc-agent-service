// Package migration provides tenant-scoped copy, dual-write, validation and
// cutover primitives for runtime backends.
package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"sync"
)

var (
	ErrInvalid    = errors.New("invalid migration request")
	ErrConflict   = errors.New("migration conflict")
	ErrValidation = errors.New("migration validation failed")
	ErrNotFound   = errors.New("migration record not found")
)

type Phase string

const (
	PhaseDualWrite Phase = "dual_write"
	PhaseCopy      Phase = "copy"
	PhaseCatchUp   Phase = "catch_up"
	PhaseValidate  Phase = "validate"
	PhaseCutover   Phase = "cutover"
	PhaseRollback  Phase = "rollback"
)

type Backend string

const (
	BackendSource      Backend = "source"
	BackendDestination Backend = "destination"
)

// Record is the provider-neutral migration unit. Payload is opaque to the
// tool and is never copied into logs or reports.
type Record struct {
	TenantID string
	Kind     string
	Key      string
	Payload  []byte
	Version  int64
}

type Change struct {
	Sequence int64
	Record   Record
}

type Snapshot struct {
	Records   []Record
	Watermark int64
}

type Source interface {
	BeginDualWrite(context.Context, string) (int64, error)
	Snapshot(context.Context, string) (Snapshot, error)
	Changes(context.Context, string, int64) ([]Change, int64, error)
}

type Destination interface {
	Apply(context.Context, []Record) error
	Snapshot(context.Context, string) (Snapshot, error)
}

type Router interface {
	Current(context.Context, string) (Backend, error)
	Set(context.Context, string, Backend) error
}

type Report struct {
	TenantID             string
	Phase                Phase
	Copied               int
	CaughtUp             int
	SourceWatermark      int64
	DestinationWatermark int64
	SourceDigest         string
	DestinationDigest    string
	CutoverBackend       Backend
	RollbackAllowed      bool
	Validated            bool
}

type Tool struct {
	source      Source
	destination Destination
	router      Router
	mu          sync.Mutex
	barriers    map[string]int64
	cutovers    map[string]Backend
}

func NewTool(source Source, destination Destination, router Router) (*Tool, error) {
	if source == nil || destination == nil || router == nil {
		return nil, ErrInvalid
	}
	return &Tool{source: source, destination: destination, router: router, barriers: map[string]int64{}, cutovers: map[string]Backend{}}, nil
}

func (t *Tool) Begin(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	watermark, err := t.source.BeginDualWrite(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	t.mu.Lock()
	t.barriers[tenantID] = watermark
	t.mu.Unlock()
	return Report{TenantID: tenantID, Phase: PhaseDualWrite, SourceWatermark: watermark}, nil
}

func (t *Tool) Copy(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	t.mu.Lock()
	barrier, ok := t.barriers[tenantID]
	t.mu.Unlock()
	if !ok {
		return Report{}, ErrConflict
	}
	snapshot, err := t.source.Snapshot(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	if snapshot.Watermark < barrier {
		return Report{}, ErrConflict
	}
	records := normalizeRecords(tenantID, snapshot.Records)
	if err := t.destination.Apply(ctx, records); err != nil {
		return Report{}, err
	}
	return Report{TenantID: tenantID, Phase: PhaseCopy, Copied: len(records), SourceWatermark: snapshot.Watermark}, nil
}

func (t *Tool) CatchUp(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	t.mu.Lock()
	barrier, ok := t.barriers[tenantID]
	t.mu.Unlock()
	if !ok {
		return Report{}, ErrConflict
	}
	changes, watermark, err := t.source.Changes(ctx, tenantID, barrier)
	if err != nil {
		return Report{}, err
	}
	records := make([]Record, 0, len(changes))
	for _, change := range changes {
		if change.Sequence <= barrier {
			continue
		}
		records = append(records, change.Record)
	}
	if err := t.destination.Apply(ctx, normalizeRecords(tenantID, records)); err != nil {
		return Report{}, err
	}
	return Report{TenantID: tenantID, Phase: PhaseCatchUp, CaughtUp: len(records), SourceWatermark: watermark, DestinationWatermark: watermark}, nil
}

func (t *Tool) Validate(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	source, err := t.source.Snapshot(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	destination, err := t.destination.Snapshot(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	sourceDigest := Digest(source.Records)
	destinationDigest := Digest(destination.Records)
	report := Report{TenantID: tenantID, Phase: PhaseValidate, SourceWatermark: source.Watermark, DestinationWatermark: destination.Watermark, SourceDigest: sourceDigest, DestinationDigest: destinationDigest, Validated: sourceDigest == destinationDigest && len(normalizeRecords(tenantID, source.Records)) == len(normalizeRecords(tenantID, destination.Records))}
	if !report.Validated {
		return report, ErrValidation
	}
	return report, nil
}

func (t *Tool) Cutover(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	validation, err := t.Validate(ctx, tenantID)
	if err != nil {
		return validation, err
	}
	previous, err := t.router.Current(ctx, tenantID)
	if err != nil {
		return Report{}, err
	}
	if err := t.router.Set(ctx, tenantID, BackendDestination); err != nil {
		return Report{}, err
	}
	t.mu.Lock()
	t.cutovers[tenantID] = previous
	t.mu.Unlock()
	validation.Phase, validation.CutoverBackend, validation.RollbackAllowed = PhaseCutover, BackendDestination, true
	return validation, nil
}

func (t *Tool) Rollback(ctx context.Context, tenantID string) (Report, error) {
	if err := validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	t.mu.Lock()
	previous, ok := t.cutovers[tenantID]
	t.mu.Unlock()
	if !ok || previous == BackendDestination {
		return Report{}, ErrConflict
	}
	if _, err := t.Validate(ctx, tenantID); err != nil {
		return Report{}, err
	}
	if err := t.router.Set(ctx, tenantID, previous); err != nil {
		return Report{}, err
	}
	return Report{TenantID: tenantID, Phase: PhaseRollback, CutoverBackend: previous, RollbackAllowed: false, Validated: true}, nil
}

func Digest(records []Record) string {
	ordered := normalizeRecords("", records)
	hash := sha256.New()
	for _, record := range ordered {
		payload, _ := json.Marshal(struct {
			TenantID string `json:"tenant_id"`
			Kind     string `json:"kind"`
			Key      string `json:"key"`
			Payload  []byte `json:"payload"`
			Version  int64  `json:"version"`
		}{record.TenantID, record.Kind, record.Key, record.Payload, record.Version})
		_, _ = hash.Write(payload)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func normalizeRecords(tenantID string, records []Record) []Record {
	result := make([]Record, 0, len(records))
	for _, record := range records {
		if tenantID != "" {
			record.TenantID = tenantID
		}
		record.Payload = append([]byte(nil), record.Payload...)
		result = append(result, record)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].TenantID != result[j].TenantID {
			return result[i].TenantID < result[j].TenantID
		}
		if result[i].Kind != result[j].Kind {
			return result[i].Kind < result[j].Kind
		}
		return result[i].Key < result[j].Key
	})
	return result
}

func validate(ctx context.Context, tenantID string) error {
	if ctx == nil || tenantID == "" {
		return ErrInvalid
	}
	return ctx.Err()
}
