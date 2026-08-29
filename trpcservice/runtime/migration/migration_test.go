package migration

import (
	"context"
	"errors"
	"testing"
)

func TestMigrationCopyCatchUpValidateCutoverRollback(t *testing.T) {
	ctx := context.Background()
	source, destination, router := NewMemorySource(), NewMemoryDestination(), NewMemoryRouter()
	if err := source.Put("tenant-a", Record{Kind: "session", Key: "s1", Payload: []byte(`{"state":1}`)}); err != nil {
		t.Fatal(err)
	}
	tool, err := NewTool(source, destination, router)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.Copy(ctx, "tenant-a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("copy before dual-write = %v", err)
	}
	if _, err := tool.Begin(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if report, err := tool.Copy(ctx, "tenant-a"); err != nil || report.Copied != 1 {
		t.Fatalf("copy = %+v err=%v", report, err)
	}
	if err := source.Put("tenant-a", Record{Kind: "memory", Key: "m1", Payload: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if report, err := tool.CatchUp(ctx, "tenant-a"); err != nil || report.CaughtUp != 1 {
		t.Fatalf("catch-up = %+v err=%v", report, err)
	}
	if report, err := tool.Validate(ctx, "tenant-a"); err != nil || !report.Validated || report.SourceDigest != report.DestinationDigest {
		t.Fatalf("validate = %+v err=%v", report, err)
	}
	if report, err := tool.Cutover(ctx, "tenant-a"); err != nil || report.CutoverBackend != BackendDestination {
		t.Fatalf("cutover = %+v err=%v", report, err)
	}
	if report, err := tool.Cutover(ctx, "tenant-a"); err != nil || !report.RollbackAllowed {
		t.Fatalf("idempotent cutover = %+v err=%v", report, err)
	}
	if current, _ := router.Current(ctx, "tenant-a"); current != BackendDestination {
		t.Fatalf("current backend = %q", current)
	}
	if report, err := tool.Rollback(ctx, "tenant-a"); err != nil || report.CutoverBackend != BackendSource {
		t.Fatalf("rollback = %+v err=%v", report, err)
	}
}

func TestMigrationChecksumBlocksCutover(t *testing.T) {
	ctx := context.Background()
	source, destination, router := NewMemorySource(), NewMemoryDestination(), NewMemoryRouter()
	_ = source.Put("tenant-a", Record{Kind: "session", Key: "s1", Payload: []byte("one")})
	tool, _ := NewTool(source, destination, router)
	if _, err := tool.Cutover(context.Background(), "tenant-a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("cutover before dual-write = %v", err)
	}
	_, _ = tool.Begin(ctx, "tenant-a")
	_, _ = tool.Copy(ctx, "tenant-a")
	_ = source.Put("tenant-a", Record{Kind: "session", Key: "s1", Payload: []byte("two")})
	if _, err := tool.Validate(ctx, "tenant-a"); !errors.Is(err, ErrValidation) {
		t.Fatalf("validate without catch-up = %v", err)
	}
	if _, err := tool.Cutover(ctx, "tenant-a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("cutover without catch-up = %v", err)
	}
	if current, _ := router.Current(ctx, "tenant-a"); current != BackendSource {
		t.Fatalf("backend changed after failed validation = %q", current)
	}
}

func TestMigrationCutoverRequiresCopyAndCatchUp(t *testing.T) {
	ctx := context.Background()
	source, destination, router := NewMemorySource(), NewMemoryDestination(), NewMemoryRouter()
	tool, err := NewTool(source, destination, router)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.Begin(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := tool.CatchUp(ctx, "tenant-a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("catch-up before copy = %v", err)
	}
	if _, err := tool.Cutover(ctx, "tenant-a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("cutover before copy = %v", err)
	}
	if _, err := tool.Copy(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := tool.Cutover(ctx, "tenant-a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("cutover before catch-up = %v", err)
	}
	if _, err := tool.CatchUp(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := tool.Cutover(ctx, "tenant-a"); err != nil {
		t.Fatalf("cutover after required phases = %v", err)
	}
}

func TestDigestIsOrderIndependentAndCopiesPayload(t *testing.T) {
	one := []Record{{TenantID: "t", Kind: "b", Key: "2", Payload: []byte("b"), Version: 2}, {TenantID: "t", Kind: "a", Key: "1", Payload: []byte("a"), Version: 1}}
	two := []Record{{TenantID: "t", Kind: "a", Key: "1", Payload: []byte("a"), Version: 1}, {TenantID: "t", Kind: "b", Key: "2", Payload: []byte("b"), Version: 2}}
	if Digest(one) != Digest(two) {
		t.Fatal("digest depends on record order")
	}
	digest := Digest(one)
	one[0].Payload[0] = 'x'
	if Digest(two) != digest {
		t.Fatal("digest input mutation unexpectedly changed prior result")
	}
}

func TestMigrationValidationBoundaries(t *testing.T) {
	if _, err := NewTool(nil, nil, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid tool = %v", err)
	}
	source, destination, router := NewMemorySource(), NewMemoryDestination(), NewMemoryRouter()
	tool, _ := NewTool(source, destination, router)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tool.Begin(canceled, "tenant-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled begin = %v", err)
	}
	if _, err := tool.Rollback(context.Background(), "tenant-a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("rollback before cutover = %v", err)
	}
	if err := source.Put("tenant-a", Record{Kind: "session", Key: "s1", Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	_, _ = tool.Begin(context.Background(), "tenant-a")
	if _, err := tool.Copy(context.Background(), "tenant-b"); !errors.Is(err, ErrConflict) {
		t.Fatalf("copy without tenant barrier = %v", err)
	}
	if err := source.Put("", Record{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid source put = %v", err)
	}
	if _, err := router.Current(context.Background(), ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid router current = %v", err)
	}
	if err := router.Set(context.Background(), "tenant-a", Backend("unknown")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid router set = %v", err)
	}
}

type stubSource struct {
	watermark   int64
	records     []Record
	changes     []Change
	beginErr    error
	snapshotErr error
	changesErr  error
}

func (s *stubSource) BeginDualWrite(context.Context, string) (int64, error) {
	return s.watermark, s.beginErr
}

func (s *stubSource) Snapshot(context.Context, string) (Snapshot, error) {
	return Snapshot{Records: s.records, Watermark: s.watermark}, s.snapshotErr
}

func (s *stubSource) Changes(context.Context, string, int64) ([]Change, int64, error) {
	return s.changes, s.watermark, s.changesErr
}

type stubDestination struct {
	applyErr    error
	snapshotErr error
	records     []Record
}

func (d *stubDestination) Apply(context.Context, []Record) error { return d.applyErr }

func (d *stubDestination) Snapshot(context.Context, string) (Snapshot, error) {
	return Snapshot{Records: d.records}, d.snapshotErr
}

type stubRouter struct {
	backend    Backend
	currentErr error
	setErr     error
}

func (r *stubRouter) Current(context.Context, string) (Backend, error) {
	if r.currentErr != nil {
		return "", r.currentErr
	}
	if r.backend == "" {
		return BackendSource, nil
	}
	return r.backend, nil
}

func (r *stubRouter) Set(context.Context, string, Backend) error { return r.setErr }

func TestMigrationAdapterErrorsAndPhaseBoundaries(t *testing.T) {
	ctx := context.Background()
	destination := &stubDestination{}
	router := &stubRouter{}
	if _, err := (&Tool{source: &stubSource{beginErr: errors.New("begin failed")}, destination: destination, router: router, barriers: map[string]int64{}, copied: map[string]bool{}, caughtUp: map[string]bool{}, cutovers: map[string]Backend{}}).Begin(ctx, "tenant-a"); err == nil {
		t.Fatal("begin error was swallowed")
	}
	source := &stubSource{watermark: 5, records: []Record{{Kind: "session", Key: "s1", Payload: []byte("one")}}}
	tool, err := NewTool(source, destination, router)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tool.Begin(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	source.snapshotErr = errors.New("snapshot failed")
	if _, err := tool.Copy(ctx, "tenant-a"); err == nil {
		t.Fatal("snapshot error was swallowed")
	}
	source.snapshotErr = nil
	source.watermark = 4
	if _, err := tool.Copy(ctx, "tenant-a"); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale snapshot = %v", err)
	}
	source.watermark = 5
	destination.applyErr = errors.New("apply failed")
	if _, err := tool.Copy(ctx, "tenant-a"); err == nil {
		t.Fatal("copy apply error was swallowed")
	}
	destination.applyErr = nil
	if _, err := tool.Copy(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := tool.CatchUp(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	source.changesErr = errors.New("changes failed")
	if _, err := tool.CatchUp(ctx, "tenant-a"); err == nil {
		t.Fatal("changes error was swallowed")
	}
	source.changesErr = nil
	source.changes = []Change{{Sequence: 5, Record: Record{Kind: "old", Key: "old", Payload: []byte("old")}}, {Sequence: 6, Record: Record{Kind: "new", Key: "new", Payload: []byte("new")}}}
	destination.applyErr = errors.New("catch-up apply failed")
	if _, err := tool.CatchUp(ctx, "tenant-a"); err == nil {
		t.Fatal("catch-up apply error was swallowed")
	}
	destination.applyErr = nil
	if _, err := tool.CatchUp(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	destination.snapshotErr = errors.New("destination snapshot failed")
	if _, err := tool.Validate(ctx, "tenant-a"); err == nil {
		t.Fatal("destination snapshot error was swallowed")
	}
	destination.snapshotErr = nil
	source.snapshotErr = errors.New("source snapshot failed")
	if _, err := tool.Validate(ctx, "tenant-a"); err == nil {
		t.Fatal("source snapshot error was swallowed")
	}
	source.snapshotErr = nil
	destination.records = []Record{{Kind: "different", Key: "d", Payload: []byte("d")}}
	if _, err := tool.Validate(ctx, "tenant-a"); !errors.Is(err, ErrValidation) {
		t.Fatalf("mismatched validation = %v", err)
	}
	// Restore matching snapshots so cutover reaches router branches.
	destination.records = source.records
	router.currentErr = errors.New("current failed")
	if _, err := tool.Cutover(ctx, "tenant-a"); err == nil {
		t.Fatal("router current error was swallowed")
	}
	router.currentErr = nil
	router.setErr = errors.New("set failed")
	if _, err := tool.Cutover(ctx, "tenant-a"); err == nil {
		t.Fatal("router set error was swallowed")
	}
	router.setErr = nil
	if _, err := tool.Cutover(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	// A destination route without a recorded cutover cannot be replayed safely.
	destinationTool, _ := NewTool(source, destination, &stubRouter{backend: BackendDestination})
	_, _ = destinationTool.Begin(ctx, "tenant-b")
	_, _ = destinationTool.Copy(ctx, "tenant-b")
	_, _ = destinationTool.CatchUp(ctx, "tenant-b")
	if _, err := destinationTool.Cutover(ctx, "tenant-b"); !errors.Is(err, ErrConflict) {
		t.Fatalf("destination without rollback marker = %v", err)
	}
}
