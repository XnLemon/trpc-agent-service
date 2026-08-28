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
	if _, err := tool.Cutover(ctx, "tenant-a"); !errors.Is(err, ErrValidation) {
		t.Fatalf("cutover without catch-up = %v", err)
	}
	if current, _ := router.Current(ctx, "tenant-a"); current != BackendSource {
		t.Fatalf("backend changed after failed validation = %q", current)
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
