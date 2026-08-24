package main

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestMergeProfilesPreservesUncoveredBlocksAndHighestCount(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.out")
	second := filepath.Join(dir, "second.out")
	if err := os.WriteFile(first, []byte("mode: set\nexample.go:1.1,1.2 1 0\nexample.go:2.1,2.2 1 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("mode: set\nexample.go:2.1,2.2 1 3\nexample.go:3.1,3.2 2 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	mode, blocks, err := mergeProfiles([]string{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if mode != "set" {
		t.Fatalf("mode = %q", mode)
	}
	if got := blocks["example.go:1.1,1.2 1"]; got != 0 {
		t.Fatalf("uncovered block count = %d", got)
	}
	if got := blocks["example.go:2.1,2.2 1"]; got != 3 {
		t.Fatalf("highest block count = %d", got)
	}
	if got := blocks["example.go:3.1,3.2 2"]; got != 0 {
		t.Fatalf("second uncovered block count = %d", got)
	}
}

func TestCoverageBlockLessOrdersSourceLocationsNumerically(t *testing.T) {
	blocks := []string{
		"example.go:101.1,101.2 1",
		"other.go:1.1,1.2 1",
		"example.go:16.3,16.4 1",
		"example.go:16.1,16.2 1",
	}
	sort.Slice(blocks, func(i, j int) bool { return coverageBlockLess(blocks[i], blocks[j]) })
	want := []string{
		"example.go:16.1,16.2 1",
		"example.go:16.3,16.4 1",
		"example.go:101.1,101.2 1",
		"other.go:1.1,1.2 1",
	}
	for index := range want {
		if blocks[index] != want[index] {
			t.Fatalf("block %d = %q, want %q", index, blocks[index], want[index])
		}
	}
}
