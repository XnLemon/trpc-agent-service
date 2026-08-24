package main

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
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

	mode, blocks, order, err := mergeProfiles([]string{first, second})
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
	if got, want := strings.Join(order, ","), "example.go:1.1,1.2 1,example.go:2.1,2.2 1,example.go:3.1,3.2 2"; got != want {
		t.Fatalf("profile order = %q, want %q", got, want)
	}
}

func TestRunWritesMergedProfileAndValidatesArguments(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.out")
	second := filepath.Join(dir, "second.out")
	output := filepath.Join(dir, "merged.out")
	if err := os.WriteFile(first, []byte("mode: set\nexample.go:100.1,100.2 1 0\nexample.go:16.1,16.2 1 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("mode: set\nexample.go:16.1,16.2 1 4\nexample.go:20.1,20.2 1 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-out", output, first, second}); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	want := "mode: set\nexample.go:100.1,100.2 1 0\nexample.go:16.1,16.2 1 4\nexample.go:20.1,20.2 1 1\n"
	if string(contents) != want {
		t.Fatalf("merged profile = %q, want %q", contents, want)
	}
	for _, args := range [][]string{{}, {"-out", output}, {"-unknown"}, {"-out", output, filepath.Join(dir, "missing.out")}} {
		if err := run(args); err == nil {
			t.Fatalf("run(%q) succeeded", args)
		}
	}
}

func TestMergeProfilesRejectsMalformedOrIncompatibleProfiles(t *testing.T) {
	dir := t.TempDir()
	valid := filepath.Join(dir, "valid.out")
	if err := os.WriteFile(valid, []byte("mode: set\nexample.go:1.1,1.2 1 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{
		"missing mode":  "",
		"invalid mode":  "coverage: set\n",
		"invalid block": "mode: set\nexample.go:1.1,1.2 1\n",
		"invalid count": "mode: set\nexample.go:1.1,1.2 1 nope\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, strings.ReplaceAll(name, " ", "-")+".out")
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := mergeProfiles([]string{path}); err == nil {
				t.Fatal("malformed profile was accepted")
			}
		})
	}
	atomic := filepath.Join(dir, "atomic.out")
	if err := os.WriteFile(atomic, []byte("mode: atomic\nexample.go:1.1,1.2 1 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := mergeProfiles([]string{valid, atomic}); err == nil {
		t.Fatal("mixed coverage modes were accepted")
	}
}

func TestMergedCoverageBlockOrderRetainsSourceOrderAndSortsNewBlocks(t *testing.T) {
	blocks := map[string]int{
		"example.go:100.1,100.2 1": 0,
		"example.go:16.1,16.2 1":   1,
		"other.go:2.1,2.2 1":       1,
	}
	order := mergedCoverageBlockOrder([]string{"example.go:100.1,100.2 1"}, blocks)
	want := []string{"example.go:100.1,100.2 1", "example.go:16.1,16.2 1", "other.go:2.1,2.2 1"}
	if got := strings.Join(order, ","); got != strings.Join(want, ",") {
		t.Fatalf("merged order = %q, want %q", got, strings.Join(want, ","))
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

func TestCoverageBlockParserFallbacksAndWriteFailure(t *testing.T) {
	for _, block := range []string{"", "missing-separator", "file.go:1.1", "file.go:one.1,2.1 1", "file.go:1.one,2.1 1"} {
		if _, ok := parseCoverageBlockLocation(block); ok {
			t.Fatalf("invalid coverage block accepted: %q", block)
		}
	}
	if coverageBlockLess("invalid-left", "invalid-right") != ("invalid-left" < "invalid-right") {
		t.Fatal("invalid block ordering did not fall back to lexical order")
	}
	if !coverageBlockLess("a.go:1.1,1.2 1", "b.go:1.1,1.2 1") || !coverageBlockLess("a.go:1.1,1.2 1", "a.go:2.1,2.2 1") || !coverageBlockLess("a.go:1.1,1.2 1", "a.go:1.2,1.3 1") || !coverageBlockLess("a.go:1.1,1.2 1", "a.go:1.1,2.1 1") || !coverageBlockLess("a.go:1.1,1.2 1", "a.go:1.1,1.3 1") {
		t.Fatal("valid source locations were not ordered by their numeric position")
	}
	if err := writeProfile(t.TempDir(), "set", map[string]int{}, nil); err == nil {
		t.Fatal("writing a coverage profile to a directory succeeded")
	}
}
