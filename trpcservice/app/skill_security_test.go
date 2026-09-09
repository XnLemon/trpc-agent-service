package app

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRevisionPinsSkillAuthorizationIntoPublishedDigest(t *testing.T) {
	input := validRevisionInput()
	input.Configuration.Skills = []string{"demo"}
	input.Configuration.SkillAuthorizations = []SkillAuthorization{{
		Name: "demo", Version: "1.0.0", ContentDigest: strings.Repeat("a", 64), Source: "filesystem",
	}}
	revision, err := NewRevision(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(revision.Skills) != 1 || revision.Skills[0] != "demo" || len(revision.SkillAuthorizations) != 1 {
		t.Fatalf("skill authorization was not materialized: %+v", revision)
	}
	published, err := revision.Publish(revision.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	changed := published.Clone()
	changed.SkillAuthorizations[0].Version = "2.0.0"
	changedDigest, err := changed.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == published.ContentDigest {
		t.Fatal("skill version change did not alter the revision digest")
	}
	clone := published.Clone()
	clone.SkillAuthorizations[0].Execution.AllowedCommands = []string{"python"}
	if len(published.SkillAuthorizations[0].Execution.AllowedCommands) != 0 {
		t.Fatal("skill execution policy leaked through Revision.Clone")
	}
}

func TestRevisionRejectsUnpinnedSkills(t *testing.T) {
	input := validRevisionInput()
	input.Configuration.Skills = []string{"demo"}
	if _, err := NewRevision(input); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unpinned skill error = %v", err)
	}
	input.Configuration.SkillAuthorizations = []SkillAuthorization{{
		Name: "other", Version: "1", ContentDigest: strings.Repeat("a", 64), Source: "filesystem",
	}}
	if _, err := NewRevision(input); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched skill authorization error = %v", err)
	}
}
