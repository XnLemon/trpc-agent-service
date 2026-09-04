package runtimeprofile

import "testing"

func TestValidateCompositionRejectsUnknownKind(t *testing.T) {
	err := ValidateComposition("root", map[string]CompositionConfig{"root": {Kind: "unsupported"}})
	if err == nil {
		t.Fatal("expected unsupported kind error")
	}
}
