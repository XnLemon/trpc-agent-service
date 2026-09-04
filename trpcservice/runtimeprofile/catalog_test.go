package runtimeprofile

import "testing"

func TestValidateCompositionRejectsUnknownKind(t *testing.T) {
	err := ValidateComposition("root", map[string]CompositionConfig{"root": {Kind: "unsupported"}})
	if err == nil {
		t.Fatal("expected unsupported kind error")
	}
}

func TestValidateCompositionRejectsLeafChildren(t *testing.T) {
	if err := ValidateComposition("root", map[string]CompositionConfig{"root": {Kind: "builtin-llm", Children: []string{"child"}}, "child": {Kind: "builtin-llm"}}); err == nil {
		t.Fatal("expected leaf child error")
	}
}
