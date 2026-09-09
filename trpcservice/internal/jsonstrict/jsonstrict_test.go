package jsonstrict

import (
	"errors"
	"testing"
)

func TestValidateRejectsAmbiguousJSON(t *testing.T) {
	for _, value := range []string{
		`{"name":1,"name":2}`,
		`{"outer":{"name":1,"name":2}}`,
		`{"name":"\ud800"}`,
		`{"name":"\udc00"}`,
		`{"name":1}{"name":2}`,
		`[]`,
	} {
		if err := Validate([]byte(value), true); err == nil {
			t.Fatalf("Validate(%q) unexpectedly succeeded", value)
		}
	}
	if err := Validate([]byte(`{"name":"\ud83d\ude00"}`), true); err != nil {
		t.Fatalf("valid surrogate pair rejected: %v", err)
	}
}

func TestValidateReportsDuplicateAndDepthErrors(t *testing.T) {
	if err := Validate([]byte(`{"a":1,"a":2}`), true); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate error = %v", err)
	}
	deep := []byte(`{"a":0}`)
	for i := 0; i < maxDepth+2; i++ {
		deep = append([]byte(`{"a":`), append(deep, '}')...)
	}
	if err := Validate(deep, true); !errors.Is(err, ErrTooDeep) {
		t.Fatalf("deep JSON error = %v", err)
	}
}
