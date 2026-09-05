package log

import (
	"bytes"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewDefaultsToInfoJSONAndRedactsSensitiveFields(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	logger.Debug("hidden", zap.String("debug", "not emitted"))
	logger.With(
		zap.String("client_secret", "child-secret"),
	).Info("created", zap.String("provider_token", "request-secret"), zap.String("tenant_id", "tenant-1"))

	logged := output.String()
	if strings.Contains(logged, "child-secret") || strings.Contains(logged, "request-secret") {
		t.Fatalf("sensitive values leaked: %s", logged)
	}
	if !strings.Contains(logged, RedactedValue) {
		t.Fatalf("redacted marker missing: %s", logged)
	}
	if !strings.Contains(logged, `"tenant_id":"tenant-1"`) {
		t.Fatalf("structured field missing: %s", logged)
	}
	if strings.Contains(logged, "hidden") {
		t.Fatalf("debug entry was emitted at the default level: %s", logged)
	}
	if !strings.Contains(logged, `"level":"info"`) {
		t.Fatalf("info level missing: %s", logged)
	}
}

func TestNewFiltersEntriesBelowConfiguredLevel(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Level: zapcore.WarnLevel, Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	logger.Info("not emitted")
	logger.Warn("emitted")

	logged := output.String()
	if strings.Contains(logged, "not emitted") {
		t.Fatalf("info entry was emitted at warn level: %s", logged)
	}
	if !strings.Contains(logged, "emitted") {
		t.Fatalf("warn entry was not emitted: %s", logged)
	}
}

func TestNewRedactsConfiguredKeySuffix(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Output: &output, RedactKeys: []string{"session_key"}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	logger.Info("configured", zap.String("oauth-session-key", "session-secret"))

	logged := output.String()
	if strings.Contains(logged, "session-secret") || !strings.Contains(logged, RedactedValue) {
		t.Fatalf("configured sensitive field was not redacted: %s", logged)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	if _, err := New(Config{Level: zapcore.Level(99)}); err == nil {
		t.Fatal("New() accepted an invalid log level")
	}
	if _, err := New(Config{Encoding: Encoding("xml")}); err == nil {
		t.Fatal("New() accepted an invalid log encoding")
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input string
		want  zapcore.Level
	}{
		{input: "debug", want: zapcore.DebugLevel},
		{input: " INFO ", want: zapcore.InfoLevel},
		{input: "warn", want: zapcore.WarnLevel},
		{input: "error", want: zapcore.ErrorLevel},
		{input: "dpanic", want: zapcore.DPanicLevel},
		{input: "panic", want: zapcore.PanicLevel},
		{input: "fatal", want: zapcore.FatalLevel},
	}

	for _, test := range tests {
		got, err := ParseLevel(test.input)
		if err != nil {
			t.Errorf("ParseLevel(%q) error = %v", test.input, err)
			continue
		}
		if got != test.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", test.input, got, test.want)
		}
	}

	if _, err := ParseLevel("verbose"); err == nil {
		t.Fatal("ParseLevel accepted an unknown level")
	}
}

func TestSetDefaultUsesPackageHelpers(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Output: &output})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	restore := SetDefault(logger)
	t.Cleanup(restore)

	Info("default logger", zap.String("request_id", "request-1"))

	logged := output.String()
	if !strings.Contains(logged, `"request_id":"request-1"`) {
		t.Fatalf("package helper did not use the configured default: %s", logged)
	}
	if strings.Contains(logged, "log/log.go") {
		t.Fatalf("package helper reported its implementation as the caller: %s", logged)
	}
}
