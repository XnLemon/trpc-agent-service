package bootstrap

import (
	"errors"
	"testing"
)

func TestEnvironmentProtocolAdaptersAreOptInAndValidated(t *testing.T) {
	config := environmentConfig{}
	if err := config.loadProtocols(); err != nil || config.http.A2A.Enabled || config.http.TRPCAgent.Enabled {
		t.Fatalf("protocols should be disabled by default: config=%+v err=%v", config.http, err)
	}
	t.Setenv(envA2AEnabled, "true")
	if err := config.loadProtocols(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing A2A host error = %v", err)
	}
	t.Setenv(envA2AHost, "https://agents.example.test")
	t.Setenv(envA2APath, "/tenant/a2a")
	t.Setenv(envA2AAgentName, "support-agent")
	t.Setenv(envTRPCAgentEnabled, "true")
	t.Setenv(envTRPCAgentBasePath, "/trpc-agent/v1/apps")
	t.Setenv(envTRPCAgentAppName, "support")
	config = environmentConfig{}
	if err := config.loadProtocols(); err != nil {
		t.Fatal(err)
	}
	if !config.http.A2A.Enabled || config.http.A2A.Path != "/tenant/a2a" || !config.http.TRPCAgent.Enabled || config.http.TRPCAgent.AppName != "support" {
		t.Fatalf("protocol configuration = %+v", config.http)
	}
	t.Setenv(envA2APath, "/../escape")
	invalidPathConfig := environmentConfig{}
	if err := invalidPathConfig.loadProtocols(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid A2A path error = %v", err)
	}
}
