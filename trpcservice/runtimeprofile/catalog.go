package runtimeprofile

import (
	"fmt"
	"strings"
)

// CatalogEntry describes one platform-maintained runtime kind and schema.
type CatalogEntry struct {
	Kind           string
	Version        string
	SchemaVersion  int
	ExecutionMode  string
	Capabilities   []string
	GovernanceMode string
}

// BuiltinCatalog returns the immutable platform catalog. Configuration for
// composition is interpreted by the selected implementation; arbitrary code
// references are never accepted as a kind.
func BuiltinCatalog() []CatalogEntry {
	return []CatalogEntry{
		{Kind: "builtin-llm", Version: "v1", SchemaVersion: 1, ExecutionMode: "builtin", Capabilities: []string{"text", "tool"}, GovernanceMode: "full"},
		{Kind: "builtin-chain", Version: "v1", SchemaVersion: 1, ExecutionMode: "builtin", Capabilities: []string{"text", "composition"}, GovernanceMode: "full"},
		{Kind: "builtin-parallel", Version: "v1", SchemaVersion: 1, ExecutionMode: "builtin", Capabilities: []string{"text", "composition"}, GovernanceMode: "full"},
		{Kind: "builtin-cycle", Version: "v1", SchemaVersion: 1, ExecutionMode: "builtin", Capabilities: []string{"text", "composition"}, GovernanceMode: "full"},
		{Kind: "builtin-graph", Version: "v1", SchemaVersion: 1, ExecutionMode: "builtin", Capabilities: []string{"text", "composition"}, GovernanceMode: "full"},
	}
}

// CompositionConfig is a bounded declarative child-agent graph.
type CompositionConfig struct {
	Kind     string
	Children []string
}

// ValidateComposition rejects undeclared children, duplicate edges and cycles.
func ValidateComposition(root string, graph map[string]CompositionConfig) error {
	if strings.TrimSpace(root) == "" {
		return fmt.Errorf("%w: root is required", ErrInvalid)
	}
	state := map[string]uint8{}
	var visit func(string) error
	visit = func(node string) error {
		if state[node] == 1 {
			return fmt.Errorf("%w: composition cycle", ErrInvalid)
		}
		if state[node] == 2 {
			return nil
		}
		cfg, ok := graph[node]
		if !ok {
			return fmt.Errorf("%w: child %s is not declared", ErrInvalid, node)
		}
		validKind := false
		for _, entry := range BuiltinCatalog() {
			if cfg.Kind == entry.Kind {
				validKind = true
				break
			}
		}
		if !validKind {
			return fmt.Errorf("%w: unsupported composition kind %q", ErrInvalid, cfg.Kind)
		}
		state[node] = 1
		seen := map[string]bool{}
		for _, child := range cfg.Children {
			if child == "" || seen[child] {
				return fmt.Errorf("%w: duplicate child", ErrInvalid)
			}
			seen[child] = true
			if err := visit(child); err != nil {
				return err
			}
		}
		state[node] = 2
		return nil
	}
	return visit(root)
}
