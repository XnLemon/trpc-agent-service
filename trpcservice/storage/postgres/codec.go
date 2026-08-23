package postgres

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func encodeJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode control-plane value: %w", err)
	}
	return encoded, nil
}

func decodeJSON(data []byte, destination any) error {
	if len(data) == 0 {
		data = []byte("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(append([]byte(nil), data...)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return ErrStorage
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return ErrStorage
	}
	return nil
}

// modelJSON is the secret-free shape stored in model_profile JSONB columns.
type modelJSON struct {
	Options    map[string]string      `json:"options,omitempty"`
	Generation model.GenerationConfig `json:"generation,omitempty"`
}

func encodeModelJSON(configuration model.Configuration) ([]byte, []byte, error) {
	options, err := encodeJSON(configuration.Options)
	if err != nil {
		return nil, nil, err
	}
	generation, err := encodeJSON(configuration.Generation)
	if err != nil {
		return nil, nil, err
	}
	return options, generation, nil
}

func decodeModelJSON(options, generation []byte, configuration *model.Configuration) error {
	if err := decodeJSON(options, &configuration.Options); err != nil {
		return err
	}
	if err := decodeJSON(generation, &configuration.Generation); err != nil {
		return err
	}
	return nil
}

type backendBindingJSON struct {
	Capability string            `json:"capability"`
	Provider   string            `json:"provider"`
	Endpoint   string            `json:"endpoint,omitempty"`
	Options    map[string]string `json:"options,omitempty"`
	SecretRef  string            `json:"secret_ref,omitempty"`
}

func encodeBackendBindings(bindings []backend.CapabilityBinding) ([]byte, error) {
	values := make([]backendBindingJSON, 0, len(bindings))
	for _, binding := range bindings {
		values = append(values, backendBindingJSON{
			Capability: string(binding.Capability), Provider: binding.Provider,
			Endpoint: binding.Endpoint, Options: binding.Options, SecretRef: binding.SecretRef,
		})
	}
	return encodeJSON(values)
}

func decodeBackendBindings(data []byte) ([]backend.CapabilityBinding, error) {
	var values []backendBindingJSON
	if err := decodeJSON(data, &values); err != nil {
		return nil, err
	}
	bindings := make([]backend.CapabilityBinding, 0, len(values))
	for _, value := range values {
		bindings = append(bindings, backend.CapabilityBinding{
			Capability: backend.Capability(value.Capability), Provider: value.Provider,
			Endpoint: value.Endpoint, Options: value.Options, SecretRef: value.SecretRef,
		})
	}
	return bindings, nil
}

func encodeAgentRevisionParts(revision agent.Revision) ([]byte, []byte, []byte, error) {
	generation, err := encodeJSON(revision.Generation)
	if err != nil {
		return nil, nil, nil, err
	}
	runtime, err := encodeJSON(revision.Runtime)
	if err != nil {
		return nil, nil, nil, err
	}
	tools, err := encodeJSON(revision.Tools)
	if err != nil {
		return nil, nil, nil, err
	}
	return generation, runtime, tools, nil
}

func decodeAgentRevisionParts(generation, runtime []byte, revision *agent.Revision) error {
	if err := decodeJSON(generation, &revision.Generation); err != nil {
		return err
	}
	if err := decodeJSON(runtime, &revision.Runtime); err != nil {
		return err
	}
	return nil
}

func encodeProtocol(protocol channels.ProtocolConfiguration) ([]byte, error) {
	return encodeJSON(protocol)
}

func decodeProtocol(data []byte, protocol *channels.ProtocolConfiguration) error {
	return decodeJSON(data, protocol)
}
