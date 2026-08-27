package mysql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// EncodeJSON encodes a secret-free control-plane payload.
func EncodeJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode control-plane value: %w", err)
	}
	return encoded, nil
}

// DecodeJSON decodes a complete JSON payload and fails closed on malformed or
// trailing data.
func DecodeJSON(data []byte, destination any) error {
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
