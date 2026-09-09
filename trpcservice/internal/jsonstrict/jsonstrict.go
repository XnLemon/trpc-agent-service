// Package jsonstrict provides the small strict-JSON boundary used by
// protocol adapters. encoding/json intentionally accepts duplicate object keys
// and repairs lone UTF-16 surrogates; protocol inputs must not have either
// ambiguity.
package jsonstrict

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const maxDepth = 128

var (
	ErrInvalid        = errors.New("invalid JSON")
	ErrDuplicateKey   = errors.New("duplicate JSON object key")
	ErrTooDeep        = errors.New("JSON nesting exceeds limit")
	ErrInvalidUnicode = errors.New("JSON contains invalid Unicode escape")
)

// Validate validates one complete JSON value. When requireObject is true, the
// root value must be a JSON object. Duplicate keys and unpaired UTF-16
// surrogate escapes are rejected rather than being silently normalized by the
// standard library.
func Validate(data []byte, requireObject bool) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("%w: invalid UTF-8", ErrInvalid)
	}
	data = trimJSONSpace(data)
	if len(data) == 0 {
		return fmt.Errorf("%w: empty document", ErrInvalid)
	}
	if requireObject && data[0] != '{' {
		return fmt.Errorf("%w: root object is required", ErrInvalid)
	}
	if err := validateStringEscapes(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := validateValue(decoder, 0); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("%w: trailing JSON value", ErrInvalid)
		}
		return fmt.Errorf("%w: trailing data: %v", ErrInvalid, err)
	}
	return nil
}

func trimJSONSpace(data []byte) []byte {
	start := 0
	for start < len(data) && isJSONSpace(data[start]) {
		start++
	}
	end := len(data)
	for end > start && isJSONSpace(data[end-1]) {
		end--
	}
	return data[start:end]
}

func isJSONSpace(value byte) bool {
	switch value {
	case ' ', '\t', '\r', '\n':
		return true
	default:
		return false
	}
}

func validateValue(decoder *json.Decoder, depth int) error {
	if depth > maxDepth {
		return ErrTooDeep
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	switch delimiter := token.(type) {
	case json.Delim:
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				key, ok := tokenString(decoder)
				if !ok {
					return fmt.Errorf("%w: object key is not a string", ErrInvalid)
				}
				if _, exists := seen[key]; exists {
					return ErrDuplicateKey
				}
				seen[key] = struct{}{}
				if err := validateValue(decoder, depth+1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return fmt.Errorf("%w: object is not closed", ErrInvalid)
			}
		case '[':
			for decoder.More() {
				if err := validateValue(decoder, depth+1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return fmt.Errorf("%w: array is not closed", ErrInvalid)
			}
		default:
			return fmt.Errorf("%w: unexpected delimiter", ErrInvalid)
		}
	}
	return nil
}

func tokenString(decoder *json.Decoder) (string, bool) {
	token, err := decoder.Token()
	value, ok := token.(string)
	return value, err == nil && ok && utf8.ValidString(value)
}

// validateStringEscapes rejects lone UTF-16 surrogate escapes. JSON's syntax
// is validated separately by encoding/json; this pass only needs to recognize
// quoted strings and Unicode escape sequences.
func validateStringEscapes(data []byte) error {
	for index := 0; index < len(data); index++ {
		if data[index] != '"' {
			continue
		}
		for index = index + 1; index < len(data); index++ {
			value := data[index]
			switch value {
			case '"':
				goto stringDone
			case '\\':
				if index+1 >= len(data) {
					return fmt.Errorf("%w: incomplete escape", ErrInvalid)
				}
				if data[index+1] != 'u' {
					index++
					continue
				}
				code, ok := unicodeEscape(data[index+2:])
				if !ok {
					return ErrInvalidUnicode
				}
				if code >= 0xDC00 && code <= 0xDFFF {
					return ErrInvalidUnicode
				}
				if code >= 0xD800 && code <= 0xDBFF {
					if index+12 > len(data) || data[index+6] != '\\' || data[index+7] != 'u' {
						return ErrInvalidUnicode
					}
					low, lowOK := unicodeEscape(data[index+8:])
					if !lowOK || low < 0xDC00 || low > 0xDFFF {
						return ErrInvalidUnicode
					}
					index += 11
					continue
				}
				index += 5
			}
		}
		return fmt.Errorf("%w: unterminated string", ErrInvalid)
	stringDone:
	}
	return nil
}

func unicodeEscape(data []byte) (uint16, bool) {
	if len(data) < 4 {
		return 0, false
	}
	var value uint16
	for _, digit := range data[:4] {
		value <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			value += uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			value += uint16(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			value += uint16(digit-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}
