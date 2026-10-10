// Package jsonx encodes JSON the way the SDK sends it: without HTML escaping, so the bytes the
// engine measures are the bytes the SDK measured.
package jsonx

import (
	"bytes"
	"encoding/json"
)

// Marshal is json.Marshal without HTML escaping (and without the Encoder's newline).
func Marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// Compact returns raw without insignificant whitespace, or nil when raw isn't JSON.
func Compact(raw []byte) []byte {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil
	}
	return buf.Bytes()
}
