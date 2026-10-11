package protocol

import (
	"bytes"
	"encoding/json"
)

// MarshalJSON writes the command as the engine reads it, with stepFailed's retryable always
// present (false is meaningful there), and without HTML escaping, so the bytes the SDK measured
// are the bytes sent.
func (c Command) MarshalJSON() ([]byte, error) {
	type plain Command
	if c.Type != CommandStepFailed {
		return marshal(plain(c))
	}
	return marshal(struct {
		plain
		Retryable bool `json:"retryable"`
	}{plain(c), c.Retryable})
}

func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}
