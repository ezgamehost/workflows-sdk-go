package workflows

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/ezgamehost/workflows-sdk-go/internal/jsonx"
	"github.com/ezgamehost/workflows-sdk-go/protocol"
)

// encodeValue is the wire form of a step's result or a workflow's output: JSON. A value
// encoding/json refuses (a func, a channel, a NaN, a cycle) fails non-retriably; more than 1 MiB
// fails with workflows.output_too_large. The live result is then decoded from these bytes, so it
// equals the replayed one.
func encodeValue(v any) (json.RawMessage, error) {
	b, err := jsonx.Marshal(v)
	if err != nil {
		return nil, NonRetriable(fmt.Errorf("the result isn't JSON: %w", err))
	}
	if len(b) > protocol.MaxOutputBytes {
		return nil, newError("workflows.output_too_large", "OutputTooLarge", "the result is %d bytes of JSON, more than 1 MiB", len(b))
	}
	return b, nil
}

// decodeInto decodes recorded JSON into T; absent or null leaves T's zero value.
func decodeInto[T any](raw json.RawMessage) (T, error) {
	var out T
	if len(raw) == 0 || string(raw) == "null" {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, NonRetriable(fmt.Errorf("the recorded value doesn't decode into %T: %w", out, err))
	}
	return out, nil
}

// newUUIDv4 is a random (version 4) UUID.
func newUUIDv4(r io.Reader) (string, error) {
	var b [16]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
