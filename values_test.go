package workflows

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// The codec vectors name JavaScript values by tag; these are their Go counterparts. The tags
// with no Go counterpart are skipped: bigint (Go's integers encode as JSON numbers),
// object-with-undefined-field and array-with-undefined (Go has no undefined), and date-epoch,
// whose millisecond format is JavaScript's Date.toJSON (a Go time.Time encodes as RFC 3339 with
// nanoseconds; it round-trips equal either way, which nested-date checks).
func TestCodecVectors(t *testing.T) {
	var f struct {
		Cases []struct {
			Tag     string
			Encoded json.RawMessage
			Error   string
		}
	}
	loadVector(t, "codec.json", &f)
	seen := 0
	for _, c := range f.Cases {
		switch c.Tag {
		case "function":
			_, err := encodeValue(func() {})
			if !errors.Is(err, ErrNonRetriable) || encodeError(err).Code != c.Error {
				t.Errorf("function: %v", err)
			}
			seen++
		case "undefined":
			b, err := encodeValue(nil)
			if err != nil || string(b) != string(c.Encoded) {
				t.Errorf("undefined: %s %v", b, err)
			}
			seen++
		case "nested-date":
			var want struct {
				At time.Time `json:"at"`
			}
			if err := json.Unmarshal(c.Encoded, &want); err != nil {
				t.Fatal(err)
			}
			b, err := encodeValue(want)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeInto[struct {
				At time.Time `json:"at"`
			}](b)
			if err != nil || !got.At.Equal(want.At) {
				t.Errorf("nested-date: %v %v", got, err)
			}
			seen++
		case "bigint", "object-with-undefined-field", "array-with-undefined", "date-epoch":
		default:
			t.Errorf("unknown codec tag %q: map it to Go or skip it with a reason", c.Tag)
		}
	}
	if seen != 3 {
		t.Fatalf("%d cases checked", seen)
	}
}

func TestEncodeValueKeepsKeyOrderAndNUL(t *testing.T) {
	raw := json.RawMessage(`{"b":1,"a":"x\u0000y<&>","c":{"z":[2,1],"y":null}}`)
	b, err := encodeValue(raw)
	if err != nil || string(b) != `{"b":1,"a":"x\u0000y<&>","c":{"z":[2,1],"y":null}}` {
		t.Fatalf("%s %v", b, err)
	}
}

func TestUUIDv4(t *testing.T) {
	id, err := newUUIDv4(zeroReader{})
	if err != nil || id != "00000000-0000-4000-8000-000000000000" {
		t.Fatalf("%s %v", id, err)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
