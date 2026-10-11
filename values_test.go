package workflows

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// The codec vectors name JavaScript values by tag; these are their Go counterparts.
//
// Dates: JavaScript encodes a Date as toISOString, always three fractional digits
// ("1970-01-01T00:00:00.000Z"). Go's encoding/json writes a time.Time as RFC 3339 with the
// fractional seconds it has, trailing zeros trimmed ("1970-01-01T00:00:00Z"). Both are RFC 3339
// for the same instant, and a Go step's live result equals its replayed one either way (the
// result is decoded from the recorded JSON). So the date cases check the instant, Go's exact
// string, and that formatting with JavaScript's layout (what Now records) gives the vector's.
//
// Skipped, with no Go counterpart: bigint (Go integers are JSON numbers), and
// object-with-undefined-field and array-with-undefined (Go has no undefined).
func TestCodecVectors(t *testing.T) {
	var f struct {
		Cases []struct {
			Tag     string
			Encoded json.RawMessage
			Error   string
		}
	}
	loadVector(t, "codec.json", &f)
	checkDate := func(tag, vector, goString string) {
		t.Helper()
		want, err := time.Parse(time.RFC3339Nano, vector)
		if err != nil {
			t.Fatalf("%s: %v", tag, err)
		}
		b, err := encodeValue(want)
		if err != nil || string(b) != `"`+goString+`"` {
			t.Errorf("%s: Go encodes %s (%v), want %q", tag, b, err, goString)
		}
		got, err := decodeInto[time.Time](b)
		if err != nil || !got.Equal(want) {
			t.Errorf("%s: round trip %v (%v), want %v", tag, got, err, want)
		}
		if js := want.UTC().Format(isoMillis); js != vector {
			t.Errorf("%s: JavaScript's layout gives %s, want %s", tag, js, vector)
		}
	}
	seen := 0
	for _, c := range f.Cases {
		switch c.Tag {
		case "function":
			_, err := encodeValue(func() {})
			if !errors.Is(err, ErrNonRetriable) || encodeError(err).Code != c.Error {
				t.Errorf("function: %v", err)
			}
		case "undefined":
			if b, err := encodeValue(nil); err != nil || string(b) != string(c.Encoded) {
				t.Errorf("undefined: %s %v", b, err)
			}
		case "date-epoch":
			var v string
			_ = json.Unmarshal(c.Encoded, &v)
			checkDate(c.Tag, v, "1970-01-01T00:00:00Z")
		case "nested-date":
			var v struct {
				At string `json:"at"`
			}
			_ = json.Unmarshal(c.Encoded, &v)
			checkDate(c.Tag, v.At, "2026-10-09T12:00:00Z")
		case "bigint", "object-with-undefined-field", "array-with-undefined":
			continue
		default:
			t.Errorf("unknown codec tag %q: map it to Go or skip it with a reason", c.Tag)
			continue
		}
		seen++
	}
	if seen != 4 {
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
