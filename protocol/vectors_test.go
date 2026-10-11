package protocol

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func load(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "testdata", "vectors", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func TestStepIDVectors(t *testing.T) {
	var f struct {
		Cases []struct {
			Name       string
			Occurrence int
			StepID     string
		}
	}
	load(t, "step-ids.json", &f)
	if len(f.Cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range f.Cases {
		if got := StepID(c.Name, c.Occurrence); got != c.StepID {
			t.Errorf("StepID(%q,%d) = %s, want %s", c.Name, c.Occurrence, got, c.StepID)
		}
	}
}

func TestSignatureVectors(t *testing.T) {
	var f struct {
		Sign []struct {
			Secrets []string
			T       int64
			Body    string
			Header  string
		}
		Verify []struct {
			Name    string
			Secrets []string
			Header  string
			Body    string
			Now     int64
			Result  string // "ok" | "invalid" | "expired"
		}
	}
	load(t, "signatures.json", &f)
	if len(f.Sign) == 0 || len(f.Verify) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range f.Sign {
		var keys [][]byte
		for _, s := range c.Secrets {
			k, err := DecodeSecret(s)
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, k)
		}
		if got := Sign(keys, time.Unix(c.T, 0), []byte(c.Body)); got != c.Header {
			t.Errorf("Sign = %s, want %s", got, c.Header)
		}
	}
	for i, c := range f.Verify {
		var keys [][]byte
		for _, s := range c.Secrets {
			k, err := DecodeSecret(s)
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, k)
		}
		err := Verify(c.Header, keys, []byte(c.Body), time.Unix(c.Now, 0))
		got := "ok"
		var se *SignatureError
		if errors.As(err, &se) {
			got = se.Reason
		} else if err != nil {
			got = "error"
		}
		if got != c.Result {
			t.Errorf("verify[%d] %s: %s, want %s", i, c.Name, got, c.Result)
		}
	}
}

func TestInvocationTokenVectors(t *testing.T) {
	var f struct {
		Valid   []struct{ Token string }
		Invalid []struct{ Token, Why string }
	}
	load(t, "invocation-tokens.json", &f)
	for _, c := range f.Valid {
		if !IsInvocationToken(c.Token) {
			t.Errorf("valid token refused: %s", c.Token)
		}
	}
	for _, c := range f.Invalid {
		if IsInvocationToken(c.Token) {
			t.Errorf("invalid token accepted (%s): %s", c.Why, c.Token)
		}
	}
}

func TestRunIDShardVectors(t *testing.T) {
	var f struct {
		Valid []struct {
			Shard int
			ID    string
		}
		Invalid []struct{ ID string }
	}
	load(t, "run-ids.json", &f)
	for _, c := range f.Valid {
		if s, ok := ShardOf(c.ID); !ok || int(s) != c.Shard {
			t.Errorf("ShardOf(%s) = %d %v, want %d", c.ID, s, ok, c.Shard)
		}
	}
	for _, c := range f.Invalid {
		if _, ok := ShardOf(c.ID); ok {
			t.Errorf("ShardOf(%s) accepted", c.ID)
		}
	}
	// Derived (v8) run IDs carry their shard the same way.
	var d struct {
		Child, Scheduled, Triggered []struct {
			ID    string
			Shard int
		}
	}
	load(t, "derived-ids.json", &d)
	for _, set := range [][]struct {
		ID    string
		Shard int
	}{d.Child, d.Scheduled, d.Triggered} {
		for _, c := range set {
			if s, ok := ShardOf(c.ID); !ok || int(s) != c.Shard {
				t.Errorf("ShardOf(derived %s) = %d %v, want %d", c.ID, s, ok, c.Shard)
			}
		}
	}
}

func TestBackoffVectors(t *testing.T) {
	var f struct {
		Policy   RetryPolicy
		DelaysMs []int64
	}
	load(t, "backoff.json", &f)
	for i, want := range f.DelaysMs {
		if got := f.Policy.Backoff(i + 1).Milliseconds(); got != want {
			t.Errorf("Backoff(%d) = %d, want %d", i+1, got, want)
		}
	}
	if f.Policy != DefaultRetry {
		t.Errorf("backoff.json's policy %+v isn't DefaultRetry %+v", f.Policy, DefaultRetry)
	}
}

func TestCallbackTokenVectors(t *testing.T) {
	var f struct {
		Valid []struct {
			Shard     uint16 `json:"shard"`
			SecretHex string `json:"secretHex"`
			Token     string `json:"token"`
			HashHex   string `json:"hashHex"`
		} `json:"valid"`
		Invalid []struct{ Token, Why string } `json:"invalid"`
	}
	load(t, "callback-tokens.json", &f)
	if len(f.Valid) == 0 || len(f.Invalid) == 0 {
		t.Fatal("callback-tokens.json is incomplete")
	}
	for _, c := range f.Valid {
		raw, _ := hex.DecodeString(c.SecretHex)
		var secret [32]byte
		copy(secret[:], raw)
		tok, err := NewCallbackToken(c.Shard, bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if tok.Secret != secret || tok.String() != c.Token {
			t.Errorf("token %s, want %s", tok.String(), c.Token)
		}
		if h := tok.Hash(); hex.EncodeToString(h[:]) != c.HashHex {
			t.Errorf("hash %x, want %s", h, c.HashHex)
		}
		if p, err := ParseCallbackToken(c.Token); err != nil || p != tok {
			t.Errorf("ParseCallbackToken(%s) = %v, %v", c.Token, p, err)
		}
	}
	for _, c := range f.Invalid {
		if _, err := ParseCallbackToken(c.Token); err == nil {
			t.Errorf("invalid callback token accepted (%s): %s", c.Why, c.Token)
		}
	}
}

// TestValidateMessageVectors: the SDK's Validate refuses exactly what the engine's does, on the
// engine's own message vectors.
func TestValidateMessageVectors(t *testing.T) {
	var m struct {
		Command          struct{ Valid, Invalid []json.RawMessage }
		Checkpoint       struct{ Valid, Invalid []json.RawMessage }
		InvocationResult struct{ Valid []json.RawMessage }
		PollRequest      struct{ Valid, Invalid []json.RawMessage }
	}
	load(t, "messages.json", &m)
	if len(m.Command.Valid) == 0 || len(m.Command.Invalid) == 0 || len(m.PollRequest.Valid) == 0 {
		t.Fatal("messages.json is incomplete")
	}
	for _, raw := range m.Command.Valid {
		var c Command
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatal(err)
		}
		if err := c.Validate(); err != nil {
			t.Errorf("valid %s: %v", raw, err)
		}
	}
	for _, raw := range m.Command.Invalid {
		var c Command
		if err := json.Unmarshal(raw, &c); err == nil {
			if err := c.Validate(); !errors.Is(err, ErrInvalid) {
				t.Errorf("invalid %s validated: %v", raw, err)
			}
		}
	}
	for _, raw := range m.Checkpoint.Valid {
		var c Checkpoint
		_ = json.Unmarshal(raw, &c)
		if err := c.Validate(); err != nil {
			t.Errorf("valid %s: %v", raw, err)
		}
	}
	for _, raw := range m.Checkpoint.Invalid {
		var c Checkpoint
		_ = json.Unmarshal(raw, &c)
		if err := c.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("invalid %s validated: %v", raw, err)
		}
	}
	for _, raw := range m.InvocationResult.Valid {
		var r InvocationResult
		_ = json.Unmarshal(raw, &r)
		if err := r.Validate(); err != nil {
			t.Errorf("valid %s: %v", raw, err)
		}
	}
	for _, raw := range m.PollRequest.Valid {
		var r PollRequest
		_ = json.Unmarshal(raw, &r)
		if err := r.Validate(); err != nil {
			t.Errorf("valid %s: %v", raw, err)
		}
	}
	for _, raw := range m.PollRequest.Invalid {
		var r PollRequest
		_ = json.Unmarshal(raw, &r)
		if err := r.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("invalid %s validated: %v", raw, err)
		}
	}
}

func TestInvocationVectorsDecode(t *testing.T) {
	var m struct {
		Invocation struct{ Valid []json.RawMessage }
	}
	load(t, "messages.json", &m)
	for _, raw := range m.Invocation.Valid {
		var inv Invocation
		if err := json.Unmarshal(raw, &inv); err != nil || inv.Kind == "" || inv.Deadline.IsZero() {
			t.Errorf("%s: %v", raw, err)
		}
	}
	var u struct {
		Cases []struct {
			Kind       string
			Invocation json.RawMessage
		}
	}
	load(t, "unknown-fields.json", &u)
	if len(u.Cases) == 0 {
		t.Fatal("no unknown-fields cases")
	}
	for _, c := range u.Cases {
		var inv Invocation
		if err := json.Unmarshal(c.Invocation, &inv); err != nil || inv.Kind != c.Kind {
			t.Errorf("%s: %v %s", c.Kind, err, inv.Kind)
		}
	}
}
