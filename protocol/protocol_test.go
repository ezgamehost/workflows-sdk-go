package protocol

import (
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestTruncateUTF8NeverSplitsACharacter(t *testing.T) {
	s := strings.Repeat("é", 5000) // 10,000 bytes
	got := TruncateUTF8(s, 8192)
	if len(got) > 8192 || !strings.HasPrefix(s, got) || len(got)%2 != 0 {
		t.Fatalf("len %d", len(got))
	}
	if got := TruncateUTF8("a\xffb", 10); !utf8.ValidString(got) {
		t.Fatalf("invalid UTF-8 kept: %q", got)
	}
}

func TestParseSignatureLimits(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	if _, err := ParseSignature(strings.Repeat("v1=", 400), now); err == nil {
		t.Fatal("an over-long header was accepted")
	}
	parts := []string{"t=1800000000"}
	for i := 0; i < 9; i++ {
		parts = append(parts, "v1="+strings.Repeat("a", 64))
	}
	if _, err := ParseSignature(strings.Join(parts, ","), now); err == nil {
		t.Fatal("nine signatures were accepted")
	}
	if _, err := ParseSignature("t=+123,v1="+strings.Repeat("a", 64), now); err == nil {
		t.Fatal("t=+123 was accepted")
	}
	reason := func(h string) string {
		_, err := ParseSignature(h, now)
		var se *SignatureError
		if errors.As(err, &se) {
			return se.Reason
		}
		return "ok"
	}
	if r := reason("t=1799999000,v1=" + strings.Repeat("a", 64)); r != "expired" {
		t.Fatalf("an old signature is %s", r)
	}
	if r := reason("t=1800000999,v1=" + strings.Repeat("a", 64)); r != "expired" {
		t.Fatalf("a future-dated signature is %s", r)
	}
	if r := reason("t=1800000300,v1=" + strings.Repeat("a", 64)); r != "ok" {
		t.Fatalf("a signature 300 s ahead is %s", r)
	}
}

func TestDecodeSecretStrict(t *testing.T) {
	for _, s := range []string{"", "whsec_", "sk_" + strings.Repeat("A", 43), "whsec_" + strings.Repeat("A", 42) + "B"} {
		if _, err := DecodeSecret(s); err == nil {
			t.Errorf("%q accepted", s)
		}
	}
}

func TestBackoffNeverOverflows(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 100, InitialMs: 86_400_000, MaxMs: 86_400_000, Multiplier: 10}
	if d := p.Backoff(100); d != 86_400_000*time.Millisecond {
		t.Fatalf("%v", d)
	}
}

func TestValidateCaps(t *testing.T) {
	sid := StepID("a", 0)
	until := time.Unix(1791547200, 0)
	for name, c := range map[string]Command{
		"name 257 bytes":     {Type: CommandStepCompleted, StepID: sid, Name: strings.Repeat("n", 257)},
		"name 129 é":         {Type: CommandStepCompleted, StepID: sid, Name: strings.Repeat("é", 129)},
		"NUL in name":        {Type: CommandStepCompleted, StepID: sid, Name: "a\x00"},
		"value over 64 KiB":  {Type: CommandValueRecorded, StepID: sid, Name: "a", Value: []byte(`"` + strings.Repeat("v", 64<<10) + `"`)},
		"error depth 9":      {Type: CommandRunFailed, Error: chain(9)},
		"message over 8 KiB": {Type: CommandRunFailed, Error: &WireError{Code: "user", Name: "E", Message: strings.Repeat("m", 8193)}},
		"custom code":        {Type: CommandRunFailed, Error: &WireError{Code: "http.418", Name: "E", Message: "m"}},
		"token elsewhere":    {Type: CommandSleep, StepID: sid, Name: "a", Until: &until, Token: "201.AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"},
		"duplicate planned":  {Type: CommandPlanParallel, Steps: []PlannedStep{{StepID: sid, Name: "a"}, {StepID: sid, Name: "a"}}},
		"callback timeout 0": {Type: CommandCallbackCreated, StepID: sid, Name: "a", Token: "201.AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"},
	} {
		if err := c.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := (Command{Type: CommandRunFailed, Error: chain(8)}).Validate(); err != nil {
		t.Errorf("depth 8: %v", err)
	}
	if err := (InvocationResult{Deployment: strings.Repeat("d", 257)}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Error("a 257-byte deployment validated")
	}
	if got := CleanDeployment(strings.Repeat("é", 200) + "\x00"); len(got) > MaxDeploymentLen || strings.Contains(got, "\x00") {
		t.Errorf("CleanDeployment: %d bytes", len(got))
	}
}

func chain(n int) *WireError {
	var e *WireError
	for i := 0; i < n; i++ {
		e = &WireError{Code: "user", Name: "E", Message: "m", Cause: e}
	}
	return e
}
