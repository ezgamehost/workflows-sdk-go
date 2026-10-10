package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

const (
	SignatureHeader          = "EZGH-Signature"
	MaxSignatureAgeSeconds   = 300
	MaxSignatures            = 8
	maxSignatureHeaderLength = 1024
)

// SignatureError is a refused signature: Reason is "invalid" or "expired".
type SignatureError struct{ Reason, Message string }

func (e *SignatureError) Error() string { return e.Message }

func mac(key []byte, t int64, body []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(strconv.FormatInt(t, 10) + "."))
	h.Write(body)
	return h.Sum(nil)
}

// Sign: t=<unix seconds>,v1=<hex HMAC-SHA256(key, "<t>." + body)>, one v1 per key.
func Sign(keys [][]byte, t time.Time, body []byte) string {
	ts := t.Unix()
	parts := []string{"t=" + strconv.FormatInt(ts, 10)}
	for _, k := range keys {
		parts = append(parts, "v1="+hex.EncodeToString(mac(k, ts, body)))
	}
	return strings.Join(parts, ",")
}

// ParsedSignature is a header that passed ParseSignature.
type ParsedSignature struct {
	T          int64
	Signatures [][]byte
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// ParseSignature checks the header and its timestamp without the body, so a malformed or stale
// signature is refused before the body is read: at most 1,024 bytes, at most 8 v1 values, and t
// within 300 s of now either way.
func ParseSignature(header string, now time.Time) (ParsedSignature, error) {
	bad := &SignatureError{"invalid", "the EZGH-Signature header is missing or malformed"}
	if header == "" || len(header) > maxSignatureHeaderLength {
		return ParsedSignature{}, bad
	}
	var p ParsedSignature
	haveT := false
	for _, raw := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(raw), "=")
		if !ok {
			return ParsedSignature{}, bad
		}
		switch {
		case k == "t":
			if len(v) == 0 || len(v) > 12 || !isDigits(v) {
				return ParsedSignature{}, bad
			}
			p.T, _ = strconv.ParseInt(v, 10, 64)
			haveT = true
		case k == "v1" && len(v) == 64:
			b, err := hex.DecodeString(strings.ToLower(v))
			if err != nil {
				continue
			}
			if len(p.Signatures) == MaxSignatures {
				return ParsedSignature{}, &SignatureError{"invalid", "the EZGH-Signature header has more than 8 signatures"}
			}
			p.Signatures = append(p.Signatures, b)
		}
	}
	if !haveT || len(p.Signatures) == 0 {
		return ParsedSignature{}, bad
	}
	age := now.Sub(time.Unix(p.T, 0))
	if age > MaxSignatureAgeSeconds*time.Second || age < -MaxSignatureAgeSeconds*time.Second {
		return ParsedSignature{}, &SignatureError{"expired", "the EZGH-Signature timestamp is more than 5 minutes from now"}
	}
	return p, nil
}

// VerifyParsed: one HMAC per key; every candidate compared in constant time.
func VerifyParsed(p ParsedSignature, keys [][]byte, body []byte) error {
	ok := false
	for _, k := range keys {
		want := mac(k, p.T, body)
		for _, got := range p.Signatures {
			if hmac.Equal(want, got) {
				ok = true
			}
		}
	}
	if !ok {
		return &SignatureError{"invalid", "the EZGH-Signature doesn't match any signing secret"}
	}
	return nil
}

// Verify = ParseSignature, then VerifyParsed.
func Verify(header string, keys [][]byte, body []byte, now time.Time) error {
	p, err := ParseSignature(header, now)
	if err != nil {
		return err
	}
	return VerifyParsed(p, keys, body)
}
