package protocol

import (
	"encoding/base64"
	"errors"
	"strings"
)

// SecretPrefix starts every signing secret.
const SecretPrefix = "whsec_"

// ErrMalformedSecret: a signing secret isn't whsec_ and 43 base64url characters.
var ErrMalformedSecret = errors.New("a signing secret is whsec_ and 43 base64url characters, as the API returned it")

// DecodeSecret: "whsec_" + 43 base64url characters → the 32 HMAC key bytes.
func DecodeSecret(secret string) ([]byte, error) {
	s := strings.TrimSpace(secret)
	if !strings.HasPrefix(s, SecretPrefix) || len(s) != len(SecretPrefix)+43 {
		return nil, ErrMalformedSecret
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(s[len(SecretPrefix):])
	if err != nil || len(b) != 32 {
		return nil, ErrMalformedSecret
	}
	return b, nil
}
