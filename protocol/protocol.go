// Package protocol is the part of the EZGH Workflows wire protocol an app may need on its own:
// the protocol versions and the EZGH-Signature on push invocations, for apps that verify
// signatures themselves (workflows.Handler already does). The rest of the wire model is the SDK's
// internal business.
package protocol

import (
	"time"

	iprotocol "github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// Version is the newest protocol version this SDK speaks.
const Version = iprotocol.Version

// Versions are the protocol versions this SDK declares.
func Versions() []string { return append([]string(nil), iprotocol.Versions...) }

const (
	// SignatureHeader carries a push invocation's signature: t=<unix seconds>,v1=<hex HMAC>,….
	SignatureHeader = iprotocol.SignatureHeader
	// MaxSignatureAgeSeconds: a signature more than 5 minutes from now, either way, is expired.
	MaxSignatureAgeSeconds = iprotocol.MaxSignatureAgeSeconds
	// SecretPrefix starts every signing secret.
	SecretPrefix = iprotocol.SecretPrefix
)

// ErrMalformedSecret: a signing secret isn't whsec_ and 43 base64url characters.
var ErrMalformedSecret = iprotocol.ErrMalformedSecret

// SignatureError is a refused signature: Reason is "invalid" or "expired".
type SignatureError = iprotocol.SignatureError

// ParsedSignature is a signature header that passed ParseSignature.
type ParsedSignature = iprotocol.ParsedSignature

// DecodeSecret turns a signing secret (whsec_ + 43 base64url characters) into its 32 key bytes.
func DecodeSecret(secret string) ([]byte, error) { return iprotocol.DecodeSecret(secret) }

// Sign is the EZGH-Signature header of body at t: one v1 per key.
func Sign(keys [][]byte, t time.Time, body []byte) string { return iprotocol.Sign(keys, t, body) }

// ParseSignature checks the header and its timestamp without the body, so a malformed or stale
// signature can be refused before the body is read.
func ParseSignature(header string, now time.Time) (ParsedSignature, error) {
	return iprotocol.ParseSignature(header, now)
}

// VerifyParsed checks a parsed signature against the body, in constant time.
func VerifyParsed(p ParsedSignature, keys [][]byte, body []byte) error {
	return iprotocol.VerifyParsed(p, keys, body)
}

// Verify is ParseSignature, then VerifyParsed.
func Verify(header string, keys [][]byte, body []byte, now time.Time) error {
	return iprotocol.Verify(header, keys, body, now)
}
