package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Shards is the number of engine shards; run IDs and tokens carry one in 0–1023.
const Shards = 1024

// ShardOf reads the 10-bit shard from a v7 or v8 run ID (rand_a = 0b00 + shard).
func ShardOf(id string) (uint16, bool) {
	if !uuidRe.MatchString(id) {
		return 0, false
	}
	b, _ := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	version := b[6] >> 4
	if (version != 7 && version != 8) || b[8]&0xc0 != 0x80 || b[6]&0x0c != 0 {
		return 0, false
	}
	return uint16(b[6]&0x03)<<8 | uint16(b[7]), true
}

// IsInvocationToken: "<shard 3 hex>.<run UUID>.<inv_…>.<43 base64url>", the shard being the
// run's. SDKs pass tokens through; they check the shape only to fail fast.
func IsInvocationToken(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 4 || len(parts[0]) != 3 || strings.ToLower(parts[0]) != parts[0] || !invocationIDRe.MatchString(parts[2]) {
		return false
	}
	shard, err := strconv.ParseUint(parts[0], 16, 16)
	if err != nil {
		return false
	}
	run, ok := ShardOf(parts[1])
	if !ok || uint64(run) != shard {
		return false
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(parts[3])
	return err == nil && len(b) == 32
}

// ErrMalformedCallbackToken: not "<shard 3 lowercase hex>.<43 base64url>", or a shard ≥ 1024.
var ErrMalformedCallbackToken = errors.New("protocol: malformed callback token")

// CallbackToken is the only credential of a public callback URL: "<shard 3 hex>." + 43
// base64url characters. The SDK mints it with the run's shard; the engine stores its Hash.
type CallbackToken struct {
	Shard  uint16
	Secret [32]byte
}

// NewCallbackToken reads the 32-byte secret from rnd (crypto/rand when nil).
func NewCallbackToken(shard uint16, rnd io.Reader) (CallbackToken, error) {
	if shard >= Shards {
		return CallbackToken{}, fmt.Errorf("%w: shard %d", ErrMalformedCallbackToken, shard)
	}
	if rnd == nil {
		rnd = rand.Reader
	}
	t := CallbackToken{Shard: shard}
	if _, err := io.ReadFull(rnd, t.Secret[:]); err != nil {
		return CallbackToken{}, err
	}
	return t, nil
}

// String is the token's wire form (47 characters). It's a credential: never log it.
func (t CallbackToken) String() string {
	return fmt.Sprintf("%03x.%s", t.Shard, base64.RawURLEncoding.EncodeToString(t.Secret[:]))
}

// Hash is SHA-256 of the secret.
func (t CallbackToken) Hash() [32]byte { return sha256.Sum256(t.Secret[:]) }

// ParseCallbackToken parses the wire form strictly (canonical base64url, lowercase hex).
func ParseCallbackToken(s string) (CallbackToken, error) {
	shardHex, secret, ok := strings.Cut(s, ".")
	if !ok || len(shardHex) != 3 || shardHex != strings.ToLower(shardHex) {
		return CallbackToken{}, ErrMalformedCallbackToken
	}
	n, err := strconv.ParseUint(shardHex, 16, 16)
	if err != nil || n >= Shards {
		return CallbackToken{}, ErrMalformedCallbackToken
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(secret)
	if err != nil || len(raw) != 32 {
		return CallbackToken{}, ErrMalformedCallbackToken
	}
	t := CallbackToken{Shard: uint16(n)}
	copy(t.Secret[:], raw)
	return t, nil
}
