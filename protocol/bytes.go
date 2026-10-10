package protocol

import (
	"strings"
	"unicode/utf8"
)

// Limits, in bytes, exactly workflows-stack's internal/protocol/validate.go: the engine refuses
// a command past any of them, so the SDK never sends one.
const (
	MaxStepNameBytes  = 256
	MaxErrorNameBytes = 256 // what SDKs send (the engine allows up to 8 KiB)
	MaxErrorTextBytes = 8192
	MaxStackBytes     = 8192
	MaxErrorDataBytes = 64 << 10
	MaxErrorDepth     = 8
	MaxValueBytes     = 64 << 10
	MaxOutputBytes    = 1 << 20
	MaxDeploymentLen  = 256
	MaxRetryAttempts  = 100
	MaxRetryMs        = 86_400_000
	MaxMultiplier     = 10
	MaxPlannedSteps   = 1000
	MaxPollTasks      = 10
)

// TruncateUTF8 cuts s to at most n bytes without splitting a character. Invalid UTF-8 is
// replaced first, so the result is always valid UTF-8, which is what the engine stores.
func TruncateUTF8(s string, n int) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
