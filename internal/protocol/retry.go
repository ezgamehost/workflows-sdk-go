package protocol

import (
	"math"
	"time"
)

// Backoff is the delay before attempt+1 = min(maxMs, floor(initialMs * multiplier^(attempt-1))).
// Attempts count from 1.
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	m := p.Multiplier
	if math.IsNaN(m) || m < 1 {
		m = 1
	}
	ms := math.Floor(float64(p.InitialMs) * math.Pow(m, float64(attempt-1)))
	if math.IsInf(ms, 0) || ms > float64(p.MaxMs) {
		ms = float64(p.MaxMs)
	}
	return time.Duration(ms) * time.Millisecond
}

// Validate checks the policy against the engine's bounds: maxAttempts 1–100, initialMs ≥ 0,
// maxMs 0–86,400,000, multiplier 1–10.
func (p RetryPolicy) Validate() error {
	switch {
	case p.MaxAttempts < 1 || p.MaxAttempts > MaxRetryAttempts:
		return invalid("retry.maxAttempts %d is outside 1-%d", p.MaxAttempts, MaxRetryAttempts)
	case p.InitialMs < 0:
		return invalid("retry.initialMs %d is negative", p.InitialMs)
	case p.MaxMs < 0 || p.MaxMs > MaxRetryMs:
		return invalid("retry.maxMs %d is outside 0-%d", p.MaxMs, MaxRetryMs)
	case math.IsNaN(p.Multiplier) || p.Multiplier < 1 || p.Multiplier > MaxMultiplier:
		return invalid("retry.multiplier %v is outside 1-%d", p.Multiplier, MaxMultiplier)
	}
	return nil
}
