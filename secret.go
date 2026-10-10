package workflows

import (
	"fmt"
	"log/slog"
)

// secret holds a credential; every way of printing, logging or marshalling it redacts it.
type secret string

const redacted = "[redacted]"

func (secret) String() string               { return redacted }
func (secret) GoString() string             { return redacted }
func (secret) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte(redacted)) }
func (secret) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }
func (secret) MarshalText() ([]byte, error) { return []byte(redacted), nil }
func (secret) LogValue() slog.Value         { return slog.StringValue(redacted) }
func (s secret) reveal() string             { return string(s) }
