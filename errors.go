package workflows

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ezgamehost/workflows-sdk-go/internal/jsonx"
	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// Error is a workflow error with a code and JSON data: what a recorded failure is rebuilt as on
// replay (so errors.As and errors.Is work the same after a replay), and what to return for a
// custom code. Codes on the wire are "user" or the platform's "workflows.*": any other Code is
// sent as "user", with the custom code as the name (when Name is empty) and as data.code (when
// Data is empty or an object without a code).
type Error struct {
	Code    string
	Name    string
	Message string
	Data    json.RawMessage
	Stack   string
	Cause   error
}

func (e *Error) Error() string { return e.Message }

// Unwrap returns the cause.
func (e *Error) Unwrap() error { return e.Cause }

// Is matches the platform sentinels by code: errors.Is(err, workflows.ErrCancelled).
func (e *Error) Is(target error) bool {
	if s, ok := target.(*sentinel); ok {
		return e.Code == s.code
	}
	return false
}

type sentinel struct{ code, name string }

func (s *sentinel) Error() string { return s.code }

// The platform's errors, matched by code with errors.Is.
var (
	ErrCancelled         error = &sentinel{"workflows.cancelled", "CancelledError"}
	ErrStepTimeout       error = &sentinel{"workflows.step_timeout", "StepTimeout"}
	ErrCallbackTimeout   error = &sentinel{"workflows.callback_timeout", "CallbackTimeout"}
	ErrChildFailed       error = &sentinel{"workflows.child_failed", "ChildFailed"}
	ErrNonRetriable      error = &sentinel{"workflows.non_retriable", "NonRetriableError"}
	ErrOutputTooLarge    error = &sentinel{"workflows.output_too_large", "OutputTooLarge"}
	ErrDuplicateStepName error = &sentinel{"workflows.duplicate_step_name", "DuplicateStepName"}
)

// platformCodes are the codes that go on the wire as they are; any other code becomes "user".
var platformCodes = map[string]bool{
	"workflows.callback_timeout": true, "workflows.cancelled": true, "workflows.child_failed": true,
	"workflows.step_timeout": true, "workflows.invocation_lost": true, "workflows.non_retriable": true,
	"workflows.output_too_large": true, "workflows.duplicate_step_name": true, "workflows.workflow_not_found": true,
	"workflows.invocation_failed": true, "workflows.timed_out": true,
}

type nonRetriable struct{ err error }

func (n *nonRetriable) Error() string        { return n.err.Error() }
func (n *nonRetriable) Unwrap() error        { return n.err }
func (n *nonRetriable) Is(target error) bool { return target == ErrNonRetriable }

// NonRetriable marks err so the step fails at once, without retries (code
// workflows.non_retriable).
func NonRetriable(err error) error {
	if err == nil {
		err = errors.New("non-retriable error")
	}
	return &nonRetriable{err}
}

type retryAfter struct {
	err   error
	delay time.Duration
}

func (r *retryAfter) Error() string { return r.err.Error() }
func (r *retryAfter) Unwrap() error { return r.err }

// RetryAfter retries the step after d (at most 24 hours), for example after an upstream 429.
func RetryAfter(err error, d time.Duration) error {
	if err == nil {
		err = errors.New("retry later")
	}
	return &retryAfter{err, d}
}

// argumentError is what a misused API returns: code "user", which is what the conformance
// scenarios check. The name "TypeError" matches the TypeScript SDK for readers.
func argumentError(format string, args ...any) error {
	return &Error{Code: "user", Name: "TypeError", Message: fmt.Sprintf(format, args...)}
}

func newError(code, name, format string, args ...any) *Error {
	return &Error{Code: code, Name: name, Message: fmt.Sprintf(format, args...)}
}

// codeAndName: the code, name and data of err itself (not of what it wraps).
func codeAndName(err error) (string, string, json.RawMessage) {
	switch e := err.(type) {
	case *Error:
		return e.Code, e.Name, e.Data
	case *nonRetriable:
		_, name, data := codeAndName(e.err)
		if name == "" || name == "Error" {
			name = "NonRetriableError"
		}
		return "workflows.non_retriable", name, data
	case *retryAfter:
		code, name, data := codeAndName(e.err)
		if name == "" || name == "Error" {
			name = "RetryAfterError"
		}
		return code, name, data
	case *sentinel:
		return e.code, e.name, nil
	}
	return "user", typeName(err), nil
}

// typeName is "Error" for the standard library's errors and unexported types, else the Go type
// name (an exported custom error type names itself).
func typeName(err error) string {
	t := reflect.TypeOf(err)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	name := t.Name()
	r, _ := utf8.DecodeRuneInString(name)
	if name == "" || !unicode.IsUpper(r) || t.PkgPath() == "errors" || t.PkgPath() == "fmt" {
		return "Error"
	}
	return name
}

// encodeError is the wire shape of anything a workflow or step returns. Names are cut to 256
// bytes, messages and stacks to 8 KiB, data over 64 KiB is dropped, and the chain holds at most 8
// errors (the top one and 7 causes): the engine refuses anything past these limits.
func encodeError(err error) *protocol.WireError { return encodeErrorDepth(err, 0) }

func encodeErrorDepth(err error, depth int) *protocol.WireError {
	code, name, data := codeAndName(err)
	if len(data) > 0 {
		data = jsonx.Compact(data)
	}
	if code != "user" && !platformCodes[code] {
		if name == "" || name == "WorkflowError" {
			name = code
		}
		data = withCode(data, code)
		code = "user"
	}
	if name == "" {
		name = "Error"
	}
	w := &protocol.WireError{
		Code:    code,
		Name:    protocol.TruncateUTF8(name, protocol.MaxErrorNameBytes),
		Message: protocol.TruncateUTF8(err.Error(), protocol.MaxErrorTextBytes),
	}
	if len(data) > 0 && len(data) <= protocol.MaxErrorDataBytes {
		w.Data = data
	}
	if e, ok := err.(*Error); ok && e.Stack != "" {
		w.Stack = protocol.TruncateUTF8(e.Stack, protocol.MaxStackBytes)
	}
	if cause := causeOf(err); cause != nil && depth+1 < protocol.MaxErrorDepth {
		w.Cause = encodeErrorDepth(cause, depth+1)
	}
	return w
}

// causeOf is the error's cause on the wire: an Error's Cause, or what a wrapper wraps (the first
// of several).
func causeOf(err error) error {
	switch e := err.(type) {
	case *Error:
		return e.Cause
	case *nonRetriable:
		return causeOf(e.err)
	case *retryAfter:
		return causeOf(e.err)
	case *sentinel:
		return nil
	}
	if u, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range u.Unwrap() {
			if e != nil {
				return e
			}
		}
		return nil
	}
	return errors.Unwrap(err)
}

// withCode adds code to a JSON object without one; absent data becomes {"code": code}.
func withCode(data json.RawMessage, code string) json.RawMessage {
	if len(data) == 0 {
		b, _ := jsonx.Marshal(map[string]string{"code": code})
		return b
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(data, &m) != nil || m == nil {
		return data
	}
	if _, has := m["code"]; has {
		return data
	}
	m["code"], _ = jsonx.Marshal(code)
	b, _ := jsonx.Marshal(m)
	return b
}

// decodeError rebuilds a recorded failure as an *Error; encodeError(decodeError(w)) gives w back.
func decodeError(w *protocol.WireError) *Error {
	if w == nil {
		return &Error{Code: "user", Name: "Error", Message: "Step failed"}
	}
	e := &Error{Code: w.Code, Name: w.Name, Message: w.Message, Data: w.Data, Stack: w.Stack}
	if w.Cause != nil {
		e.Cause = decodeError(w.Cause)
	}
	return e
}

// isNonRetriable: the step's failure goes on the wire with a code that retrying won't fix.
func isNonRetriable(w *protocol.WireError) bool {
	return w.Code == "workflows.non_retriable" || w.Code == "workflows.output_too_large"
}

// walkErrors calls fn on err and everything it wraps, depth first, until fn returns true.
func walkErrors(err error, fn func(error) bool) bool {
	for depth := 0; err != nil && depth < 64; depth++ {
		if fn(err) {
			return true
		}
		if u, ok := err.(interface{ Unwrap() []error }); ok {
			for _, e := range u.Unwrap() {
				if walkErrors(e, fn) {
					return true
				}
			}
			return false
		}
		err = errors.Unwrap(err)
	}
	return false
}
