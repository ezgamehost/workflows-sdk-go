package workflows

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

func loadVector(t *testing.T, name string, v any) {
	t.Helper()
	b, err := os.ReadFile("testdata/vectors/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

func jsonEqual(t *testing.T, a, b any) bool {
	t.Helper()
	var x, y any
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	_ = json.Unmarshal(ab, &x)
	_ = json.Unmarshal(bb, &y)
	return reflect.DeepEqual(x, y)
}

func TestErrorVectorsRoundTrip(t *testing.T) {
	var f struct {
		Cases []struct{ Wire protocol.WireError }
	}
	loadVector(t, "errors.json", &f)
	if len(f.Cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range f.Cases {
		got := encodeError(decodeError(&c.Wire))
		if !jsonEqual(t, got, c.Wire) {
			t.Errorf("round trip:\n got %s\nwant %s", show(got), show(c.Wire))
		}
	}
	// A replayed generic error keeps its recorded name ("Error", "TypeError").
	w := &protocol.WireError{Code: "user", Name: "TypeError", Message: "inner", Data: json.RawMessage(`{"k":1}`), Cause: &protocol.WireError{Code: "user", Name: "Error", Message: "root"}}
	if got := encodeError(decodeError(w)); !jsonEqual(t, got, w) {
		t.Errorf("got %s", show(got))
	}
}

func TestErrorsMatchSentinelsByCode(t *testing.T) {
	for code, s := range map[string]error{
		"workflows.cancelled": ErrCancelled, "workflows.step_timeout": ErrStepTimeout, "workflows.callback_timeout": ErrCallbackTimeout,
		"workflows.child_failed": ErrChildFailed, "workflows.non_retriable": ErrNonRetriable, "workflows.output_too_large": ErrOutputTooLarge,
		"workflows.duplicate_step_name": ErrDuplicateStepName,
	} {
		err := fmt.Errorf("wrapped: %w", decodeError(&protocol.WireError{Code: code, Name: "X", Message: "m"}))
		if !errors.Is(err, s) {
			t.Errorf("%s doesn't match its sentinel", code)
		}
	}
	if !errors.Is(NonRetriable(errors.New("x")), ErrNonRetriable) {
		t.Error("NonRetriable doesn't match ErrNonRetriable")
	}
}

func TestEncodeErrorShapes(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{errors.New("plain"), `{"code":"user","name":"Error","message":"plain"}`},
		{NonRetriable(errors.New("nope")), `{"code":"workflows.non_retriable","name":"NonRetriableError","message":"nope"}`},
		{RetryAfter(errors.New("slow down"), 1), `{"code":"user","name":"RetryAfterError","message":"slow down"}`},
		{&Error{Code: "http.418", Message: "teapot", Data: json.RawMessage(`{"status":418}`)}, `{"code":"user","name":"http.418","message":"teapot","data":{"code":"http.418","status":418}}`},
		{&Error{Code: "billing.declined", Message: "no"}, `{"code":"user","name":"billing.declined","message":"no","data":{"code":"billing.declined"}}`},
		{&Error{Code: "user", Name: "E", Message: "nul \x00 kept"}, `{"code":"user","name":"E","message":"nul \u0000 kept"}`},
		{ErrCancelled, `{"code":"workflows.cancelled","name":"CancelledError","message":"workflows.cancelled"}`},
		{fmt.Errorf("outer: %w", &Error{Code: "user", Name: "Inner", Message: "inner"}), `{"code":"user","name":"Error","message":"outer: inner","cause":{"code":"user","name":"Inner","message":"inner"}}`},
		{&MyError{}, `{"code":"user","name":"MyError","message":"mine"}`},
	}
	for _, c := range cases {
		if got := show(encodeError(c.err)); got != c.want {
			t.Errorf("%v:\n got %s\nwant %s", c.err, got, c.want)
		}
	}
}

type MyError struct{}

func (*MyError) Error() string { return "mine" }

func TestEncodeErrorLimits(t *testing.T) {
	// A deep chain is cut to 8 levels in all: the error and 7 causes.
	var err error = errors.New("root")
	for i := 0; i < 20; i++ {
		err = &Error{Code: "user", Name: "E", Message: fmt.Sprint(i), Cause: err}
	}
	w := encodeError(err)
	depth := 0
	for e := w; e != nil; e = e.Cause {
		depth++
	}
	if depth != protocol.MaxErrorDepth {
		t.Fatalf("depth %d, want %d", depth, protocol.MaxErrorDepth)
	}
	if err := (protocol.Command{Type: protocol.CommandRunFailed, Error: w}).Validate(); err != nil {
		t.Fatal(err)
	}
	big := &Error{Code: "user", Name: strings.Repeat("n", 300), Message: strings.Repeat("é", 5000), Stack: strings.Repeat("é", 5000),
		Data: json.RawMessage(`"` + strings.Repeat("d", 64<<10) + `"`)}
	w = encodeError(big)
	if len(w.Name) != 256 || len(w.Message) > 8192 || len(w.Stack) > 8192 || w.Data != nil {
		t.Fatalf("name %d message %d stack %d data %d", len(w.Name), len(w.Message), len(w.Stack), len(w.Data))
	}
	if err := w.Validate(); err != nil {
		t.Fatal(err)
	}
	// Invalid UTF-8 never reaches the wire.
	w = encodeError(errors.New("bad \xff byte"))
	if !strings.Contains(w.Message, "\uFFFD") {
		t.Fatalf("message %q", w.Message)
	}
}
