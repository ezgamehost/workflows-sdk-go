package workflows

import (
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ezgamehost/workflows-sdk-go/internal/jsonx"
	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// StepOption configures one step.
type StepOption func(*stepOpts) error

type stepOpts struct {
	id      string
	retry   *protocol.RetryPolicy
	timeout time.Duration
}

// WithID gives the step an explicit ID (^[A-Za-z0-9._:-]{1,128}$) instead of one derived from
// its name and occurrence. An explicit ID names one step for the whole run.
func WithID(id string) StepOption {
	return func(o *stepOpts) error {
		if !protocol.ValidStepIDOverride(id) {
			return argumentError("step IDs match ^[A-Za-z0-9._:-]{1,128}$; got %q", id)
		}
		o.id = id
		return nil
	}
}

// WithStepRetry sets the step's retry policy, used as given (start from DefaultRetry).
func WithStepRetry(p RetryPolicy) StepOption {
	return func(o *stepOpts) error {
		w, err := p.wire()
		if err != nil {
			return argumentError("%v", err)
		}
		o.retry = &w
		return nil
	}
}

// WithStepTimeout sets the step's timeout: 1 ms to 15 minutes.
func WithStepTimeout(d time.Duration) StepOption {
	return func(o *stepOpts) error {
		if d < time.Millisecond || d > MaxStepTimeout {
			return argumentError("a step timeout is 1 ms to 15 minutes, got %v", d)
		}
		o.timeout = d
		return nil
	}
}

func applyOpts(opts []StepOption) (stepOpts, error) {
	var o stepOpts
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&o); err != nil {
			return o, err
		}
	}
	return o, nil
}

func checkName(name string) error {
	if name == "" || len(name) > protocol.MaxStepNameBytes || strings.IndexByte(name, 0) >= 0 || !utf8.ValidString(name) {
		return argumentError("a step name is 1–256 bytes of UTF-8, without NUL")
	}
	return nil
}

// Run runs fn at least once, as a step, and records its JSON result: on replay the result is
// returned without running fn. The live result is decoded from the recorded JSON, so it equals
// what a replay returns. A failed attempt is retried by the step's policy; once it's exhausted,
// Run returns the error.
func Run[T any](ctx Context, name string, fn func(StepContext) (T, error), opts ...StepOption) (T, error) {
	var zero T
	b, err := branchOf(ctx)
	if err != nil {
		return zero, err
	}
	if err := checkName(name); err != nil {
		return zero, err
	}
	if fn == nil {
		return zero, argumentError("workflows.Run needs a function")
	}
	o, err := applyOpts(opts)
	if err != nil {
		return zero, err
	}
	retry := b.x.defaultRetry()
	if o.retry != nil {
		retry = *o.retry
	}
	timeout := o.timeout
	if timeout == 0 {
		timeout = b.x.wf.timeout
	}
	r := b.call(&call{
		kind: kindRun, name: name, explicit: o.id, retry: retry, timeout: timeout,
		fn: func(sc StepContext) (json.RawMessage, error) {
			v, err := fn(sc)
			if err != nil {
				return nil, err
			}
			return encodeValue(v)
		},
	})
	if r.err != nil {
		return zero, r.err
	}
	return decodeInto[T](r.out)
}

// MaxSleep is the longest sleep or callback timeout: a year.
const MaxSleep = 365 * 24 * time.Hour

// Sleep suspends the workflow on a durable timer for d (at most a year).
func Sleep(ctx Context, name string, d time.Duration, opts ...StepOption) error {
	b, err := branchOf(ctx)
	if err != nil {
		return err
	}
	if d < 0 || d > MaxSleep {
		return argumentError("a sleep is 0 to 1 year, got %v", d)
	}
	return sleepUntil(b, name, b.x.now().Add(d), opts)
}

// SleepUntil suspends the workflow until t (at most a year away; a past time doesn't wait).
func SleepUntil(ctx Context, name string, t time.Time, opts ...StepOption) error {
	b, err := branchOf(ctx)
	if err != nil {
		return err
	}
	now := b.x.now()
	if t.Sub(now) > MaxSleep {
		return argumentError("a sleep is at most a year")
	}
	if t.Before(now) {
		t = now
	}
	return sleepUntil(b, name, t, opts)
}

func sleepUntil(b *branch, name string, until time.Time, opts []StepOption) error {
	if err := checkName(name); err != nil {
		return err
	}
	o, err := applyOpts(opts)
	if err != nil {
		return err
	}
	// Milliseconds, as every SDK sends them; rounded up so the sleep is never shorter.
	until = until.UTC()
	if t := until.Truncate(time.Millisecond); !t.Equal(until) {
		until = t.Add(time.Millisecond)
	}
	return b.call(&call{kind: kindSleep, name: name, explicit: o.id, until: until}).err
}

// isoMillis is Now's wire format: RFC 3339 in UTC with exactly three fractional digits
// (JavaScript's Date.toISOString), the same in every SDK.
const isoMillis = "2006-01-02T15:04:05.000Z07:00"

// Now records the current time, once: a replay returns the same time (millisecond precision).
func Now(ctx Context, name string) (time.Time, error) {
	b, err := branchOf(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if err := checkName(name); err != nil {
		return time.Time{}, err
	}
	r := b.call(&call{kind: kindValue, name: name, produce: func(x *executor) (json.RawMessage, error) {
		return jsonx.Marshal(x.now().UTC().Format(isoMillis))
	}})
	if r.err != nil {
		return time.Time{}, r.err
	}
	s, err := decodeInto[string](r.out)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, NonRetriable(err)
	}
	return t, nil
}

// UUID records a random UUID, once: a replay returns the same one.
func UUID(ctx Context, name string) (string, error) {
	b, err := branchOf(ctx)
	if err != nil {
		return "", err
	}
	if err := checkName(name); err != nil {
		return "", err
	}
	r := b.call(&call{kind: kindValue, name: name, produce: func(x *executor) (json.RawMessage, error) {
		id, err := newUUIDv4(x.rand)
		if err != nil {
			return nil, err
		}
		return jsonx.Marshal(id)
	}})
	if r.err != nil {
		return "", r.err
	}
	return decodeInto[string](r.out)
}

// Callback is a single-use callback: POSTing JSON to URL completes it, once. Token is its only
// credential; treat both as secrets.
type Callback struct {
	URL, Token string
	stepID     string
}

// CreateCallback creates a callback whose timeout (1 second to 1 year) starts now. The callback
// is recorded before CreateCallback returns, so the URL can be handed out at once (in a step).
// Wait for it with WaitCallback.
func CreateCallback(ctx Context, name string, timeout time.Duration) (*Callback, error) {
	b, err := branchOf(ctx)
	if err != nil {
		return nil, err
	}
	if err := checkName(name); err != nil {
		return nil, err
	}
	if timeout < time.Second || timeout > MaxSleep {
		return nil, argumentError("a callback timeout is 1 second to 1 year, got %v", timeout)
	}
	c := &call{kind: kindCallbackCreate, name: name, timeoutMs: timeout.Milliseconds()}
	r := b.call(c)
	if r.err != nil {
		return nil, r.err
	}
	var v struct{ URL, Token string }
	if err := json.Unmarshal(r.out, &v); err != nil {
		return nil, NonRetriable(err)
	}
	return &Callback{URL: v.URL, Token: v.Token, stepID: c.stepID}, nil
}

// WaitCallback suspends until the callback is completed, and returns its JSON body decoded into
// T; or it returns an error matching ErrCallbackTimeout once the timeout passes.
func WaitCallback[T any](ctx Context, cb *Callback) (T, error) {
	var zero T
	b, err := branchOf(ctx)
	if err != nil {
		return zero, err
	}
	if cb == nil || cb.stepID == "" {
		return zero, argumentError("WaitCallback needs a callback from CreateCallback")
	}
	r := b.call(&call{kind: kindCallbackWait, stepID: cb.stepID})
	if r.err != nil {
		return zero, r.err
	}
	return decodeInto[T](r.out)
}

// Invoke starts a child run of workflow (in the same app) with input (JSON, at most 1 MiB; nil
// for none) and waits for it: it returns the child's output, or an error matching ErrChildFailed
// whose cause is the child's error.
func Invoke[T any](ctx Context, name, workflow string, input any) (T, error) {
	var zero T
	b, err := branchOf(ctx)
	if err != nil {
		return zero, err
	}
	if err := checkName(name); err != nil {
		return zero, err
	}
	if !protocol.ValidWorkflowName(workflow) {
		return zero, argumentError("%q isn't a workflow name (^[A-Za-z0-9_-]{1,64}$)", workflow)
	}
	var in json.RawMessage // no input: the field is omitted, as in every SDK
	if input != nil {
		in, err = jsonx.Marshal(input)
		if err != nil {
			return zero, argumentError("a child's input must be JSON: %v", err)
		}
		if len(in) > protocol.MaxOutputBytes {
			return zero, argumentError("a child's input is at most 1 MiB of JSON, got %d bytes", len(in))
		}
		if string(in) == "null" {
			in = nil
		}
	}
	r := b.call(&call{kind: kindInvoke, name: name, workflow: workflow, input: in})
	if r.err != nil {
		return zero, r.err
	}
	return decodeInto[T](r.out)
}

// Parallel runs each branch concurrently and returns once every branch has returned: nil, the
// one error, or the errors joined in branch order. Steps the branches reach together are planned
// together and run as separate step invocations. Each branch must call steps with the Context
// it receives.
func Parallel(ctx Context, branches ...func(Context) error) error {
	b, err := branchOf(ctx)
	if err != nil {
		return err
	}
	for _, fn := range branches {
		if fn == nil {
			return argumentError("Parallel's branches can't be nil")
		}
	}
	return b.call(&call{kind: kindGroup, branches: branches}).err
}

// Race runs each branch concurrently and returns the index and error of the first to return,
// in the order the engine recorded their steps, so a replay picks the same winner. The other
// branches keep running and may still record steps.
func Race(ctx Context, branches ...func(Context) error) (int, error) {
	b, err := branchOf(ctx)
	if err != nil {
		return -1, err
	}
	if len(branches) == 0 {
		return -1, argumentError("Race needs at least one branch")
	}
	for _, fn := range branches {
		if fn == nil {
			return -1, argumentError("Race's branches can't be nil")
		}
	}
	r := b.call(&call{kind: kindGroup, branches: branches, race: true})
	return r.winner, r.err
}
