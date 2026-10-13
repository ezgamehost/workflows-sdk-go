package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// Workflow is a workflow definition, as New returns it.
type Workflow interface {
	Name() string
	definition() *definition
}

// RetryPolicy is a step retry policy: at most MaxAttempts attempts, waiting
// min(Max, Initial × Multiplier^(attempt−1)) before each retry. Every field counts, zero
// included: start from DefaultRetry and change what you need.
//
//	p := workflows.DefaultRetry
//	p.MaxAttempts = 3
type RetryPolicy struct {
	MaxAttempts int           // 1–100
	Initial     time.Duration // the first retry's delay, at least 0 (whole milliseconds)
	Max         time.Duration // the longest delay, 0 to 24 hours (whole milliseconds)
	Multiplier  float64       // 1–10
}

// DefaultRetry is the retry policy of a step that sets none: 5 attempts, 1 s apart doubling to
// at most 10 minutes.
var DefaultRetry = RetryPolicy{MaxAttempts: 5, Initial: time.Second, Max: 10 * time.Minute, Multiplier: 2}

// wire checks the policy against the engine's bounds and converts it.
func (p RetryPolicy) wire() (protocol.RetryPolicy, error) {
	if p.Initial%time.Millisecond != 0 || p.Max%time.Millisecond != 0 {
		return protocol.RetryPolicy{}, fmt.Errorf("a retry policy's Initial and Max are whole milliseconds, got %v and %v", p.Initial, p.Max)
	}
	w := protocol.RetryPolicy{MaxAttempts: p.MaxAttempts, InitialMs: p.Initial.Milliseconds(), MaxMs: p.Max.Milliseconds(), Multiplier: p.Multiplier}
	if err := w.Validate(); err != nil {
		return protocol.RetryPolicy{}, fmt.Errorf("the retry policy %+v is out of bounds (MaxAttempts 1–100, Initial ≥ 0, Max 0–24 h, Multiplier 1–10): %w", p, err)
	}
	return w, nil
}

func retryFromWire(w protocol.RetryPolicy) RetryPolicy {
	return RetryPolicy{MaxAttempts: w.MaxAttempts, Initial: time.Duration(w.InitialMs) * time.Millisecond, Max: time.Duration(w.MaxMs) * time.Millisecond, Multiplier: w.Multiplier}
}

// MarshalJSON writes the policy as the API does: {maxAttempts, initialMs, maxMs, multiplier}.
func (p RetryPolicy) MarshalJSON() ([]byte, error) {
	return json.Marshal(protocol.RetryPolicy{MaxAttempts: p.MaxAttempts, InitialMs: p.Initial.Milliseconds(), MaxMs: p.Max.Milliseconds(), Multiplier: p.Multiplier})
}

// UnmarshalJSON reads the API's form.
func (p *RetryPolicy) UnmarshalJSON(b []byte) error {
	var w protocol.RetryPolicy
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	*p = retryFromWire(w)
	return nil
}

type definition struct {
	name    string
	retry   *protocol.RetryPolicy // the workflow's default for its steps; nil: DefaultRetry
	timeout time.Duration
	run     func(ctx Context, input json.RawMessage) (json.RawMessage, error)
}

func (d *definition) Name() string            { return d.name }
func (d *definition) definition() *definition { return d }

func (d *definition) info() protocol.WorkflowInfo {
	wi := protocol.WorkflowInfo{Name: d.name}
	if d.retry != nil {
		r := *d.retry
		wi.Retry = &r
	}
	if d.timeout > 0 {
		wi.TimeoutMs = d.timeout.Milliseconds()
	}
	return wi
}

// WorkflowOption configures a workflow in New.
type WorkflowOption func(*definition) error

// WithRetry sets the default retry policy of the workflow's steps. The policy is used as given
// (start from DefaultRetry), and must be within the engine's bounds.
func WithRetry(p RetryPolicy) WorkflowOption {
	return func(d *definition) error {
		w, err := p.wire()
		if err != nil {
			return err
		}
		d.retry = &w
		return nil
	}
}

// MaxStepTimeout is the longest step timeout: 24 hours, a pull invocation's longest deadline. A
// push handler refuses a workflow timeout longer than its MaxInvocationSeconds.
const MaxStepTimeout = 24 * time.Hour

// WithTimeout sets the default step timeout: 1 ms to 24 hours. A pull app's invocation deadline
// is its longest announced workflow timeout (at least 15 minutes).
func WithTimeout(t time.Duration) WorkflowOption {
	return func(d *definition) error {
		if t < time.Millisecond || t > MaxStepTimeout {
			return fmt.Errorf("a step timeout is 1 ms to 24 hours, got %v", t)
		}
		d.timeout = t
		return nil
	}
}

// New defines a workflow named name (^[A-Za-z0-9_-]{1,64}$) running fn. Its input is decoded from
// JSON into I, and its output O is returned as JSON. New panics on an invalid name or option, as
// regexp.MustCompile does: workflows are defined once at start-up.
func New[I, O any](name string, fn func(ctx Context, in I) (O, error), opts ...WorkflowOption) Workflow {
	if !protocol.ValidWorkflowName(name) {
		panic(fmt.Sprintf("workflows.New: %q isn't a workflow name (^[A-Za-z0-9_-]{1,64}$)", name))
	}
	if fn == nil {
		panic("workflows.New(" + name + "): fn is nil")
	}
	d := &definition{name: name}
	for _, o := range opts {
		if err := o(d); err != nil {
			panic("workflows.New(" + name + "): " + err.Error())
		}
	}
	d.run = func(ctx Context, raw json.RawMessage) (json.RawMessage, error) {
		in, err := decodeInto[I](raw)
		if err != nil {
			return nil, err
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}
		return encodeValue(out)
	}
	return d
}

// ScheduleInfo is set on a run a schedule started: the schedule's ID and the scheduled (cron)
// time, which isn't the jittered time it fired.
type ScheduleInfo struct {
	ID            string    `json:"id"`
	ScheduledTime time.Time `json:"scheduledTime"`
}

// RunInfo describes the run a workflow is executing.
type RunInfo struct {
	ID, RunKey, Workflow string
	Attempt              int
	Schedule             *ScheduleInfo // set for runs a schedule started
	// Traceparent is the invocation's W3C traceparent, exactly as received (the push request's
	// traceparent header, or a pulled invocation's traceparent field), while the project's
	// observability setting is on; "" otherwise. Parent your own spans on it: the SDK has no
	// OpenTelemetry dependency.
	Traceparent string
}

// Context is what a workflow (and each Parallel or Race branch) receives. Its Done channel is
// closed once the run is cancelling. Pass it to the step functions.
type Context interface {
	context.Context
	Run() RunInfo
}

// StepContext is what a Run step's function receives. It's cancelled at the step's timeout, at
// the invocation's deadline, or when a pull worker's heartbeat reports the run is cancelling.
type StepContext interface {
	context.Context
	Attempt() int
	// IdempotencyKey is "<runId>:<stepId>:<attempt>", for idempotent calls to other systems.
	IdempotencyKey() string
}
