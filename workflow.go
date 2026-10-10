package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/protocol"
)

// Workflow is a workflow definition, as New returns it.
type Workflow interface {
	Name() string
	definition() *definition
}

// RetryPolicy is a step retry policy: MaxAttempts attempts, waiting
// min(MaxMs, InitialMs × Multiplier^(attempt−1)) before each retry.
type RetryPolicy = protocol.RetryPolicy

// DefaultRetry is the retry policy of a step that sets none: 5 attempts, 1 s, ×2, at most 10
// minutes.
var DefaultRetry = protocol.DefaultRetry

type definition struct {
	name    string
	retry   *RetryPolicy // the workflow's default for its steps (merged with DefaultRetry); nil: DefaultRetry
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

// WithRetry sets the default retry policy of the workflow's steps. Zero fields take
// DefaultRetry's values, and the result must be within the engine's bounds (MaxAttempts 1–100,
// MaxMs at most 24 hours, Multiplier 1–10).
func WithRetry(p RetryPolicy) WorkflowOption {
	return func(d *definition) error {
		m := mergeRetry(DefaultRetry, p)
		if err := m.Validate(); err != nil {
			return err
		}
		d.retry = &m
		return nil
	}
}

// MaxStepTimeout is the longest step timeout: a step can't outlive an invocation.
const MaxStepTimeout = 15 * time.Minute

// WithTimeout sets the default step timeout: 1 ms to 15 minutes.
func WithTimeout(t time.Duration) WorkflowOption {
	return func(d *definition) error {
		if t < time.Millisecond || t > MaxStepTimeout {
			return fmt.Errorf("a step timeout is 1 ms to 15 minutes, got %v", t)
		}
		d.timeout = t
		return nil
	}
}

func mergeRetry(base, p RetryPolicy) RetryPolicy {
	if p.MaxAttempts != 0 {
		base.MaxAttempts = p.MaxAttempts
	}
	if p.InitialMs != 0 {
		base.InitialMs = p.InitialMs
	}
	if p.MaxMs != 0 {
		base.MaxMs = p.MaxMs
	}
	if p.Multiplier != 0 {
		base.Multiplier = p.Multiplier
	}
	return base
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
// time.
type ScheduleInfo = protocol.ScheduleInfo

// RunInfo describes the run a workflow is executing.
type RunInfo struct {
	ID, RunKey, Workflow string
	Attempt              int
	Schedule             *ScheduleInfo // set for runs a schedule started
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
