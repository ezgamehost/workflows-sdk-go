package workflows

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// A step's function that calls a step with the workflow's Context gets an argument error, and
// the step fails non-retriably; nothing hangs.
func TestStepCalledInsideAStepFails(t *testing.T) {
	wf := New("nested", func(ctx Context, _ none) (int, error) {
		return Run(ctx, "outer", func(StepContext) (int, error) {
			return Run(ctx, "inner", func(StepContext) (int, error) { return 1, nil })
		})
	})
	done := make(chan []protocol.Command, 1)
	go func() { done <- run(t, wf, invocation("nested", nil), &fakeIO{}) }()
	select {
	case cmds := <-done:
		if len(cmds) != 1 || cmds[0].Type != "stepFailed" || cmds[0].Retryable || !strings.Contains(cmds[0].Error.Message, "inside a step") {
			t.Fatalf("%s", show(cmds))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the invocation hung")
	}
}

// runtime.Goexit inside a step's function fails the step instead of hanging the invocation.
func TestGoexitInAStepFails(t *testing.T) {
	wf := New("goexit", func(ctx Context, _ none) (int, error) {
		return Run(ctx, "s", func(StepContext) (int, error) { runtime.Goexit(); return 0, nil })
	})
	done := make(chan []protocol.Command, 1)
	go func() { done <- run(t, wf, invocation("goexit", nil), &fakeIO{}) }()
	select {
	case cmds := <-done:
		if len(cmds) != 1 || cmds[0].Type != "stepFailed" || cmds[0].Retryable {
			t.Fatalf("%s", show(cmds))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the invocation hung")
	}
}

func TestWrappedNonRetriableIsNotRetried(t *testing.T) {
	wf := New("nr", func(ctx Context, _ none) (int, error) {
		return Run(ctx, "s", func(StepContext) (int, error) {
			return 0, fmt.Errorf("charge: %w", NonRetriable(errors.New("card declined")))
		})
	})
	cmds := run(t, wf, invocation("nr", nil), &fakeIO{})
	if last := cmds[len(cmds)-1]; last.Type != "stepFailed" || last.Retryable {
		t.Fatalf("%s", show(cmds))
	}
}

// A step invocation never sends runCompleted or runFailed, even when the journal lets the
// workflow finish (a race whose winner is recorded) or fail.
func TestStepInvocationNeverEndsTheRun(t *testing.T) {
	wf := New("r", func(ctx Context, _ none) (int, error) {
		return Race(ctx,
			func(c Context) error { _, err := step(c, "a", 1); return err },
			func(c Context) error { _, err := step(c, "b", 2); return err },
		)
	})
	inv := invocation("r", nil, entry(2, "a", "run", "completed", 1, "1"), entry(1, "b", "run", "running", 0, ""))
	inv.Kind, inv.TargetStepID = protocol.InvocationStep, protocol.StepID("b", 0)
	if cmds := run(t, wf, inv, &fakeIO{}); len(cmds) != 0 {
		t.Fatalf("%s", show(cmds))
	}
	dupe := New("dupe", func(ctx Context, _ none) (none, error) {
		return nil, Parallel(ctx,
			func(c Context) error { _, err := step(c, "x", 1); return err },
			func(c Context) error { _, err := step(c, "x", 2); return err },
		)
	})
	inv = invocation("dupe", nil)
	inv.Kind, inv.TargetStepID = protocol.InvocationStep, protocol.StepID("x", 0)
	if cmds := run(t, dupe, inv, &fakeIO{}); len(cmds) != 0 {
		t.Fatalf("fatal: %s", show(cmds))
	}
}

// A heartbeat's cancel cancels the running step with ErrCancelled as the cause, closes the
// workflow's Done, and no new inline step starts.
func TestHeartbeatCancelStopsInlineSteps(t *testing.T) {
	cancelled := make(chan struct{})
	var cause error
	var wfDone atomic.Bool
	wf := New("hb", func(ctx Context, _ none) (int, error) {
		_, err := Run(ctx, "first", func(sc StepContext) (int, error) {
			close(cancelled)
			<-sc.Done()
			cause = context.Cause(sc)
			return 1, nil
		})
		select {
		case <-ctx.Done():
			wfDone.Store(true)
		default:
		}
		if err != nil {
			return 0, err
		}
		return step(ctx, "second", 2)
	})
	io := &fakeIO{}
	cmds := run(t, wf, invocation("hb", nil), io, func(o *execOptions) { o.Cancelled = cancelled })
	if !errors.Is(cause, ErrCancelled) || !wfDone.Load() || len(io.checkpoints) != 1 || show(types(cmds)) != `["stepCompleted"]` {
		t.Fatalf("cause %v done %v checkpoints %s cmds %s", cause, wfDone.Load(), show(io.checkpoints), show(cmds))
	}
}

func TestOversizedResultsFailTheRun(t *testing.T) {
	many := New("many", func(ctx Context, _ none) (none, error) {
		branches := make([]func(Context) error, protocol.MaxPlannedSteps+1)
		for i := range branches {
			branches[i] = func(c Context) error { _, err := step(c, fmt.Sprintf("s%d", i), i); return err }
		}
		return nil, Parallel(ctx, branches...)
	})
	cmds := run(t, many, invocation("many", nil), &fakeIO{})
	if len(cmds) != 1 || cmds[0].Type != "runFailed" || cmds[0].Error.Code != "workflows.non_retriable" {
		t.Fatalf("%.300s", show(cmds))
	}
	big := strings.Repeat("x", 1<<20-100)
	fat := New("fat", func(ctx Context, _ none) (none, error) {
		branches := make([]func(Context) error, 10)
		for i := range branches {
			branches[i] = func(c Context) error { _, err := Invoke[int](c, fmt.Sprintf("kid%d", i), "child", big); return err }
		}
		return nil, Parallel(ctx, branches...)
	})
	cmds = run(t, fat, invocation("fat", nil), &fakeIO{}, func(o *execOptions) { o.MaxResultBytes = 8 << 20 })
	if len(cmds) != 1 || cmds[0].Type != "runFailed" || !strings.Contains(cmds[0].Error.Message, "bytes of JSON") {
		t.Fatalf("%.300s", show(cmds))
	}
}

func TestDrainEndsWithTheBranches(t *testing.T) {
	events := make(chan event, 4)
	ended := make(chan struct{})
	go func() { drain(events, 2); close(ended) }()
	c := &call{reply: make(chan reply, 1)}
	events <- event{park: c}
	events <- event{done: &branch{}}
	events <- event{done: &branch{}}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("drain didn't return")
	}
	if r := <-c.reply; !r.exit {
		t.Fatal("a parked call wasn't told to exit")
	}
}

func TestCompleteCallbackURLNeverShowsTheToken(t *testing.T) {
	err := CompleteCallbackURL(context.Background(), nil, "http://127.0.0.1:1/public/v1/callbacks/3ff.SECRETTOKEN", "x")
	if err == nil || strings.Contains(err.Error(), "SECRETTOKEN") {
		t.Fatalf("%v", err)
	}
	if err := CompleteCallbackURL(context.Background(), nil, "://bad/SECRETTOKEN", "x"); err == nil || strings.Contains(err.Error(), "SECRETTOKEN") {
		t.Fatalf("%v", err)
	}
}

func TestCreateAppNeverResendsA503(t *testing.T) {
	var n atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.WriteHeader(503)
	}))
	defer s.Close()
	c, _ := NewClient(ClientOptions{APIKey: "k", OrganizationID: "o", ProjectID: "p", BaseURL: s.URL, MaxRetryDelay: time.Millisecond})
	_, err := c.CreateApp(context.Background(), "a", CreateAppOptions{})
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != "outcome_unknown" || n.Load() != 1 {
		t.Fatalf("%v after %d", err, n.Load())
	}
	// A 2xx without a secret (a replay) is ambiguous too.
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"app":{"id":"a1","kind":"push"},"signingSecret":null}`))
	}))
	defer s2.Close()
	c, _ = NewClient(ClientOptions{APIKey: "k", OrganizationID: "o", ProjectID: "p", BaseURL: s2.URL})
	if _, err := c.CreateApp(context.Background(), "a", CreateAppOptions{}); !errors.As(err, &ae) || ae.Code != "outcome_unknown" {
		t.Fatalf("%v", err)
	}
	if _, err := c.CreateApp(context.Background(), "a", CreateAppOptions{IdempotencyKey: "mine"}); err != nil {
		t.Fatalf("a replay of the caller's own key: %v", err)
	}
}

func TestWorkerRunsWhatItReceivedAtShutdown(t *testing.T) {
	f := newFakeLive(t)
	f.push(pulledInvocation("twostep", map[string]int{"X": 1}))
	ctx, cancel := context.WithCancel(context.Background())
	// The context ends just as a poll's answer (with a task) has arrived: the task still runs.
	httpc := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		res, err := http.DefaultTransport.RoundTrip(r)
		if err != nil || !strings.HasSuffix(r.URL.Path, "/workers/poll") || res.StatusCode != 200 {
			return res, err
		}
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		res.Body = io.NopCloser(bytes.NewReader(body))
		cancel()
		return res, err
	})}
	w, err := NewWorker(WorkerOptions{APIKey: "ezgh_key", OrganizationID: testOrg, ProjectID: testProject, App: testApp, LiveURL: f.srv.URL,
		Workflows: []Workflow{twoStep()}, HTTPClient: httpc})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.completed:
	default:
		t.Fatal("the polled task was dropped at shutdown")
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
