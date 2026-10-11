package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/live"
	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

func sequenceWF() Workflow {
	return New("sequence", func(ctx Context, in struct{ N int }) (int, error) {
		sum := 0
		for i := 0; i < in.N; i++ {
			v, err := step(ctx, "add", i)
			if err != nil {
				return 0, err
			}
			sum += v
		}
		return sum, nil
	})
}

func TestSequenceRunsStepsInlineAfterAcknowledgedStart(t *testing.T) {
	io := &fakeIO{}
	cmds := run(t, sequenceWF(), invocation("sequence", map[string]int{"N": 2}), io)
	if len(io.checkpoints) != 2 || io.checkpoints[0][0].Type != "stepStarted" || io.checkpoints[0][0].StepID != protocol.StepID("add", 0) {
		t.Fatalf("checkpoints %s", show(io.checkpoints))
	}
	if io.checkpoints[1][0].Type != "stepCompleted" || io.checkpoints[1][1].StepID != protocol.StepID("add", 1) {
		t.Fatalf("second checkpoint %s", show(io.checkpoints[1]))
	}
	if got := types(cmds); len(got) != 2 || got[0] != "stepCompleted" || got[1] != "runCompleted" || string(cmds[1].Output) != "1" {
		t.Fatalf("commands %s", show(cmds))
	}
}

func raceWF() Workflow {
	return New("race", func(ctx Context, _ none) (string, error) {
		var a, b string
		w, err := Race(ctx,
			func(c Context) (err error) { a, err = step(c, "a", "a"); return },
			func(c Context) (err error) { b, err = step(c, "b", "b"); return },
		)
		if err != nil {
			return "", err
		}
		win := []string{a, b}[w]
		return step(ctx, "after-"+win, win)
	})
}

func TestReplayResolvesBySeqSoRaceIsDeterministic(t *testing.T) {
	inv := invocation("race", nil,
		entry(1, "b", "run", "completed", 1, `"b"`),
		entry(2, "a", "run", "completed", 1, `"a"`),
	)
	for i := 0; i < 50; i++ { // goroutine scheduling must never change the answer
		io := &fakeIO{}
		cmds := run(t, raceWF(), inv, io)
		if len(io.checkpoints) == 0 || io.checkpoints[0][0].StepID != protocol.StepID("after-b", 0) {
			t.Fatalf("iteration %d took the wrong branch: %s", i, show(io.checkpoints))
		}
		if cmds[len(cmds)-1].Type != "runCompleted" || string(cmds[len(cmds)-1].Output) != `"b"` {
			t.Fatalf("result %s", show(cmds))
		}
	}
}

func TestRaceFirstTurnPlansBoth(t *testing.T) {
	for i := 0; i < 50; i++ {
		cmds := run(t, raceWF(), invocation("race", nil), &fakeIO{})
		if len(cmds) != 1 || cmds[0].Type != "planParallel" || len(cmds[0].Steps) != 2 ||
			cmds[0].Steps[0].StepID != protocol.StepID("a", 0) || cmds[0].Steps[1].StepID != protocol.StepID("b", 0) {
			t.Fatalf("got %s", show(cmds))
		}
	}
}

func fanoutWF() Workflow {
	return New("fanout", func(ctx Context, _ none) (int, error) {
		vals := make([]int, 3)
		branches := make([]func(Context) error, 3)
		for i := range branches {
			branches[i] = func(c Context) (err error) {
				vals[i], err = step(c, "p"+string(rune('0'+i)), i)
				return
			}
		}
		err := Parallel(ctx, branches...)
		return vals[0] + vals[1] + vals[2], err
	})
}

func TestParallelPlansNewStepsTogether(t *testing.T) {
	for i := 0; i < 50; i++ {
		io := &fakeIO{}
		cmds := run(t, fanoutWF(), invocation("fanout", nil), io)
		if len(io.checkpoints) != 0 || len(cmds) != 1 || cmds[0].Type != "planParallel" || len(cmds[0].Steps) != 3 {
			t.Fatalf("got %s", show(cmds))
		}
		for j, s := range cmds[0].Steps {
			if s.StepID != protocol.StepID("p"+string(rune('0'+j)), 0) || s.Retry == nil || *s.Retry != protocol.DefaultRetry {
				t.Fatalf("step %d: %s", j, show(s))
			}
		}
	}
}

func TestParallelSequentialAtProtocol10(t *testing.T) {
	inv := invocation("fanout", nil)
	inv.ProtocolVersion = "1.0"
	io := &fakeIO{}
	cmds := run(t, fanoutWF(), inv, io)
	if len(io.checkpoints) != 3 || io.checkpoints[1][0].Type != "stepCompleted" || io.checkpoints[1][1].StepID != protocol.StepID("p1", 0) {
		t.Fatalf("checkpoints %s", show(io.checkpoints))
	}
	if show(types(cmds)) != `["stepCompleted","runCompleted"]` || string(cmds[1].Output) != "3" {
		t.Fatalf("commands %s", show(cmds))
	}
}

func TestStepInvocationRunsOnlyItsTarget(t *testing.T) {
	ran := map[string]int{}
	var mu sync.Mutex
	branch := func(name string) func(Context) error {
		return func(c Context) error {
			_, err := Run(c, name, func(StepContext) (int, error) { mu.Lock(); ran[name]++; mu.Unlock(); return 1, nil })
			return err
		}
	}
	wf := New("fanout2", func(ctx Context, _ none) (int, error) { return 0, Parallel(ctx, branch("x"), branch("y")) })
	inv := invocation("fanout2", nil, entry(1, "x", "run", "running", 0, ""), entry(2, "y", "run", "running", 0, ""))
	inv.Kind, inv.TargetStepID = protocol.InvocationStep, protocol.StepID("y", 0)
	io := &fakeIO{}
	cmds := run(t, wf, inv, io)
	if ran["x"] != 0 || ran["y"] != 1 || len(cmds) != 1 || cmds[0].Type != "stepCompleted" || cmds[0].StepID != protocol.StepID("y", 0) {
		t.Fatalf("ran %v cmds %s", ran, show(cmds))
	}
	if io.checkpoints[0][0].Attempt != 1 {
		t.Fatalf("a planned step's first run is attempt 1: %s", show(io.checkpoints))
	}
	// A retrying parallel step is journaled running at its last attempt: it runs at attempt + 1.
	inv.Journal.Entries[1].Attempt = 2
	io = &fakeIO{}
	run(t, wf, inv, io)
	if io.checkpoints[0][0].Attempt != 3 {
		t.Fatalf("attempt %d, want 3", io.checkpoints[0][0].Attempt)
	}
}

func TestParallelCoalescedReplay(t *testing.T) {
	inv := invocation("fanout", nil, entry(1, "p0", "run", "completed", 1, "0"), entry(2, "p1", "run", "running", 1, ""), entry(3, "p2", "run", "completed", 1, "2"))
	if cmds := run(t, fanoutWF(), inv, &fakeIO{}); len(cmds) != 0 {
		t.Fatalf("a running step is awaited: %s", show(cmds))
	}
	inv = invocation("fanout", nil, entry(1, "p0", "run", "completed", 1, "0"), entry(3, "p1", "run", "completed", 1, "1"), entry(2, "p2", "run", "completed", 1, "2"))
	if cmds := run(t, fanoutWF(), inv, &fakeIO{}); show(types(cmds)) != `["runCompleted"]` || string(cmds[0].Output) != "3" {
		t.Fatalf("got %s", show(cmds))
	}
}

func raceTimeoutWF() Workflow {
	return New("racetimeout", func(ctx Context, _ none) (string, error) {
		var out [2]string
		w, err := Race(ctx,
			func(c Context) (err error) { out[0], err = step(c, "work", "work"); return },
			func(c Context) error {
				if err := Sleep(c, "limit", time.Minute); err != nil {
					return err
				}
				out[1] = "limit"
				return nil
			},
		)
		if err != nil {
			return "", err
		}
		return out[w], nil
	})
}

func TestRaceWithSleep(t *testing.T) {
	cmds := run(t, raceTimeoutWF(), invocation("racetimeout", nil), &fakeIO{})
	if show(types(cmds)) != `["planParallel","sleep"]` || cmds[0].Steps[0].Name != "work" || cmds[1].Name != "limit" {
		t.Fatalf("got %s", show(cmds))
	}
	limitFirst := invocation("racetimeout", nil, entry(1, "limit", "sleep", "completed", 1, ""), entry(2, "work", "run", "running", 1, ""))
	if cmds := run(t, raceTimeoutWF(), limitFirst, &fakeIO{}); show(cmds) != `[{"type":"runCompleted","output":"limit"}]` {
		t.Fatalf("got %s", show(cmds))
	}
	workFirst := invocation("racetimeout", nil, entry(1, "work", "run", "completed", 1, `"work"`), entry(2, "limit", "sleep", "sleeping", 1, ""))
	if cmds := run(t, raceTimeoutWF(), workFirst, &fakeIO{}); show(cmds) != `[{"type":"runCompleted","output":"work"}]` {
		t.Fatalf("got %s", show(cmds))
	}
}

// One new run discovered while another step is still running is planned, not run inline.
func TestNewRunBesideARunningStepIsPlanned(t *testing.T) {
	wf := New("beside", func(ctx Context, _ none) (int, error) {
		var a, b int
		err := Parallel(ctx,
			func(c Context) (err error) { a, err = step(c, "a", 1); return },
			func(c Context) (err error) {
				if _, err = step(c, "b0", 0); err != nil {
					return err
				}
				b, err = step(c, "b1", 2)
				return
			},
		)
		return a + b, err
	})
	inv := invocation("beside", nil, entry(1, "a", "run", "running", 1, ""), entry(2, "b0", "run", "completed", 1, "0"))
	io := &fakeIO{}
	cmds := run(t, wf, inv, io)
	if len(io.checkpoints) != 0 || show(types(cmds)) != `["planParallel"]` || len(cmds[0].Steps) != 1 || cmds[0].Steps[0].Name != "b1" {
		t.Fatalf("got %s %s", show(io.checkpoints), show(cmds))
	}
}

func TestDuplicatePendingNamesFailTheRun(t *testing.T) {
	wf := New("dupe", func(ctx Context, _ none) (none, error) {
		return nil, Parallel(ctx,
			func(c Context) error { _, err := step(c, "x", 1); return err },
			func(c Context) error { _, err := step(c, "x", 2); return err },
		)
	})
	cmds := run(t, wf, invocation("dupe", nil), &fakeIO{})
	if len(cmds) != 1 || cmds[0].Type != "runFailed" || cmds[0].Error.Code != "workflows.duplicate_step_name" || cmds[0].Error.Name != "DuplicateStepName" {
		t.Fatalf("got %s", show(cmds))
	}
}

func TestExplicitIDReusedFails(t *testing.T) {
	seq := New("dupeseq", func(ctx Context, _ none) (int, error) {
		if _, err := step(ctx, "a", 1, WithID("same")); err != nil {
			return 0, err
		}
		return step(ctx, "b", 2, WithID("same"))
	})
	cmds := run(t, seq, invocation("dupeseq", nil), &fakeIO{})
	if last := cmds[len(cmds)-1]; last.Type != "runFailed" || last.Error.Code != "workflows.duplicate_step_name" {
		t.Fatalf("got %s", show(cmds))
	}
	par := New("dupeid", func(ctx Context, _ none) (none, error) {
		return nil, Parallel(ctx,
			func(c Context) error { _, err := step(c, "a", 1, WithID("same")); return err },
			func(c Context) error { _, err := step(c, "b", 2, WithID("same")); return err },
		)
	})
	cmds = run(t, par, invocation("dupeid", nil), &fakeIO{})
	if len(cmds) != 1 || cmds[0].Type != "runFailed" || cmds[0].Error.Code != "workflows.duplicate_step_name" {
		t.Fatalf("got %s", show(cmds))
	}
}

func TestExplicitIDKeepsTheNameCounter(t *testing.T) {
	wf := New("explicitid", func(ctx Context, _ none) (int, error) {
		if _, err := step(ctx, "x", 1, WithID("custom-1")); err != nil {
			return 0, err
		}
		return step(ctx, "x", 2)
	})
	io := &fakeIO{}
	run(t, wf, invocation("explicitid", nil), io)
	if io.checkpoints[0][0].StepID != "custom-1" || io.checkpoints[1][1].StepID != protocol.StepID("x", 0) {
		t.Fatalf("checkpoints %s", show(io.checkpoints))
	}
}

func TestSleepEndsTheInvocationAndBranchesExit(t *testing.T) {
	exited := make(chan struct{})
	var deferredStep atomic.Bool
	wf := New("sleepy", func(ctx Context, _ none) (string, error) {
		defer close(exited)
		defer func() {
			// A step called while exiting exits at once and records nothing.
			_, _ = step(ctx, "deferred", 1)
			deferredStep.Store(true)
		}()
		if err := Sleep(ctx, "nap", 2*time.Second); err != nil {
			return "", err
		}
		return "never", nil
	})
	cmds := run(t, wf, invocation("sleepy", nil), &fakeIO{})
	if len(cmds) != 1 || cmds[0].Type != "sleep" || cmds[0].Until == nil || cmds[0].StepID != protocol.StepID("nap", 0) {
		t.Fatalf("got %s", show(cmds))
	}
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("the workflow goroutine didn't exit")
	}
	if deferredStep.Load() {
		t.Fatal("a step call while exiting returned")
	}
}

func cancellableWF() Workflow {
	return New("cancellable", func(ctx Context, _ none) (string, error) {
		err := Sleep(ctx, "wait", time.Hour)
		if _, cerr := step(ctx, "cleanup", "cleaned"); cerr != nil {
			return "", cerr
		}
		if err != nil {
			return "", err
		}
		return "finished", nil
	})
}

func TestCancelReturnsErrCancelledAndAllowsCleanup(t *testing.T) {
	var sawDone, sawCancelled atomic.Bool
	wf := New("cancellable2", func(ctx Context, _ none) (string, error) {
		if err := Sleep(ctx, "wait", time.Hour); err != nil {
			sawCancelled.Store(errors.Is(err, ErrCancelled))
			select {
			case <-ctx.Done():
				sawDone.Store(true)
			default:
			}
			if _, cerr := step(ctx, "cleanup", "cleaned"); cerr != nil {
				return "", cerr
			}
			if _, verr := Now(ctx, "late"); !errors.Is(verr, ErrCancelled) {
				return "", errors.New("other primitives must fail while cancelling")
			}
			return "", err
		}
		return "finished", nil
	})
	inv := invocation("cancellable2", nil, entry(1, "wait", "sleep", "sleeping", 1, ""))
	inv.Cancel = &protocol.Cancel{RequestedAt: time.Now(), GraceUntil: time.Now().Add(5 * time.Minute)}
	io := &fakeIO{}
	cmds := run(t, wf, inv, io)
	if !sawCancelled.Load() || !sawDone.Load() {
		t.Fatalf("ErrCancelled %v, Done closed %v", sawCancelled.Load(), sawDone.Load())
	}
	if len(io.checkpoints) != 1 || io.checkpoints[0][0].Name != "cleanup" || show(types(cmds)) != `["stepCompleted","runFailed"]` || cmds[1].Error.Code != "workflows.cancelled" {
		t.Fatalf("checkpoints %s cmds %s", show(io.checkpoints), show(cmds))
	}
	// Replayed: the wait is cancelled, the cleanup completed.
	inv = invocation("cancellable", nil, entry(1, "wait", "sleep", "cancelled", 1, ""), entry(2, "cleanup", "run", "completed", 1, `"cleaned"`))
	inv.Cancel = &protocol.Cancel{RequestedAt: time.Now(), GraceUntil: time.Now().Add(5 * time.Minute)}
	cmds = run(t, cancellableWF(), inv, &fakeIO{})
	if show(types(cmds)) != `["runFailed"]` || cmds[0].Error.Code != "workflows.cancelled" || cmds[0].CauseStepID != "" {
		t.Fatalf("got %s", show(cmds))
	}
}

func TestStepRunsOnlyAfterItsStartIsAcknowledged(t *testing.T) {
	var ran atomic.Bool
	wf := New("effect", func(ctx Context, _ none) (string, error) {
		return Run(ctx, "effect", func(StepContext) (string, error) { ran.Store(true); return "done", nil })
	})
	for _, answer := range []error{live.ErrSuperseded, &live.CheckpointFailed{Code: "checkpoint_failed"}} {
		io := &fakeIO{answer: func(int) error { return answer }}
		inv := invocation("effect", nil)
		_, err := execute(context.Background(), inv, wf.definition(), io, execOptions{Deadline: inv.Deadline})
		if !errors.Is(err, answer) || ran.Load() {
			t.Fatalf("err %v ran %v", err, ran.Load())
		}
	}
}

func TestLiveValueEqualsReplayedValue(t *testing.T) {
	type out struct {
		At time.Time `json:"at"`
		N  *int      `json:"n"`
		M  map[string]int
	}
	var live1 out
	wf := New("vals", func(ctx Context, _ none) (out, error) {
		v, err := Run(ctx, "v", func(StepContext) (out, error) { return out{At: time.Unix(0, 1).UTC()}, nil })
		live1 = v
		return v, err
	})
	cmds := run(t, wf, invocation("vals", nil), &fakeIO{})
	var replayed out
	if err := json.Unmarshal(cmds[0].Output, &replayed); err != nil || !replayed.At.Equal(live1.At) || replayed.N != nil || live1.M != nil {
		t.Fatalf("live %+v replayed %+v", live1, replayed)
	}
}

func TestStepFailedCarriesRetryableAndRetryAfter(t *testing.T) {
	cases := []struct {
		err       error
		retryable bool
		after     int64
		code      string
		name      string
	}{
		{RetryAfter(errors.New("slow down"), 1500*time.Millisecond), true, 1500, "user", "RetryAfterError"},
		{RetryAfter(errors.New("slow"), time.Duration(1e18)), true, 86_400_000, "user", "RetryAfterError"},
		{RetryAfter(errors.New("slow"), -time.Second), true, 0, "user", "RetryAfterError"},
		{NonRetriable(errors.New("nope")), false, 0, "workflows.non_retriable", "NonRetriableError"},
		{&Error{Code: "user", Name: "Error", Message: "boom", Data: json.RawMessage(`{"n":1}`)}, true, 0, "user", "Error"},
	}
	for _, c := range cases {
		wf := New("f", func(ctx Context, _ none) (none, error) {
			_, err := Run(ctx, "s", func(StepContext) (int, error) { return 0, c.err })
			return nil, err
		})
		cmds := run(t, wf, invocation("f", nil), &fakeIO{})
		got := cmds[0]
		if got.Type != "stepFailed" || got.Retryable != c.retryable || got.RetryAfterMs != c.after || got.Error.Code != c.code || got.Error.Name != c.name || got.Attempt != 1 {
			t.Errorf("%v: got %s", c.err, show(got))
		}
	}
}

func TestOutputOver1MiBFailsNonRetriably(t *testing.T) {
	wf := New("big", func(ctx Context, _ none) (string, error) { return step(ctx, "big", strings.Repeat("x", 1<<20+1)) })
	cmds := run(t, wf, invocation("big", nil), &fakeIO{})
	if cmds[0].Type != "stepFailed" || cmds[0].Retryable || cmds[0].Error.Code != "workflows.output_too_large" || cmds[0].Error.Name != "OutputTooLarge" {
		t.Fatalf("got %s", show(cmds[0]))
	}
	fn := New("fn", func(ctx Context, _ none) (func(), error) {
		return Run(ctx, "fn", func(StepContext) (func(), error) { return func() {}, nil })
	})
	cmds = run(t, fn, invocation("fn", nil), &fakeIO{})
	if cmds[0].Type != "stepFailed" || cmds[0].Retryable || cmds[0].Error.Code != "workflows.non_retriable" {
		t.Fatalf("got %s", show(cmds[0]))
	}
}

func TestStepBudgetEndsWithoutTerminal(t *testing.T) {
	cmds := run(t, sequenceWF(), invocation("sequence", map[string]int{"N": 3}), &fakeIO{}, func(o *execOptions) { o.MaxSteps = 2 })
	if show(types(cmds)) != `["stepCompleted"]` {
		t.Fatalf("got %s", show(cmds))
	}
}

func TestDeadlineNearEndsWithoutCommands(t *testing.T) {
	inv := invocation("sequence", map[string]int{"N": 1})
	inv.Deadline = time.Now().Add(3 * time.Second)
	io := &fakeIO{}
	if cmds := run(t, sequenceWF(), inv, io); len(cmds) != 0 || len(io.checkpoints) != 0 {
		t.Fatalf("got %s", show(cmds))
	}
}

func TestStepTimeout(t *testing.T) {
	wf := New("timeout", func(ctx Context, _ none) (int, error) {
		return Run(ctx, "slow", func(sc StepContext) (int, error) { <-sc.Done(); return 1, nil }, WithStepTimeout(50*time.Millisecond))
	})
	io := &fakeIO{}
	cmds := run(t, wf, invocation("timeout", nil), io)
	if cmds[0].Type != "stepFailed" || cmds[0].Error.Code != "workflows.step_timeout" || !cmds[0].Retryable || io.checkpoints[0][0].TimeoutMs != 50 {
		t.Fatalf("got %s", show(cmds))
	}
}

func TestStepContextIdempotencyKeyAndCancelledChannel(t *testing.T) {
	cancelled := make(chan struct{})
	var key string
	var attempt int
	wf := New("long", func(ctx Context, _ none) (string, error) {
		return Run(ctx, "long", func(sc StepContext) (string, error) {
			key, attempt = sc.IdempotencyKey(), sc.Attempt()
			close(cancelled) // the heartbeat reports cancel while the step runs
			select {
			case <-sc.Done():
				return "aborted", nil
			case <-time.After(5 * time.Second):
				return "done", nil
			}
		})
	})
	cmds := run(t, wf, invocation("long", nil), &fakeIO{}, func(o *execOptions) { o.Cancelled = cancelled })
	if string(cmds[0].Output) != `"aborted"` || key != testRunID+":"+protocol.StepID("long", 0)+":1" || attempt != 1 {
		t.Fatalf("got %s key %s", show(cmds), key)
	}
}

func approvalWF() Workflow {
	return New("approval", func(ctx Context, in struct {
		APIBase string `json:"apiBase"`
	}) (map[string]any, error) {
		cb, err := CreateCallback(ctx, "approval", time.Minute)
		if err != nil {
			return nil, err
		}
		urlOK, err := Run(ctx, "notify", func(StepContext) (bool, error) { return cb.URL == in.APIBase+"/public/v1/callbacks/"+cb.Token, nil })
		if err != nil {
			return nil, err
		}
		v, err := WaitCallback[any](ctx, cb)
		if err != nil {
			return nil, err
		}
		return map[string]any{"value": v, "urlOk": urlOK}, nil
	})
}

func TestCallbacks(t *testing.T) {
	io := &fakeIO{}
	inv := invocation("approval", map[string]string{"apiBase": "https://api.test"})
	cmds := run(t, approvalWF(), inv, io)
	if len(io.checkpoints) != 2 || len(io.checkpoints[0]) != 1 {
		t.Fatalf("checkpoints %s", show(io.checkpoints))
	}
	cc := io.checkpoints[0][0]
	tok, err := protocol.ParseCallbackToken(cc.Token)
	if cc.Type != "callbackCreated" || cc.StepID != protocol.StepID("approval", 0) || cc.Name != "approval" || cc.TimeoutMs != 60000 || err != nil || tok.Shard != 0x3ff {
		t.Fatalf("callbackCreated %s (%v)", show(cc), err)
	}
	if show(cmds) != `[{"type":"stepCompleted","stepId":"`+protocol.StepID("notify", 0)+`","name":"notify","output":true},{"type":"waitForCallback","stepId":"`+protocol.StepID("approval", 0)+`"}]` {
		t.Fatalf("commands %s", show(cmds))
	}
	token := cc.Token
	cbEntry := func(seq int64, status, output string) protocol.JournalEntry {
		e := entry(seq, "approval", "callback", status, 1, output)
		e.Token = token
		return e
	}
	// Waiting: the wait is sent again; no new checkpoint.
	io = &fakeIO{}
	inv = invocation("approval", map[string]string{"apiBase": "https://api.test"}, cbEntry(1, "waiting", ""), entry(2, "notify", "run", "completed", 1, "true"))
	cmds = run(t, approvalWF(), inv, io)
	if len(io.checkpoints) != 0 || show(types(cmds)) != `["waitForCallback"]` {
		t.Fatalf("waiting: %s %s", show(io.checkpoints), show(cmds))
	}
	// Completed: the value is the output; the replayed URL is the same.
	inv = invocation("approval", map[string]string{"apiBase": "https://api.test"}, cbEntry(3, "completed", `{"ok":true}`), entry(2, "notify", "run", "completed", 1, "true"))
	cmds = run(t, approvalWF(), inv, &fakeIO{})
	if show(cmds) != `[{"type":"runCompleted","output":{"urlOk":true,"value":{"ok":true}}}]` {
		t.Fatalf("completed: %s", show(cmds))
	}
	// Timed out.
	timeout := New("approvaltimeout", func(ctx Context, _ none) (any, error) {
		cb, err := CreateCallback(ctx, "approval", time.Second)
		if err != nil {
			return nil, err
		}
		if _, err := WaitCallback[any](ctx, cb); err != nil {
			var we *Error
			if !errors.Is(err, ErrCallbackTimeout) || !errors.As(err, &we) {
				return nil, errors.New("not a callback timeout")
			}
			return map[string]string{"code": we.Code}, nil
		}
		return "completed", nil
	})
	failed := cbEntry(1, "failed", "")
	failed.Error = &protocol.WireError{Code: "workflows.callback_timeout", Name: "CallbackTimeout", Message: "No completion within 1000 ms"}
	cmds = run(t, timeout, invocation("approvaltimeout", nil, failed), &fakeIO{})
	if show(cmds) != `[{"type":"runCompleted","output":{"code":"workflows.callback_timeout"}}]` {
		t.Fatalf("timeout: %s", show(cmds))
	}
	// Protocol 1.0 can't create callbacks: the run fails non-retriably.
	inv = invocation("approval", map[string]string{"apiBase": "x"})
	inv.ProtocolVersion = "1.0"
	cmds = run(t, approvalWF(), inv, &fakeIO{})
	if show(types(cmds)) != `["runFailed"]` || cmds[0].Error.Code != "workflows.non_retriable" {
		t.Fatalf("1.0: %s", show(cmds))
	}
}

func TestCallbackURLDefaultsToThePublicAPI(t *testing.T) {
	var url string
	wf := New("cb", func(ctx Context, _ none) (none, error) {
		cb, err := CreateCallback(ctx, "c", time.Hour)
		if err == nil {
			url = cb.URL
		}
		return nil, err
	})
	inv := invocation("cb", nil)
	inv.Endpoints.API = ""
	run(t, wf, inv, &fakeIO{})
	if !strings.HasPrefix(url, DefaultAPIBase+"/public/v1/callbacks/3ff.") {
		t.Fatalf("url %s", url)
	}
}

func TestInvoke(t *testing.T) {
	parent := New("parent", func(ctx Context, _ none) (int, error) {
		return Invoke[int](ctx, "child", "sequence", map[string]int{"n": 3})
	})
	cmds := run(t, parent, invocation("parent", nil), &fakeIO{})
	if show(cmds) != `[{"type":"invoke","stepId":"`+protocol.StepID("child", 0)+`","name":"child","workflow":"sequence","input":{"n":3}}]` {
		t.Fatalf("start: %s", show(cmds))
	}
	if cmds := run(t, parent, invocation("parent", nil, entry(1, "child", "invoke", "waiting", 1, "")), &fakeIO{}); len(cmds) != 0 {
		t.Fatalf("waiting: %s", show(cmds))
	}
	if cmds := run(t, parent, invocation("parent", nil, entry(1, "child", "invoke", "completed", 1, "3")), &fakeIO{}); show(cmds) != `[{"type":"runCompleted","output":3}]` {
		t.Fatalf("completed: %s", show(cmds))
	}
	catch := New("parentcatch", func(ctx Context, _ none) (any, error) {
		if _, err := Invoke[any](ctx, "child", "fatal", nil); err != nil {
			var we, cause *Error
			if !errors.Is(err, ErrChildFailed) || !errors.As(err, &we) || !errors.As(we.Cause, &cause) {
				return nil, errors.New("not a child failure")
			}
			return map[string]string{"code": we.Code, "causeCode": cause.Code}, nil
		}
		return "unreachable", nil
	})
	cmds = run(t, catch, invocation("parentcatch", nil), &fakeIO{})
	if show(cmds) != `[{"type":"invoke","stepId":"`+protocol.StepID("child", 0)+`","name":"child","workflow":"fatal"}]` {
		t.Fatalf("no input is omitted: %s", show(cmds))
	}
	failed := entry(1, "child", "invoke", "failed", 1, "")
	failed.Error = &protocol.WireError{Code: "workflows.child_failed", Name: "ChildFailed", Message: "The child run failed",
		Cause: &protocol.WireError{Code: "workflows.non_retriable", Name: "NonRetriableError", Message: "nope"}}
	cmds = run(t, catch, invocation("parentcatch", nil, failed), &fakeIO{})
	if show(cmds) != `[{"type":"runCompleted","output":{"causeCode":"workflows.non_retriable","code":"workflows.child_failed"}}]` {
		t.Fatalf("failed: %s", show(cmds))
	}
	// Returned as it is, a child's failure is the run's, with its step as the cause.
	plain := New("plain", func(ctx Context, _ none) (int, error) { return Invoke[int](ctx, "child", "fatal", nil) })
	cmds = run(t, plain, invocation("plain", nil, failed), &fakeIO{})
	if cmds[0].Type != "runFailed" || cmds[0].CauseStepID != protocol.StepID("child", 0) || cmds[0].Error.Cause == nil || cmds[0].Error.Cause.Code != "workflows.non_retriable" {
		t.Fatalf("returned: %s", show(cmds))
	}
}

func TestMixedSuspendsInCallOrder(t *testing.T) {
	wf := New("mixed", func(ctx Context, _ none) (none, error) {
		cb, err := CreateCallback(ctx, "cb", time.Hour)
		if err != nil {
			return nil, err
		}
		return nil, Parallel(ctx,
			func(c Context) error { _, err := WaitCallback[any](c, cb); return err },
			func(c Context) error { _, err := step(c, "r1", 1); return err },
			func(c Context) error { return Sleep(c, "s", time.Minute) },
			func(c Context) error { _, err := Invoke[any](c, "kid", "child", nil); return err },
			func(c Context) error { _, err := step(c, "r2", 2); return err },
		)
	})
	for i := 0; i < 20; i++ {
		cmds := run(t, wf, invocation("mixed", nil), &fakeIO{})
		if show(types(cmds)) != `["planParallel","waitForCallback","sleep","invoke"]` || len(cmds[0].Steps) != 2 || cmds[0].Steps[0].Name != "r1" || cmds[0].Steps[1].Name != "r2" {
			t.Fatalf("got %s", show(cmds))
		}
	}
}

func TestValuesAtTheFrontier(t *testing.T) {
	wf := New("values", func(ctx Context, _ none) (map[string]any, error) {
		tm, err := Now(ctx, "t")
		if err != nil {
			return nil, err
		}
		id, err := UUID(ctx, "id")
		if err != nil {
			return nil, err
		}
		return step(ctx, "use", map[string]any{"year": tm.Year() >= 2026, "idLength": len(id)})
	})
	io := &fakeIO{}
	cmds := run(t, wf, invocation("values", nil), io)
	cp := io.checkpoints[0]
	if show(types(cp)) != `["valueRecorded","valueRecorded","stepStarted"]` || show(types(cmds)) != `["stepCompleted","runCompleted"]` {
		t.Fatalf("got %s %s", show(io.checkpoints), show(cmds))
	}
	var ts, id string
	_ = json.Unmarshal(cp[0].Value, &ts)
	_ = json.Unmarshal(cp[1].Value, &id)
	if len(ts) != len("2026-10-10T12:00:00.000Z") || !strings.HasSuffix(ts, "Z") || ts[19] != '.' || len(id) != 36 || id[14] != '4' {
		t.Fatalf("now %q uuid %q", ts, id)
	}
}

func TestScheduleMetadata(t *testing.T) {
	wf := New("scheduled", func(ctx Context, _ none) (*ScheduleInfo, error) { return ctx.Run().Schedule, nil })
	inv := invocation("scheduled", nil)
	at := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	inv.Run.Schedule = &protocol.ScheduleInfo{ID: "0199a3d0-0000-7012-8000-000000000001", ScheduledTime: at}
	cmds := run(t, wf, inv, &fakeIO{})
	if show(cmds) != `[{"type":"runCompleted","output":{"id":"0199a3d0-0000-7012-8000-000000000001","scheduledTime":"2026-10-10T00:00:00Z"}}]` {
		t.Fatalf("got %s", show(cmds))
	}
	if cmds := run(t, wf, invocation("scheduled", nil), &fakeIO{}); show(cmds) != `[{"type":"runCompleted","output":null}]` {
		t.Fatalf("unscheduled: %s", show(cmds))
	}
}

func TestPanicInWorkflowFailsTheRun(t *testing.T) {
	wf := New("panics", func(ctx Context, _ none) (int, error) { panic("boom") })
	cmds := run(t, wf, invocation("panics", nil), &fakeIO{})
	if len(cmds) != 1 || cmds[0].Type != "runFailed" || cmds[0].Error.Code != "user" || cmds[0].Error.Name != "panic" || cmds[0].Error.Message != "boom" {
		t.Fatalf("got %s", show(cmds))
	}
	inBranch := New("panicbranch", func(ctx Context, _ none) (int, error) {
		return 0, Parallel(ctx, func(Context) error { panic("branch") })
	})
	cmds = run(t, inBranch, invocation("panicbranch", nil), &fakeIO{})
	if cmds[0].Type != "runFailed" || cmds[0].Error.Name != "panic" {
		t.Fatalf("got %s", show(cmds))
	}
}

func TestCallingStepsFromAnotherGoroutineFails(t *testing.T) {
	wf := New("misuse", func(ctx Context, _ none) (int, error) {
		parked, called := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(called)
			<-parked
			_, _ = step(ctx, "a", 1) // the workflow's goroutine is parked in Parallel: misuse
		}()
		err := Parallel(ctx, func(Context) error {
			close(parked)
			<-called
			return nil
		})
		return 0, err
	})
	for i := 0; i < 10; i++ {
		cmds := run(t, wf, invocation("misuse", nil), &fakeIO{})
		last := cmds[len(cmds)-1]
		if last.Type != "runFailed" || last.Error.Code != "user" || !strings.Contains(last.Error.Message, "workflows.Parallel") {
			t.Fatalf("got %s", show(cmds))
		}
	}
}

func TestArgumentErrorsFailTheRun(t *testing.T) {
	wf := New("widename", func(ctx Context, _ none) (int, error) { return step(ctx, strings.Repeat("é", 200), 1) })
	cmds := run(t, wf, invocation("widename", nil), &fakeIO{})
	if len(cmds) != 1 || cmds[0].Type != "runFailed" || cmds[0].Error.Code != "user" {
		t.Fatalf("got %s", show(cmds))
	}
	var err error
	outside := New("outside", func(ctx Context, _ none) (int, error) {
		_, err = Run[int](nil, "x", func(StepContext) (int, error) { return 1, nil })
		return 0, nil
	})
	run(t, outside, invocation("outside", nil), &fakeIO{})
	if err == nil {
		t.Fatal("a nil Context was accepted")
	}
}

func TestRecordedFailureIsTheCause(t *testing.T) {
	wf := New("flaky", func(ctx Context, _ none) (none, error) {
		_, err := Run(ctx, "flaky", func(StepContext) (int, error) { return 0, errors.New("boom") })
		return nil, err
	})
	failed := entry(1, "flaky", "run", "failed", 3, "")
	failed.Error = &protocol.WireError{Code: "user", Name: "Error", Message: "boom", Data: json.RawMessage(`{"n":1}`)}
	cmds := run(t, wf, invocation("flaky", nil, failed), &fakeIO{})
	if show(cmds) != `[{"type":"runFailed","error":{"code":"user","name":"Error","message":"boom","data":{"n":1}},"causeStepId":"`+protocol.StepID("flaky", 0)+`"}]` {
		t.Fatalf("got %s", show(cmds))
	}
	retrying := entry(1, "flaky", "run", "retrying", 1, "")
	retrying.Error = &protocol.WireError{Code: "user", Name: "Error", Message: "boom"}
	io := &fakeIO{}
	cmds = run(t, wf, invocation("flaky", nil, retrying), io)
	if io.checkpoints[0][0].Attempt != 2 || cmds[0].Attempt != 2 {
		t.Fatalf("retry attempt: %s", show(io.checkpoints))
	}
}

func TestJournalPagingAndPayloads(t *testing.T) {
	inv := invocation("sequence", nil, entry(1, "add", "run", "completed", 1, "0"))
	inv.Run.Input, inv.Run.InputRef = nil, "pay_input"
	inv.Journal.Complete = false
	second := protocol.JournalEntry{Seq: 2, StepID: protocol.StepID("add", 1), Name: "add", Kind: "run", Status: "completed", Attempt: 1, OutputRef: "pay_out"}
	io := &fakeIO{
		pages:    []protocol.Journal{{Complete: true, Entries: []protocol.JournalEntry{second}}},
		payloads: map[string]json.RawMessage{"pay_input": json.RawMessage(`{"N":2}`), "pay_out": json.RawMessage("1")},
	}
	if cmds := run(t, sequenceWF(), inv, io); show(cmds) != `[{"type":"runCompleted","output":1}]` {
		t.Fatalf("got %s", show(cmds))
	}
}

func TestGoroutinesEndAfterEveryInvocation(t *testing.T) {
	before := runtimeGoroutines()
	for i := 0; i < 20; i++ {
		run(t, fanoutWF(), invocation("fanout", nil), &fakeIO{})
		run(t, raceTimeoutWF(), invocation("racetimeout", nil, entry(1, "limit", "sleep", "completed", 1, ""), entry(2, "work", "run", "running", 1, "")), &fakeIO{})
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtimeGoroutines() > before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtimeGoroutines(); n > before+2 {
		t.Fatalf("%d goroutines, %d before", n, before)
	}
}

func TestWorkflowInfoCarriesTheFullPolicy(t *testing.T) {
	wf := New("tuned", func(ctx Context, _ none) (int, error) { return step(ctx, "t", 1) }, WithRetry(attempts(2)), WithTimeout(time.Minute))
	if got := show(wf.definition().info()); got != `{"name":"tuned","retry":{"maxAttempts":2,"initialMs":1000,"maxMs":600000,"multiplier":2},"timeoutMs":60000}` {
		t.Fatalf("got %s", got)
	}
	io := &fakeIO{}
	run(t, wf, invocation("tuned", nil), io)
	if r := io.checkpoints[0][0].Retry; r == nil || r.MaxAttempts != 2 || r.InitialMs != 1000 || io.checkpoints[0][0].TimeoutMs != 60000 {
		t.Fatalf("stepStarted %s", show(io.checkpoints[0][0]))
	}
	if plain := New("plain", func(ctx Context, _ none) (int, error) { return 0, nil }); show(plain.definition().info()) != `{"name":"plain"}` {
		t.Fatal(show(plain.definition().info()))
	}
}

func TestGroupsWithoutStepsAreDeterministic(t *testing.T) {
	wf := New("tie", func(ctx Context, _ none) (int, error) {
		if err := Parallel(ctx); err != nil {
			return 0, err
		}
		w, err := Race(ctx,
			func(Context) error { time.Sleep(time.Millisecond); return nil },
			func(Context) error { return nil },
		)
		if err != nil {
			return 0, err
		}
		// Nested: a race inside a parallel branch.
		var inner int
		err = Parallel(ctx,
			func(c Context) (err error) {
				inner, err = Race(c,
					func(cc Context) error { _, err := step(cc, "slow", 1); return err },
					func(Context) error { return nil },
				)
				return
			},
			func(c Context) error { _, err := step(c, "other", 2); return err },
		)
		return w*10 + inner, err
	})
	for i := 0; i < 30; i++ {
		cmds := run(t, wf, invocation("tie", nil, entry(1, "other", "run", "completed", 1, "2")), &fakeIO{})
		if show(cmds) != `[{"type":"runCompleted","output":1}]` {
			t.Fatalf("iteration %d: %s", i, show(cmds))
		}
	}
}

func TestRetryPolicyZeroesAreValues(t *testing.T) {
	p := DefaultRetry
	p.Initial, p.Max = 0, 0
	w, err := p.wire()
	if err != nil || w.InitialMs != 0 || w.MaxMs != 0 || w.MaxAttempts != 5 {
		t.Fatalf("%+v %v", w, err)
	}
	for name, bad := range map[string]RetryPolicy{
		"partial":         {MaxAttempts: 3},
		"sub-millisecond": {MaxAttempts: 3, Initial: time.Microsecond, Max: time.Second, Multiplier: 1},
		"max over a day":  {MaxAttempts: 3, Max: 25 * time.Hour, Multiplier: 1},
	} {
		if _, err := bad.wire(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	wf := New("zero", func(ctx Context, _ none) (int, error) {
		p := DefaultRetry
		p.Initial = 0
		return Run(ctx, "s", func(StepContext) (int, error) { return 1, nil }, WithStepRetry(p))
	})
	io := &fakeIO{}
	run(t, wf, invocation("zero", nil), io)
	if r := io.checkpoints[0][0].Retry; r == nil || r.InitialMs != 0 || r.MaxMs != 600000 {
		t.Fatalf("%s", show(io.checkpoints))
	}
	b, _ := json.Marshal(DefaultRetry)
	var back RetryPolicy
	if string(b) != `{"maxAttempts":5,"initialMs":1000,"maxMs":600000,"multiplier":2}` || json.Unmarshal(b, &back) != nil || back != DefaultRetry {
		t.Fatalf("%s %+v", b, back)
	}
}
