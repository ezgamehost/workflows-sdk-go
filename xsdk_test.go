package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// #4: while cancelling, a cleanup step doesn't wait behind a sibling the engine owns.
func TestCancelCleanupDoesntWaitBehindARunningSibling(t *testing.T) {
	wf := New("cc", func(ctx Context, _ none) (string, error) {
		_, err := Race(ctx,
			func(c Context) error { _, e := step(c, "a", 1); return e },
			func(c Context) error { _, e := step(c, "b", 2); return e })
		_, _ = step(ctx, "cleanup", "cleaned")
		return "", err
	})
	inv := invocation("cc", nil, entry(1, "a", "run", "running", 0, ""), entry(2, "b", "run", "running", 0, ""))
	inv.Cancel = &protocol.Cancel{RequestedAt: time.Now(), GraceUntil: time.Now().Add(time.Minute)}
	io := &fakeIO{}
	cmds := run(t, wf, inv, io)
	if len(io.checkpoints) != 1 || io.checkpoints[0][0].Name != "cleanup" || show(types(cmds)) != `["stepCompleted","runFailed"]` {
		t.Fatalf("checkpoints %s cmds %s", show(io.checkpoints), show(cmds))
	}
	// A non-run call anywhere is refused at once while cancelling.
	wf2 := New("cc2", func(ctx Context, _ none) (string, error) {
		err := Parallel(ctx,
			func(c Context) error { _, e := step(c, "a", 1); return e },
			func(c Context) error {
				if e := Sleep(c, "nap", time.Hour); !errors.Is(e, ErrCancelled) {
					return errors.New("the sleep wasn't refused")
				}
				_, e := step(c, "cleanup", "cleaned")
				return e
			})
		return "", err
	})
	inv = invocation("cc2", nil, entry(1, "a", "run", "running", 0, ""))
	inv.Cancel = &protocol.Cancel{RequestedAt: time.Now(), GraceUntil: time.Now().Add(time.Minute)}
	io = &fakeIO{}
	cmds = run(t, wf2, inv, io)
	// The cancel goes to the oldest call (a, which the engine owns); the sleep is refused at once,
	// and the cleanup runs.
	if len(io.checkpoints) != 1 || io.checkpoints[0][0].Name != "cleanup" || show(types(cmds)) != `["stepCompleted","runFailed"]` ||
		cmds[1].Error.Code != "workflows.cancelled" {
		t.Fatalf("checkpoints %s cmds %s", show(io.checkpoints), show(cmds))
	}
}

// #3: step timeouts up to 24 hours; a workflow's above the handler's MaxInvocationSeconds is refused.
func TestLongTimeouts(t *testing.T) {
	long := New("long", func(ctx Context, _ none) (int, error) {
		return Run(ctx, "s", func(StepContext) (int, error) { return 1, nil }, WithStepTimeout(2*time.Hour))
	}, WithTimeout(time.Hour))
	if got := long.definition().info().TimeoutMs; got != 3_600_000 {
		t.Fatal(got)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("WithTimeout(25h) accepted")
			}
		}()
		New("too", func(ctx Context, _ none) (int, error) { return 0, nil }, WithTimeout(25*time.Hour))
	}()
	if _, err := Handler(HandlerOptions{Workflows: []Workflow{long}, SigningSecrets: []string{testSecret}, MaxInvocationSeconds: 900}); err == nil {
		t.Fatal("a push handler accepted a workflow timeout longer than MaxInvocationSeconds")
	}
	if _, err := NewWorker(WorkerOptions{APIKey: "k", OrganizationID: testOrg, ProjectID: testProject, App: testApp, Workflows: []Workflow{long}}); err != nil {
		t.Fatal(err)
	}
}

// #9
func TestHeartbeatIntervalAtMost30s(t *testing.T) {
	if _, err := NewWorker(WorkerOptions{APIKey: "k", OrganizationID: testOrg, ProjectID: testProject, App: testApp, Workflows: []Workflow{twoStep()}, HeartbeatInterval: 31 * time.Second}); err == nil {
		t.Fatal("31 s accepted")
	}
}

// #8: heartbeats continue while complete is in flight.
func TestWorkerHeartbeatsUntilCompleteReturns(t *testing.T) {
	f := newFakeLive(t)
	f.completeDelay = 400 * time.Millisecond
	inv := pulledInvocation("twostep", map[string]int{"X": 1})
	f.push(inv)
	startWorker(t, f, WorkerOptions{HeartbeatInterval: 50 * time.Millisecond})
	tok := waitComplete(t, f)
	f.mu.Lock()
	beats := f.heartbeatsAfterComplete[tok]
	f.mu.Unlock()
	if beats < 2 {
		t.Fatalf("%d heartbeats while complete was in flight", beats)
	}
}

// #16: a poll whose answer has started arriving isn't aborted by shutdown; its tasks run.
func TestWorkerFinishesAnArrivingPollAtShutdown(t *testing.T) {
	f := newFakeLive(t)
	f.push(pulledInvocation("twostep", map[string]int{"X": 1}))
	f.slowBody = 300 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	f.onHeaders = cancel
	w, err := NewWorker(WorkerOptions{APIKey: "ezgh_key", OrganizationID: testOrg, ProjectID: testProject, App: testApp, LiveURL: f.srv.URL, Workflows: []Workflow{twoStep()}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.completed:
	default:
		t.Fatal("the arriving poll's task was dropped")
	}
}

// #6: a redirect is an error everywhere, never followed.
func TestRedirectsAreErrors(t *testing.T) {
	var hits atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}))
	defer s.Close()
	c, _ := NewClient(ClientOptions{APIKey: "k", OrganizationID: "o", ProjectID: "p", BaseURL: s.URL})
	_, err := c.Start(context.Background(), "a", "w", nil, StartOptions{})
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != 307 || hits.Load() != 1 {
		t.Fatalf("%v after %d requests", err, hits.Load())
	}
	user := &http.Client{}
	c, _ = NewClient(ClientOptions{APIKey: "k", OrganizationID: "o", ProjectID: "p", BaseURL: s.URL, HTTPClient: user})
	if _, err := c.Get(context.Background(), "r"); !errors.As(err, &ae) || ae.Status != 307 || user.CheckRedirect != nil {
		t.Fatalf("%v (the caller's client must not be modified)", err)
	}
	if err := CompleteCallbackURL(context.Background(), nil, s.URL+"/public/v1/callbacks/x", 1); !errors.As(err, &ae) || ae.Status != 307 {
		t.Fatalf("%v", err)
	}
}

// #1 and #14 and the shared gap.
func TestClientRetrySemantics(t *testing.T) {
	var n atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k := n.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, "/rotate-secret"):
			_, _ = w.Write([]byte(`{"app":{"id":"a1","kind":"push"},"signingSecret":null}`))
		case k == 1:
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(409)
			_, _ = w.Write([]byte(`{"error":{"code":"idempotency_in_progress","message":"wait"}}`))
		default:
			_, _ = w.Write([]byte(`{"id":"r1"}`))
		}
	}))
	defer s.Close()
	c, _ := NewClient(ClientOptions{APIKey: "k", OrganizationID: "o", ProjectID: "p", BaseURL: s.URL, MaxRetryDelay: 50 * time.Millisecond})
	if r, err := c.Get(context.Background(), "r1"); err != nil || r.ID != "r1" || n.Load() != 2 {
		t.Fatalf("idempotency_in_progress isn't retried: %v after %d", err, n.Load())
	}
	var ae *APIError
	if _, err := c.RotateSecret(context.Background(), "a1"); !errors.As(err, &ae) || ae.Code != "outcome_unknown" {
		t.Fatalf("rotate without a secret: %v", err)
	}
	// Retries: nil is the default (3), 0 is none.
	var m atomic.Int32
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { m.Add(1); w.WriteHeader(503) }))
	defer down.Close()
	none := 0
	c, _ = NewClient(ClientOptions{APIKey: "k", OrganizationID: "o", ProjectID: "p", BaseURL: down.URL, Retries: &none})
	_, _ = c.Get(context.Background(), "r")
	if m.Load() != 1 {
		t.Fatalf("Retries 0 sent %d requests", m.Load())
	}
	// A 503 on CreateApp is outcome_unknown even with no retries left.
	if _, err := c.CreateApp(context.Background(), "a", CreateAppOptions{}); !errors.As(err, &ae) || ae.Code != "outcome_unknown" {
		t.Fatalf("%v", err)
	}
	m.Store(0)
	c, _ = NewClient(ClientOptions{APIKey: "k", OrganizationID: "o", ProjectID: "p", BaseURL: down.URL, MaxRetryDelay: time.Millisecond})
	_, _ = c.Get(context.Background(), "r")
	if m.Load() != 4 {
		t.Fatalf("the default sent %d requests, want 4", m.Load())
	}
}

// #11 and #12.
func TestDevAppAndAPIParity(t *testing.T) {
	var created atomic.Int32
	apps := `{"apps":[{"id":"x","name":"other","kind":"push"},{"id":"a9","name":"dev-ada","kind":"pull","queue":"dev-ada"}],"nextCursor":null}`
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/organizations/o/projects/p/apps":
			created.Add(1)
			w.WriteHeader(409)
			_, _ = w.Write([]byte(`{"error":{"code":"app_exists","message":"exists"}}`))
		case r.URL.Path == "/v1/organizations/o/projects/p/apps":
			_, _ = w.Write([]byte(apps))
		case strings.HasSuffix(r.URL.Path, "/payloads/input"):
			_, _ = w.Write([]byte(`{"n":1}`))
		case strings.Contains(r.URL.Path, "/callbacks/"):
			_, _ = w.Write([]byte(`{"id":"r1","status":"running"}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer s.Close()
	c, _ := NewClient(ClientOptions{APIKey: "k", OrganizationID: "o", ProjectID: "p", BaseURL: s.URL})
	app, err := c.DevApp(context.Background(), DevAppOptions{User: "Ada"})
	if err != nil || app.ID != "a9" || created.Load() != 1 {
		t.Fatalf("%+v %v", app, err)
	}
	apps = `{"apps":[{"id":"a9","name":"dev-ada","kind":"push"}],"nextCursor":null}`
	var ae *APIError
	if _, err := c.DevApp(context.Background(), DevAppOptions{User: "Ada"}); !errors.As(err, &ae) || ae.Code != "app_exists" {
		t.Fatalf("a push app named dev-ada: %v", err)
	}
	page, err := c.ListApps(context.Background(), ListOptions{Limit: 10})
	if err != nil || len(page.Apps) != 1 {
		t.Fatalf("%+v %v", page, err)
	}
	var in map[string]int
	if err := c.ScheduleInput(context.Background(), "s1", &in); err != nil || in["n"] != 1 {
		t.Fatalf("%v %v", in, err)
	}
	if r, err := c.CompleteCallback(context.Background(), "r1", "s1", "yes"); err != nil || r.ID != "r1" {
		t.Fatalf("%+v %v", r, err)
	}
}

// #12: explicit IDs on CreateCallback and Invoke.
func TestExplicitIDsOnCallbacksAndInvoke(t *testing.T) {
	wf := New("ids", func(ctx Context, _ none) (int, error) {
		cb, err := CreateCallback(ctx, "approval", time.Hour, WithID("cb-1"))
		if err != nil {
			return 0, err
		}
		_ = cb
		return Invoke[int](ctx, "child", "kid", nil, WithID("child-1"))
	})
	io := &fakeIO{}
	cmds := run(t, wf, invocation("ids", nil), io)
	if io.checkpoints[0][0].StepID != "cb-1" || cmds[0].Type != "invoke" || cmds[0].StepID != "child-1" {
		t.Fatalf("%s %s", show(io.checkpoints), show(cmds))
	}
	bad := New("bad", func(ctx Context, _ none) (int, error) {
		return Invoke[int](ctx, "child", "kid", nil, WithStepTimeout(time.Second))
	})
	if cmds := run(t, bad, invocation("bad", nil), &fakeIO{}); cmds[0].Type != "runFailed" {
		t.Fatalf("%s", show(cmds))
	}
}

var _ = json.Marshal

// #6: workflows-live redirecting a checkpoint fails the invocation; it isn't followed.
func TestLiveRedirectIsNotFollowed(t *testing.T) {
	var hits atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer s.Close()
	h := mustHandler(t, HandlerOptions{})
	code, out, _ := serve(h, signed(t, testSecret, time.Now(), orchestrateBody(t, s.URL, "sequence", time.Now().Add(time.Minute))))
	if code != 503 || out["code"] != "checkpoint_failed" || hits.Load() != 1 {
		t.Fatalf("%d %v after %d requests", code, out, hits.Load())
	}
}

// All returns at the first error; cleanup then runs while a sibling is still in flight.
func TestAllFailsFast(t *testing.T) {
	wf := New("cancelfanout", func(ctx Context, _ none) (string, error) {
		err := All(ctx,
			func(c Context) error { _, e := step(c, "p0", 0); return e },
			func(c Context) error { _, e := step(c, "p1", 1); return e })
		if _, cerr := step(ctx, "cleanup", "cleaned"); cerr != nil {
			return "", cerr
		}
		if err != nil {
			return "", err
		}
		return "done", nil
	})
	inv := invocation("cancelfanout", nil, entry(1, "p0", "run", "running", 0, ""), entry(2, "p1", "run", "running", 1, ""))
	inv.Cancel = &protocol.Cancel{RequestedAt: time.Now(), GraceUntil: time.Now().Add(time.Minute)}
	io := &fakeIO{}
	cmds := run(t, wf, inv, io)
	if len(io.checkpoints) != 1 || io.checkpoints[0][0].Name != "cleanup" || show(types(cmds)) != `["stepCompleted","runFailed"]` || cmds[1].Error.Name != "CancelledError" {
		t.Fatalf("%s %s", show(io.checkpoints), show(cmds))
	}
	// With no error, All waits for every branch.
	ok := New("allok", func(ctx Context, _ none) (int, error) {
		var a, b int
		err := All(ctx,
			func(c Context) (e error) { a, e = step(c, "a", 1); return },
			func(c Context) (e error) { b, e = step(c, "b", 2); return })
		return a + b, err
	})
	if cmds := run(t, ok, invocation("allok", nil, entry(1, "a", "run", "completed", 1, "1"), entry(2, "b", "run", "completed", 1, "2")), &fakeIO{}); show(cmds) != `[{"type":"runCompleted","output":3}]` {
		t.Fatalf("%s", show(cmds))
	}
}
