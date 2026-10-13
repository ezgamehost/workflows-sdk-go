package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

const (
	testOrg     = "0199a3c2-5b1e-7d40-9f3a-2c8e6b1d4f70"
	testProject = "0199a3c2-6c2f-7e51-8a4b-3d9f7c2e5a81"
	testApp     = "0199a3c2-7d30-7f62-9b5c-4eaf8d3f6b92"
)

// fakeLive is a workflows-live stand-in: scripted poll answers, heartbeats, checkpoints and
// completes, routed by token.
type fakeLive struct {
	t        *testing.T
	srv      *httptest.Server
	mu       sync.Mutex
	polls    []protocol.PollRequest
	rawPolls []map[string]json.RawMessage
	queue    [][]*protocol.Invocation // each poll takes the next batch (empty: 204)
	pollWait chan struct{}            // closed: polls with nothing queued answer 204 at once

	heartbeats  map[string]int
	cancelAfter int // answer heartbeats with cancel from this count (0: never)
	completes   map[string][]protocol.InvocationResult
	completed   chan string
	checkpoints map[string]int
	cpStatus    int

	pollStatus     int
	pollRetryAfter string
	pollTimes      []time.Time

	completeDelay           time.Duration // a complete takes this long to answer
	completing              map[string]bool
	heartbeatsAfterComplete map[string]int // heartbeats while a complete was in flight
	slowBody                time.Duration  // a poll answer's body arrives this long after its headers
	onHeaders               func()         // called once a poll answer's headers are sent
}

func newFakeLive(t *testing.T) *fakeLive {
	f := &fakeLive{t: t, heartbeats: map[string]int{}, completes: map[string][]protocol.InvocationResult{}, completed: make(chan string, 100),
		checkpoints: map[string]int{}, pollWait: make(chan struct{}), completing: map[string]bool{}, heartbeatsAfterComplete: map[string]int{}}
	f.srv = httptest.NewServer(f)
	t.Cleanup(func() { f.srv.Close() })
	return f
}

func (f *fakeLive) push(invs ...*protocol.Invocation) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, inv := range invs {
		inv.Endpoints.Live = f.srv.URL
	}
	f.queue = append(f.queue, invs)
}

func (f *fakeLive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	switch r.URL.Path {
	case "/v1/organizations/" + testOrg + "/projects/" + testProject + "/workers/poll":
		var pr protocol.PollRequest
		var raw map[string]json.RawMessage
		_ = json.Unmarshal(body, &pr)
		_ = json.Unmarshal(body, &raw)
		if err := pr.Validate(); err != nil {
			f.t.Errorf("an invalid poll: %v", err)
		}
		f.mu.Lock()
		f.polls = append(f.polls, pr)
		f.rawPolls = append(f.rawPolls, raw)
		f.pollTimes = append(f.pollTimes, time.Now())
		status, ra := f.pollStatus, f.pollRetryAfter
		var batch []*protocol.Invocation
		if len(f.queue) > 0 {
			batch, f.queue = f.queue[0], f.queue[1:]
		}
		f.mu.Unlock()
		if token != "ezgh_key" {
			w.WriteHeader(401)
			return
		}
		if status != 0 {
			if ra != "" {
				w.Header().Set("Retry-After", ra)
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"code":"x","message":"y"}}`))
			return
		}
		if batch == nil {
			select {
			case <-f.pollWait:
			case <-r.Context().Done():
			case <-time.After(100 * time.Millisecond):
			}
			w.WriteHeader(204)
			return
		}
		if f.onHeaders != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			_, _ = w.Write([]byte(" "))
			w.(http.Flusher).Flush()
			time.Sleep(50 * time.Millisecond) // the headers have reached the worker
			f.onHeaders()
			time.Sleep(f.slowBody)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"invocations": batch})
	case "/v1/invocations/heartbeat":
		f.mu.Lock()
		f.heartbeats[token]++
		n := f.heartbeats[token]
		if f.completing[token] {
			f.heartbeatsAfterComplete[token]++
		}
		f.mu.Unlock()
		if f.cancelAfter > 0 && n >= f.cancelAfter {
			_, _ = w.Write([]byte(`{"cancel":{"requestedAt":"2026-10-10T00:00:00Z","graceUntil":"2026-10-10T00:05:00Z"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"cancel":null}`))
	case "/v1/invocations/checkpoint":
		f.mu.Lock()
		f.checkpoints[token]++
		st := f.cpStatus
		f.mu.Unlock()
		if st != 0 {
			w.WriteHeader(st)
			return
		}
		w.WriteHeader(204)
	case "/v1/invocations/complete":
		f.mu.Lock()
		f.completing[token] = true
		f.mu.Unlock()
		time.Sleep(f.completeDelay)
		var res protocol.InvocationResult
		if err := json.Unmarshal(body, &res); err != nil {
			f.t.Errorf("complete: %v", err)
		}
		f.mu.Lock()
		f.completes[token] = append(f.completes[token], res)
		f.mu.Unlock()
		f.completed <- token
		w.WriteHeader(204)
	default:
		f.t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		w.WriteHeader(404)
	}
}

var invSeq atomic.Int32

func pulledInvocation(workflow string, input any) *protocol.Invocation {
	inv := invocation(workflow, input)
	n := invSeq.Add(1)
	inv.Token = inv.Token + "-" + string(rune('a'+n%26)) + strings.Repeat("x", int(n))
	return inv
}

func twoStep() Workflow {
	return New("twostep", func(ctx Context, in struct{ X int }) (int, error) {
		a, err := step(ctx, "a", in.X)
		if err != nil {
			return 0, err
		}
		return step(ctx, "b", a*2)
	})
}

func startWorker(t *testing.T, f *fakeLive, o WorkerOptions) (context.CancelFunc, chan error) {
	t.Helper()
	o.APIKey, o.OrganizationID, o.ProjectID, o.App, o.LiveURL = "ezgh_key", testOrg, testProject, testApp, f.srv.URL
	if o.Workflows == nil {
		o.Workflows = []Workflow{twoStep()}
	}
	if o.Deployment == "" {
		o.Deployment = "dpl_w"
	}
	w, err := NewWorker(o)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the worker didn't stop")
		}
	})
	return cancel, done
}

func waitComplete(t *testing.T, f *fakeLive) string {
	t.Helper()
	select {
	case tok := <-f.completed:
		return tok
	case <-time.After(5 * time.Second):
		t.Fatal("no complete")
		return ""
	}
}

func TestWorkerRunsPolledInvocationAndCompletes(t *testing.T) {
	f := newFakeLive(t)
	inv := pulledInvocation("twostep", map[string]int{"X": 3})
	f.push(inv)
	startWorker(t, f, WorkerOptions{})
	tok := waitComplete(t, f)
	f.mu.Lock()
	defer f.mu.Unlock()
	res := f.completes[tok][0]
	if tok != inv.Token || show(types(res.Commands)) != `["stepCompleted","runCompleted"]` || string(res.Commands[1].Output) != "6" || res.SDK.Name != SDKName || res.Deployment != "dpl_w" {
		t.Fatalf("%s %s", tok, show(res))
	}
}

func TestWorkerPollBody(t *testing.T) {
	f := newFakeLive(t)
	wf := New("tuned", func(ctx Context, _ none) (int, error) { return 1, nil }, WithRetry(attempts(2)))
	startWorker(t, f, WorkerOptions{Workflows: []Workflow{wf}, Queue: "conformance", WorkerID: "w-1", MaxTasks: 3})
	deadline := time.Now().Add(3 * time.Second)
	for {
		f.mu.Lock()
		n := len(f.rawPolls)
		f.mu.Unlock()
		if n >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.rawPolls) < 2 {
		t.Fatalf("%d polls", len(f.rawPolls))
	}
	for _, raw := range f.rawPolls[:2] {
		if len(raw) != 7 || string(raw["app"]) != `"`+testApp+`"` || string(raw["queue"]) != `"conformance"` || string(raw["maxTasks"]) != "3" ||
			string(raw["workerId"]) != `"w-1"` || string(raw["protocolVersions"]) != `["1.0","1.1"]` ||
			string(raw["workflows"]) != `[{"name":"tuned","retry":{"maxAttempts":2,"initialMs":1000,"maxMs":600000,"multiplier":2}}]` {
			t.Fatalf("poll %v", raw)
		}
	}
}

func TestWorkerHeartbeatsAndHonoursCancel(t *testing.T) {
	f := newFakeLive(t)
	f.cancelAfter = 2
	var stepCancelled atomic.Bool
	wf := New("long", func(ctx Context, _ none) (string, error) {
		return Run(ctx, "long", func(sc StepContext) (string, error) {
			select {
			case <-sc.Done():
				stepCancelled.Store(true)
				return "aborted", nil
			case <-time.After(5 * time.Second):
				return "done", nil
			}
		})
	})
	inv := pulledInvocation("long", nil)
	f.push(inv)
	startWorker(t, f, WorkerOptions{Workflows: []Workflow{wf}, HeartbeatInterval: 50 * time.Millisecond})
	tok := waitComplete(t, f)
	f.mu.Lock()
	defer f.mu.Unlock()
	if !stepCancelled.Load() || f.heartbeats[tok] < 2 || string(f.completes[tok][0].Commands[0].Output) != `"aborted"` {
		t.Fatalf("cancelled %v heartbeats %d %s", stepCancelled.Load(), f.heartbeats[tok], show(f.completes[tok]))
	}
}

func TestWorkerRespectsMaxConcurrent(t *testing.T) {
	f := newFakeLive(t)
	release := make(chan struct{})
	var running, peak atomic.Int32
	wf := New("block", func(ctx Context, _ none) (int, error) {
		return Run(ctx, "block", func(StepContext) (int, error) {
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-release
			running.Add(-1)
			return 1, nil
		})
	})
	for i := 0; i < 5; i++ {
		f.push(pulledInvocation("block", nil))
	}
	startWorker(t, f, WorkerOptions{Workflows: []Workflow{wf}, MaxConcurrent: 2, MaxTasks: 1})
	time.Sleep(400 * time.Millisecond)
	if p := peak.Load(); p != 2 {
		t.Fatalf("peak %d", p)
	}
	close(release)
	for i := 0; i < 5; i++ {
		waitComplete(t, f)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.polls {
		if p.MaxTasks > 1 {
			t.Fatalf("maxTasks %d", p.MaxTasks)
		}
	}
	if peak.Load() > 2 {
		t.Fatalf("peak %d", peak.Load())
	}
}

func TestWorkerAsksForNoMoreThanItsFreeSlots(t *testing.T) {
	f := newFakeLive(t)
	release := make(chan struct{})
	wf := New("block", func(ctx Context, _ none) (int, error) {
		return Run(ctx, "block", func(StepContext) (int, error) { <-release; return 1, nil })
	})
	f.push(pulledInvocation("block", nil), pulledInvocation("block", nil))
	startWorker(t, f, WorkerOptions{Workflows: []Workflow{wf}, MaxConcurrent: 3, MaxTasks: 10})
	time.Sleep(300 * time.Millisecond)
	f.mu.Lock()
	polls := append([]protocol.PollRequest(nil), f.polls...)
	f.mu.Unlock()
	close(release)
	if len(polls) < 2 || polls[0].MaxTasks != 3 || polls[1].MaxTasks != 1 {
		t.Fatalf("%+v", polls)
	}
}

func TestWorkerStopsOnRefusal(t *testing.T) {
	for _, status := range []int{401, 403, 404} {
		f := newFakeLive(t)
		f.pollStatus = status
		_, done := startWorker(t, f, WorkerOptions{})
		select {
		case err := <-done:
			var ae *APIError
			if !errors.As(err, &ae) || ae.Status != status {
				t.Fatalf("%d: %v", status, err)
			}
			done <- err // for the cleanup
		case <-time.After(3 * time.Second):
			t.Fatalf("%d: Run didn't return", status)
		}
		f.mu.Lock()
		n := len(f.polls)
		f.mu.Unlock()
		if n != 1 {
			t.Fatalf("%d: %d polls", status, n)
		}
	}
}

func TestWorkerBacksOffOn503(t *testing.T) {
	f := newFakeLive(t)
	f.pollStatus, f.pollRetryAfter = 503, "1"
	startWorker(t, f, WorkerOptions{MaxPollBackoff: 100 * time.Millisecond})
	time.Sleep(1300 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pollTimes) < 2 || f.pollTimes[1].Sub(f.pollTimes[0]) < time.Second {
		t.Fatalf("polls %v", f.pollTimes)
	}
}

func TestWorkerDrainsOnShutdown(t *testing.T) {
	f := newFakeLive(t)
	started := make(chan struct{})
	wf := New("slowstep", func(ctx Context, _ none) (int, error) {
		return Run(ctx, "slow", func(StepContext) (int, error) { close(started); time.Sleep(300 * time.Millisecond); return 1, nil })
	})
	f.push(pulledInvocation("slowstep", nil))
	cancel, done := startWorker(t, f, WorkerOptions{Workflows: []Workflow{wf}})
	<-started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		done <- nil
	case <-time.After(5 * time.Second):
		t.Fatal("Run didn't return")
	}
	select {
	case <-f.completed:
	default:
		t.Fatal("Run returned before the running invocation completed")
	}
}

func TestWorkerDropsASupersededInvocation(t *testing.T) {
	f := newFakeLive(t)
	f.cpStatus = 409
	f.push(pulledInvocation("twostep", map[string]int{"X": 1}))
	startWorker(t, f, WorkerOptions{})
	time.Sleep(500 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.completes) != 0 || len(f.checkpoints) != 1 {
		t.Fatalf("completes %v checkpoints %v", f.completes, f.checkpoints)
	}
}

func TestDevModeQueueName(t *testing.T) {
	t.Setenv("USER", "Ada.Lovelace")
	f := newFakeLive(t)
	w, err := NewWorker(WorkerOptions{APIKey: "k", OrganizationID: testOrg, ProjectID: testProject, App: testApp, Workflows: []Workflow{twoStep()}, DevMode: true, LiveURL: f.srv.URL})
	if err != nil || w.Queue() != "dev-ada-lovelace" {
		t.Fatalf("%v %v", w, err)
	}
	if q := DevQueue("__Mr_X__"); q != "dev-mr-x" {
		t.Fatal(q)
	}
	if q := DevQueue("---"); q != "dev-dev" {
		t.Fatal(q)
	}
	if q := DevQueue(strings.Repeat("a", 100)); len(q) != 62 || !protocol.ValidQueue(q) {
		t.Fatal(q)
	}
}

func TestNewWorkerChecksThePoll(t *testing.T) {
	base := WorkerOptions{APIKey: "k", OrganizationID: testOrg, ProjectID: testProject, App: testApp, Workflows: []Workflow{twoStep()}}
	for name, mod := range map[string]func(*WorkerOptions){
		"app":       func(o *WorkerOptions) { o.App = "not-a-uuid" },
		"queue":     func(o *WorkerOptions) { o.Queue = "Bad_Queue" },
		"maxTasks":  func(o *WorkerOptions) { o.MaxTasks = 11 },
		"worker ID": func(o *WorkerOptions) { o.WorkerID = "has space" },
		"no key":    func(o *WorkerOptions) { o.APIKey = "" },
	} {
		o := base
		mod(&o)
		if _, err := NewWorker(o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	base.APIKey = "ezgh_SECRETSECRET"
	w, err := NewWorker(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{fmt.Sprint(w), fmt.Sprintf("%+v", w), fmt.Sprintf("%#v", w)} {
		if strings.Contains(s, "SECRETSECRET") {
			t.Fatalf("printed: %s", s)
		}
	}
}

// A poll the gateway can't start answers 503 with Retry-After: 1; 429 and other 5xx back off
// too. None of them may hot-loop.
func TestWorkerNeverHotLoopsOnRetryableAnswers(t *testing.T) {
	for _, c := range []struct {
		status     int
		retryAfter string
	}{{503, "1"}, {503, ""}, {429, "1"}, {500, ""}, {502, ""}} {
		f := newFakeLive(t)
		f.pollStatus, f.pollRetryAfter = c.status, c.retryAfter
		cancel, done := startWorker(t, f, WorkerOptions{})
		time.Sleep(2500 * time.Millisecond)
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("%d: Run returned %v; a retryable answer must not stop the worker", c.status, err)
		}
		done <- nil
		f.mu.Lock()
		n := len(f.polls)
		f.mu.Unlock()
		// 2.5 s: Retry-After 1 allows at most 3 polls; backoff without it (250–500 ms, doubling)
		// at most 5.
		limit := 5
		if c.retryAfter == "1" {
			limit = 3
		}
		if n < 2 || n > limit {
			t.Fatalf("%d (Retry-After %q): %d polls in 2.5 s", c.status, c.retryAfter, n)
		}
	}
}

// ctx.Run().Traceparent is the pulled invocation's traceparent field; "" without it.
func TestWorkerTraceparentFromField(t *testing.T) {
	f := newFakeLive(t)
	const tp = "00-0199a3d02b3c7d4e9f506b7c8d9e0f1a-0123456789abcdef-01"
	with, without := pulledInvocation("traced", nil), pulledInvocation("traced", nil)
	with.Traceparent = tp
	f.push(with, without)
	traced := New("traced", func(ctx Context, _ none) (string, error) { return ctx.Run().Traceparent, nil })
	startWorker(t, f, WorkerOptions{Workflows: []Workflow{traced}, MaxConcurrent: 2})
	waitComplete(t, f)
	waitComplete(t, f)
	f.mu.Lock()
	defer f.mu.Unlock()
	for tok, want := range map[string]string{with.Token: `"` + tp + `"`, without.Token: `""`} {
		res := f.completes[tok]
		if len(res) != 1 || len(res[0].Commands) != 1 || string(res[0].Commands[0].Output) != want {
			t.Fatalf("%s: %s", tok, show(res))
		}
	}
}
