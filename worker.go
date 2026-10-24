package workflows

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/jsonx"
	"github.com/ezgamehost/workflows-sdk-go/internal/live"
	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// DefaultLiveURL is workflows-live, where pull workers poll.
const DefaultLiveURL = "https://workflows-live.ezghcloud.com"

// WorkerOptions configures a pull worker.
type WorkerOptions struct {
	// APIKey is an EZGH API key allowed workflows.workers.poll (and workflows.apps.sync, to
	// announce the workflows). Omitted on an EZGH compute instance with a Bot attached: the worker
	// then uses the Bot's short-lived key from the metadata service.
	APIKey         string
	OrganizationID string
	ProjectID      string
	// App is the pull app's ID (Client.CreateApp with Kind "pull").
	App string
	// Queue is the app's task queue (default "default"; ^[a-z0-9][a-z0-9-]{0,62}$).
	Queue string
	// DevMode polls the queue dev-<user> instead ($USER, else the OS user), so developers sharing
	// a project never take each other's runs.
	DevMode   bool
	Workflows []Workflow
	// MaxTasks is how many tasks one poll asks for (1–10, default 4), never more than the free
	// slots.
	MaxTasks int
	// MaxConcurrent is how many invocations run at once (default 10).
	MaxConcurrent int
	// WorkerID names this worker in the app's announcements (default host-random;
	// ^[A-Za-z0-9._:-]{1,128}$).
	WorkerID   string
	LiveURL    string       // default DefaultLiveURL
	HTTPClient *http.Client // default: a client with a 70 s timeout (a poll is held up to 55 s)
	Deployment string       // default: detected
	// HeartbeatInterval is how often a running invocation heartbeats (default 20 s; the engine's
	// heartbeat timeout is 60 s).
	HeartbeatInterval time.Duration
	// MaxPollBackoff is the longest wait between failed polls (default 30 s).
	MaxPollBackoff time.Duration
	// ShutdownTimeout is how long Run waits for running invocations once its context is done
	// (default 30 s). After that their steps' contexts are cancelled.
	ShutdownTimeout time.Duration
	// MaxStepsPerInvocation is how many steps run inline per invocation (default 50).
	MaxStepsPerInvocation int
	// APIBaseURL builds callback URLs when an invocation carries no endpoints.api.
	APIBaseURL string
	Logger     *slog.Logger
}

// Worker is a pull worker: it long-polls workflows-live for invocations of one pull app's task
// queue, runs them, heartbeats while they run, and completes them.
type Worker struct {
	o        WorkerOptions
	cred     credential
	reauthed bool // Run: an instance key the API refused was replaced, once
	byName   map[string]*definition
	infos    []protocol.WorkflowInfo
	pollURL  string
	sem      chan struct{}
	wg       sync.WaitGroup
	log      *slog.Logger
	running  atomic.Bool
	failures int
}

func (w *Worker) String() string             { return "workflows.Worker{" + w.o.App + "}" }
func (w *Worker) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, w.String()) }
func (w *Worker) GoString() string           { return w.String() }
func (w *Worker) LogValue() slog.Value       { return slog.StringValue(w.String()) }

var notQueueChars = regexp.MustCompile(`[^a-z0-9]+`)

// DevQueue is the dev-mode queue for user: dev-<user> in lowercase letters, digits and -.
func DevQueue(name string) string {
	clean := notQueueChars.ReplaceAllString(strings.ToLower(name), "-")
	if len(clean) > 58 {
		clean = clean[:58]
	}
	clean = strings.Trim(clean, "-")
	if clean == "" {
		clean = "dev"
	}
	return "dev-" + clean
}

func devUser() string {
	for _, k := range []string{"USER", "USERNAME", "LOGNAME"} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	if u, err := user.Current(); err == nil {
		return u.Username
	}
	return ""
}

func defaultWorkerID() string {
	host, _ := os.Hostname()
	host = strings.Trim(notQueueChars.ReplaceAllString(strings.ToLower(host), "-"), "-")
	if len(host) > 40 {
		host = host[:40]
	}
	if host == "" {
		host = "worker"
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	return host + "-" + hex.EncodeToString(b[:])
}

// NewWorker checks the options and returns a worker; start it with Run.
func NewWorker(o WorkerOptions) (*Worker, error) {
	cred, ok := newCredential(o.APIKey)
	if !ok || o.OrganizationID == "" || o.ProjectID == "" || o.App == "" {
		return nil, errors.New("workflows.NewWorker needs APIKey, OrganizationID, ProjectID and App")
	}
	byName, order, err := definitions(o.Workflows, "workflows.NewWorker")
	if err != nil {
		return nil, err
	}
	if o.DevMode {
		o.Queue = DevQueue(devUser())
	} else if o.Queue == "" {
		o.Queue = "default"
	}
	if o.MaxTasks == 0 {
		o.MaxTasks = 4
	}
	if o.MaxConcurrent == 0 {
		o.MaxConcurrent = 10
	}
	if o.MaxConcurrent < 1 {
		return nil, errors.New("workflows.NewWorker: MaxConcurrent is at least 1")
	}
	if o.WorkerID == "" {
		o.WorkerID = defaultWorkerID()
	}
	if o.LiveURL == "" {
		o.LiveURL = DefaultLiveURL
	}
	o.HTTPClient = noRedirects(o.HTTPClient)
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = 20 * time.Second
	}
	if o.HeartbeatInterval > 30*time.Second {
		return nil, errors.New("workflows.NewWorker: HeartbeatInterval is at most 30 s (the engine's heartbeat timeout is 60 s)")
	}
	if o.MaxPollBackoff <= 0 {
		o.MaxPollBackoff = 30 * time.Second
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 30 * time.Second
	}
	if o.MaxStepsPerInvocation < 0 {
		return nil, errors.New("workflows.NewWorker: MaxStepsPerInvocation is at least 1")
	}
	if o.Deployment == "" {
		o.Deployment = detectDeployment()
	}
	o.Deployment = protocol.CleanDeployment(o.Deployment)
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	w := &Worker{o: o, cred: cred, byName: byName, sem: make(chan struct{}, o.MaxConcurrent), log: o.Logger}
	w.o.APIKey = ""
	for _, d := range order {
		w.infos = append(w.infos, d.info())
	}
	// The poll the gateway will see: refused here rather than by the gateway.
	if err := w.request(1).Validate(); err != nil {
		return nil, fmt.Errorf("workflows.NewWorker: %w (App is the pull app's ID; Queue matches ^[a-z0-9][a-z0-9-]{0,62}$; MaxTasks is 1–10; WorkerID matches ^[A-Za-z0-9._:-]{1,128}$)", err)
	}
	if err := w.request(o.MaxTasks).Validate(); err != nil {
		return nil, fmt.Errorf("workflows.NewWorker: %w", err)
	}
	w.pollURL = strings.TrimRight(o.LiveURL, "/") + "/v1/organizations/" + url.PathEscape(o.OrganizationID) + "/projects/" + url.PathEscape(o.ProjectID) + "/workers/poll"
	return w, nil
}

func (w *Worker) request(maxTasks int) protocol.PollRequest {
	return protocol.PollRequest{App: w.o.App, Queue: w.o.Queue, MaxTasks: maxTasks, WorkerID: w.o.WorkerID,
		ProtocolVersions: protocol.Versions, SDK: protocol.SDKInfo{Name: SDKName, Version: SDKVersion}, Workflows: w.infos}
}

// Queue is the task queue the worker polls (dev-<user> in dev mode).
func (w *Worker) Queue() string { return w.o.Queue }

// Run polls and runs invocations until ctx is done, then waits up to ShutdownTimeout for running
// invocations and returns nil. It returns an *APIError at once when workflows-live refuses the
// poll for good: 401 or 403 (the API key or its permissions), or another 4xx (the app or the
// queue). 429, 5xx and network errors are retried with backoff.
func (w *Worker) Run(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return errors.New("workflows: this worker is already running")
	}
	defer w.running.Store(false)
	execCtx, stopExec := context.WithCancel(context.WithoutCancel(ctx))
	defer stopExec()
	var fatal error
	for ctx.Err() == nil {
		free := cap(w.sem) - len(w.sem)
		if free == 0 {
			select {
			case w.sem <- struct{}{}:
				<-w.sem
			case <-ctx.Done():
			}
			continue
		}
		invs, receivedAt, serverDate, err := w.poll(ctx, min(w.o.MaxTasks, free))
		if err != nil && ctx.Err() != nil {
			break
		}
		var ae *APIError
		if errors.As(err, &ae) && ae.Status == http.StatusUnauthorized && w.cred.inst != nil && !w.reauthed {
			w.reauthed = true // the instance key was replaced in poll: poll again once
			continue
		}
		if errors.As(err, &ae) && ae.Status != http.StatusTooManyRequests && ae.Status < 500 {
			fatal = err
			break
		}
		if err != nil {
			wait := w.backoff()
			if errors.As(err, &ae) && ae.RetryAfter > wait {
				wait = ae.RetryAfter
			}
			w.log.Warn("workflows: poll failed; retrying", "error", err, "wait", wait)
			sleepCtx(ctx, wait)
			continue
		}
		w.failures, w.reauthed = 0, false
		for i := range invs {
			inv := invs[i]
			w.sem <- struct{}{}
			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				defer func() { <-w.sem }()
				w.handle(execCtx, inv, receivedAt, serverDate)
			}()
		}
		// Tasks a poll returned are started even when ctx ended meanwhile: they're ours now, and
		// the shutdown below waits for them.
	}
	if !w.wait(w.o.ShutdownTimeout) {
		stopExec()
		w.wait(5 * time.Second)
	}
	return fatal
}

func (w *Worker) backoff() time.Duration {
	d := min(w.o.MaxPollBackoff, 500*time.Millisecond<<min(w.failures, 16))
	w.failures++
	// 50–100 % of it, so a fleet of workers doesn't poll in step.
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(d/2)+1))
	return d/2 + time.Duration(n.Int64())
}

func (w *Worker) wait(d time.Duration) bool {
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

func (w *Worker) poll(ctx context.Context, maxTasks int) ([]json.RawMessage, time.Time, time.Time, error) {
	pr := w.request(maxTasks)
	if err := pr.Validate(); err != nil {
		return nil, time.Time{}, time.Time{}, &APIError{Status: http.StatusBadRequest, Code: "invalid_poll", Message: err.Error()}
	}
	body, err := jsonx.Marshal(pr)
	if err != nil {
		return nil, time.Time{}, time.Time{}, err
	}
	// Shutdown aborts a poll that's still waiting, but not one whose answer has started to
	// arrive: its tasks are leased to this worker, so it reads them (within ShutdownTimeout) and
	// runs them.
	pctx, pcancel := context.WithCancel(context.WithoutCancel(ctx))
	defer pcancel()
	arrived, finished := make(chan struct{}), make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case <-finished:
			return
		case <-ctx.Done():
		case <-arrived:
			select {
			case <-finished:
				return
			case <-ctx.Done():
			}
			t := time.NewTimer(w.o.ShutdownTimeout)
			defer t.Stop()
			select {
			case <-finished:
				return
			case <-t.C:
			}
		}
		pcancel()
	}()
	req, err := http.NewRequestWithContext(pctx, http.MethodPost, w.pollURL, bytes.NewReader(body))
	if err != nil {
		return nil, time.Time{}, time.Time{}, &APIError{Status: http.StatusBadRequest, Code: "invalid_live_url", Message: err.Error()}
	}
	apiKey, err := w.cred.key(ctx)
	if err != nil {
		return nil, time.Time{}, time.Time{}, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := w.o.HTTPClient.Do(req)
	if err != nil {
		return nil, time.Time{}, time.Time{}, err
	}
	close(arrived)
	defer res.Body.Close()
	receivedAt := time.Now()
	data, err := io.ReadAll(io.LimitReader(res.Body, 64<<20))
	if res.StatusCode == http.StatusNoContent {
		return nil, receivedAt, time.Time{}, nil
	}
	if res.StatusCode != http.StatusOK {
		if res.StatusCode == http.StatusUnauthorized {
			w.cred.refused(apiKey)
		}
		return nil, receivedAt, time.Time{}, apiError(res, data)
	}
	if err != nil {
		return nil, receivedAt, time.Time{}, err
	}
	var resp struct {
		Invocations []json.RawMessage `json:"invocations"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, receivedAt, time.Time{}, fmt.Errorf("workflows-live's poll answer isn't JSON: %w", err)
	}
	serverDate, _ := http.ParseTime(res.Header.Get("Date"))
	return resp.Invocations, receivedAt, serverDate, nil
}

// handle runs one pulled invocation: no signature (it came on our authenticated poll), the
// deadline as sent (or by the poll answer's Date, when this clock is behind), heartbeats while
// it runs, then complete. A 409 anywhere drops it; a checkpoint that failed leaves it to the
// engine's heartbeat and deadline timers.
func (w *Worker) handle(ctx context.Context, raw json.RawMessage, receivedAt, serverDate time.Time) {
	inv, err := parseInvocation(raw)
	if err != nil {
		w.log.Warn("workflows: an invalid invocation was polled", "error", err)
		return
	}
	if inv.Kind == protocol.InvocationSync {
		return
	}
	deadline := inv.Deadline
	if !serverDate.IsZero() {
		if local := receivedAt.Add(inv.Deadline.Sub(serverDate)); local.Before(deadline) {
			deadline = local
		}
	}
	reserve := max(5*time.Second, max(0, deadline.Sub(receivedAt))/10)
	lc := &live.Client{Base: inv.Endpoints.Live, Token: inv.Token, HTTP: w.o.HTTPClient, Deadline: deadline, Reserve: reserve}

	cancelled := make(chan struct{})
	var once sync.Once
	var superseded atomic.Bool
	hbCtx, stopHB := context.WithCancel(ctx)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(w.o.HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				hc := &live.Client{Base: inv.Endpoints.Live, Token: inv.Token, HTTP: w.o.HTTPClient, Deadline: time.Now().Add(10 * time.Second), Reserve: 10 * time.Second}
				hb, err := hc.Heartbeat(hbCtx)
				switch {
				case errors.Is(err, live.ErrSuperseded):
					superseded.Store(true)
					once.Do(func() { close(cancelled) })
				case err == nil && hb.Cancel != nil:
					once.Do(func() { close(cancelled) })
				case err != nil && hbCtx.Err() == nil:
					w.log.Warn("workflows: a heartbeat failed", "run", inv.Run.ID, "error", err)
				}
			}
		}
	}()
	// Heartbeats continue until complete returns, so a slow complete can't lose the invocation.
	defer func() {
		stopHB()
		<-hbDone
	}()
	cmds, err := run1(ctx, inv, w.byName, lc, execOptions{Deadline: deadline, MaxSteps: w.o.MaxStepsPerInvocation, Cancelled: cancelled, APIBase: w.o.APIBaseURL, Logf: logf(w.log)})
	switch {
	case superseded.Load() || errors.Is(err, live.ErrSuperseded):
		return
	case err != nil:
		w.log.Warn("workflows: the invocation didn't finish; the engine re-dispatches it", "run", inv.Run.ID, "error", err)
		return
	}
	res := protocol.InvocationResult{Commands: cmds, Deployment: w.o.Deployment, SDK: protocol.SDKInfo{Name: SDKName, Version: SDKVersion}}
	cc := &live.Client{Base: inv.Endpoints.Live, Token: inv.Token, HTTP: w.o.HTTPClient, Deadline: deadline.Add(30 * time.Second)}
	if err := cc.Complete(ctx, res); err != nil && !errors.Is(err, live.ErrSuperseded) {
		w.log.Warn("workflows: complete failed; the engine re-dispatches the invocation", "run", inv.Run.ID, "error", err)
	}
}
