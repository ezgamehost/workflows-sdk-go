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
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/jsonx"
	"github.com/ezgamehost/workflows-sdk-go/protocol"
)

// DefaultBaseURL is the Workflows API.
const DefaultBaseURL = "https://workflows-api.ezghcloud.com"

// ClientOptions configures a Client for one project.
type ClientOptions struct {
	// APIKey is an EZGH API key (or another bearer credential) allowed the workflows actions the
	// client uses: run methods take an API key or a workflows:runs token; app and schedule methods
	// need workflows.apps.* or workflows.schedules.*.
	APIKey         string
	OrganizationID string
	ProjectID      string
	BaseURL        string       // default DefaultBaseURL
	HTTPClient     *http.Client // default: a client with a 70 s timeout
	// Retries is how many times a 429, 503 or network error is retried (default 3; negative: none),
	// after Retry-After, at most MaxRetryDelay (default 10 s). Writes are resent with the same
	// Idempotency-Key.
	Retries       int
	MaxRetryDelay time.Duration
}

// Client calls the Workflows API for one project: apps, runs, callbacks and schedules. Its API
// key never appears when the client is printed, logged or marshalled.
type Client struct {
	key      secret
	base     string
	httpc    *http.Client
	retries  int
	maxDelay time.Duration
}

func (c Client) String() string               { return "workflows.Client{" + c.base + "}" }
func (c Client) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, c.String()) }
func (c Client) GoString() string             { return c.String() }
func (c Client) LogValue() slog.Value         { return slog.StringValue(c.String()) }
func (c Client) MarshalJSON() ([]byte, error) { return json.Marshal(map[string]string{"base": c.base}) }

// NewClient checks the options and returns a client.
func NewClient(o ClientOptions) (*Client, error) {
	if o.APIKey == "" || o.OrganizationID == "" || o.ProjectID == "" {
		return nil, errors.New("workflows.NewClient needs APIKey, OrganizationID and ProjectID")
	}
	base := strings.TrimRight(o.BaseURL, "/")
	if base == "" {
		base = DefaultBaseURL
	}
	if _, err := url.Parse(base); err != nil {
		return nil, fmt.Errorf("workflows.NewClient: BaseURL: %w", err)
	}
	c := &Client{key: secret(o.APIKey), base: base + "/v1/organizations/" + url.PathEscape(o.OrganizationID) + "/projects/" + url.PathEscape(o.ProjectID),
		httpc: o.HTTPClient, retries: 3, maxDelay: 10 * time.Second}
	if o.Retries > 0 {
		c.retries = o.Retries
	} else if o.Retries < 0 {
		c.retries = 0
	}
	if o.MaxRetryDelay > 0 {
		c.maxDelay = o.MaxRetryDelay
	}
	if c.httpc == nil {
		c.httpc = &http.Client{Timeout: 70 * time.Second}
	}
	return c, nil
}

// APIError is an error answer from the Workflows API: the HTTP status and the platform's error
// code. Status 0 with Code "outcome_unknown": the request may or may not have taken effect (a
// network error on CreateApp or RotateSecret, which aren't resent).
type APIError struct {
	Status     int
	Code       string
	Message    string
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	return fmt.Sprintf("workflows API %d %s: %s", e.Status, e.Code, e.Message)
}

func newKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// apiError reads the kit's {"error":{"code","message"}} (or a top-level code).
func apiError(res *http.Response, data []byte) *APIError {
	var eb struct {
		Code  string `json:"code"`
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &eb)
	ae := &APIError{Status: res.StatusCode, Code: eb.Error.Code, Message: eb.Error.Message}
	if ae.Code == "" {
		ae.Code = eb.Code
	}
	if ae.Code == "" {
		ae.Code = "unknown"
	}
	if ae.Message == "" {
		ae.Message = http.StatusText(res.StatusCode)
	}
	if s, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && s > 0 {
		ae.RetryAfter = time.Duration(s) * time.Second
	}
	return ae
}

type request struct {
	method, path string
	body         any
	key          string // Idempotency-Key for writes; default random
	noResend     bool   // a network error isn't resent: outcome_unknown
	header       http.Header
	out          any // nil, or a pointer to decode JSON into
}

// do sends one request, retrying 429, 503 and network errors with the same Idempotency-Key.
func (c *Client) do(ctx context.Context, r request) error {
	var payload []byte
	if r.body != nil {
		var err error
		if payload, err = jsonx.Marshal(r.body); err != nil {
			return err
		}
	}
	if r.method != http.MethodGet && r.key == "" {
		r.key = newKey()
	}
	for attempt := 0; ; attempt++ {
		var body io.Reader
		if payload != nil {
			body = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, r.method, c.base+r.path, body)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.key.reveal())
		req.Header.Set("Accept", "application/json")
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if r.method != http.MethodGet {
			req.Header.Set("Idempotency-Key", r.key)
		}
		for k, vs := range r.header {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		res, err := c.httpc.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if r.noResend {
				return &APIError{Status: 0, Code: "outcome_unknown", Message: "the request failed in transit (" + err.Error() + "), so it may have taken effect: a resend would return no secret. Check with GetApp, and rotate the app's secret if you didn't receive it"}
			}
			if attempt >= c.retries {
				return err
			}
			sleepCtx(ctx, min(c.maxDelay, 500*time.Millisecond<<min(attempt, 10)))
			continue
		}
		data, rerr := io.ReadAll(io.LimitReader(res.Body, 64<<20))
		res.Body.Close()
		if res.StatusCode/100 == 2 {
			if rerr != nil {
				return rerr
			}
			if r.out == nil || len(data) == 0 {
				return nil
			}
			if raw, ok := r.out.(*json.RawMessage); ok {
				*raw = append((*raw)[:0], data...)
				return nil
			}
			return json.Unmarshal(data, r.out)
		}
		ae := apiError(res, data)
		if (res.StatusCode != http.StatusTooManyRequests && res.StatusCode != http.StatusServiceUnavailable) || attempt >= c.retries {
			return ae
		}
		wait := ae.RetryAfter
		if wait == 0 {
			wait = time.Second
		}
		sleepCtx(ctx, min(c.maxDelay, wait))
		if ctx.Err() != nil {
			return ae
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

var pe = url.PathEscape

// App kinds, sync statuses and run statuses. They're open sets: a later API may add values.
const (
	AppKindPush = "push"
	AppKindPull = "pull"

	SyncStatusUnsynced = "unsynced"
	SyncStatusSynced   = "synced"
	SyncStatusFailed   = "failed"

	RunStatusRunning    = "running"
	RunStatusCancelling = "cancelling"
	RunStatusCompleted  = "completed"
	RunStatusFailed     = "failed"
	RunStatusCancelled  = "cancelled"
	RunStatusTimedOut   = "timedOut"
)

// App is a Workflows app.
type App struct {
	ID                   string                  `json:"id"`
	Slug                 string                  `json:"slug"`
	ResourceName         string                  `json:"resourceName"`
	OrganizationID       string                  `json:"organizationId"`
	ProjectID            string                  `json:"projectId"`
	Name                 string                  `json:"name"`
	Kind                 string                  `json:"kind"`
	Endpoint             *string                 `json:"endpoint"`
	SyncStatus           string                  `json:"syncStatus"`
	Workflows            []protocol.WorkflowInfo `json:"workflows"`
	MaxInvocationSeconds *int                    `json:"maxInvocationSeconds"`
	ProtocolVersion      *string                 `json:"protocolVersion"`
	SDK                  *protocol.SDKInfo       `json:"sdk"`
	Deployment           *string                 `json:"deployment"`
	Health               string                  `json:"health"`
	LastSyncAt           *string                 `json:"lastSyncAt"`
	CreatedAt            string                  `json:"createdAt"`
	UpdatedAt            string                  `json:"updatedAt"`
	Queue                *string                 `json:"queue,omitempty"`      // pull apps
	Partitions           *int                    `json:"partitions,omitempty"` // pull apps
}

// AppWithSecret is a created app, or one whose secret was rotated. The signing secret is
// returned once: an Idempotency-Key replay answers nil.
type AppWithSecret struct {
	App           App     `json:"app"`
	SigningSecret *string `json:"signingSecret"`
}

// CreateAppOptions: Kind is "push" (default; Endpoint optional until sync) or "pull" (Queue,
// default "default", and Partitions, 1–32, default 4).
type CreateAppOptions struct {
	Kind           string
	Endpoint       string
	Queue          string
	Partitions     int
	IdempotencyKey string
}

// CreateApp creates an app and returns it with its signing secret, once. A network error isn't
// resent (a replay would answer no secret): it returns an *APIError with Code "outcome_unknown".
func (c *Client) CreateApp(ctx context.Context, name string, o CreateAppOptions) (*AppWithSecret, error) {
	kind := o.Kind
	if kind == "" {
		kind = AppKindPush
	}
	body := map[string]any{"name": name, "kind": kind}
	if o.Endpoint != "" {
		body["endpoint"] = o.Endpoint
	}
	if o.Queue != "" {
		body["queue"] = o.Queue
	}
	if o.Partitions != 0 {
		body["partitions"] = o.Partitions
	}
	var out AppWithSecret
	return &out, c.do(ctx, request{method: http.MethodPost, path: "/apps", body: body, key: o.IdempotencyKey, noResend: true, out: &out})
}

// GetApp reads an app.
func (c *Client) GetApp(ctx context.Context, appID string) (*App, error) {
	var out App
	return &out, c.do(ctx, request{method: http.MethodGet, path: "/apps/" + pe(appID), out: &out})
}

// SyncOptions: an endpoint and a deployment to record, both optional.
type SyncOptions struct {
	Endpoint   string `json:"endpoint,omitempty"`
	Deployment string `json:"deployment,omitempty"`
}

// Sync asks EZGH to call the app's endpoint and record its workflows; run it after each deploy.
// A failed sync is a 409 sync_failed.
func (c *Client) Sync(ctx context.Context, appID string, o SyncOptions) (*App, error) {
	var out App
	return &out, c.do(ctx, request{method: http.MethodPost, path: "/apps/" + pe(appID) + "/sync", body: o, out: &out})
}

// RotateSecret returns a new signing secret; the old ones stay valid for 24 hours. A network
// error isn't resent, as for CreateApp.
func (c *Client) RotateSecret(ctx context.Context, appID string) (*AppWithSecret, error) {
	var out AppWithSecret
	return &out, c.do(ctx, request{method: http.MethodPost, path: "/apps/" + pe(appID) + "/rotate-secret", body: map[string]any{}, noResend: true, out: &out})
}

// RunStep is one step of a run.
type RunStep struct {
	Seq       int64               `json:"seq"`
	StepID    string              `json:"stepId"`
	Name      string              `json:"name"`
	Kind      string              `json:"kind"`
	Status    string              `json:"status"`
	Attempt   int                 `json:"attempt"`
	HasOutput bool                `json:"hasOutput"`
	Error     *protocol.WireError `json:"error"`
}

// WorkflowRun is a run of a workflow, as the API answers it. (Run is the step function.)
type WorkflowRun struct {
	ID          string              `json:"id"`
	RunKey      *string             `json:"runKey"`
	AppID       string              `json:"appId"`
	Workflow    string              `json:"workflow"`
	Status      string              `json:"status"`
	Attempt     int                 `json:"attempt"`
	Error       *protocol.WireError `json:"error"`
	CauseStepID *string             `json:"causeStepId"`
	Steps       []RunStep           `json:"steps"`
	CreatedAt   string              `json:"createdAt"`
	UpdatedAt   string              `json:"updatedAt"`
	EndedAt     *string             `json:"endedAt"`
}

// Terminal reports whether the run has ended.
func (r *WorkflowRun) Terminal() bool {
	switch r.Status {
	case RunStatusCompleted, RunStatusFailed, RunStatusCancelled, RunStatusTimedOut:
		return true
	}
	return false
}

// StartOptions: RunKey makes the start idempotent (the same key, workflow and input return the
// same run; different input is a 409 run_key_conflict).
type StartOptions struct {
	RunKey         string
	IdempotencyKey string
}

// Start starts a run of workflow with input (JSON; nil for none).
func (c *Client) Start(ctx context.Context, appID, workflow string, input any, o StartOptions) (*WorkflowRun, error) {
	body := map[string]any{"appId": appID, "workflow": workflow}
	if input != nil {
		body["input"] = input
	}
	if o.RunKey != "" {
		body["runKey"] = o.RunKey
	}
	var out WorkflowRun
	return &out, c.do(ctx, request{method: http.MethodPost, path: "/runs", body: body, key: o.IdempotencyKey, out: &out})
}

// Get reads a run and its steps.
func (c *Client) Get(ctx context.Context, runID string) (*WorkflowRun, error) {
	var out WorkflowRun
	return &out, c.do(ctx, request{method: http.MethodGet, path: "/runs/" + pe(runID), out: &out})
}

// CancelOptions: the run's code gets GracePeriodSeconds (0–3600, default 300) to clean up.
type CancelOptions struct {
	GracePeriodSeconds *int
}

// Cancel cancels a run.
func (c *Client) Cancel(ctx context.Context, runID string, o CancelOptions) (*WorkflowRun, error) {
	body := map[string]any{}
	if o.GracePeriodSeconds != nil {
		body["gracePeriodSeconds"] = *o.GracePeriodSeconds
	}
	var out WorkflowRun
	return &out, c.do(ctx, request{method: http.MethodPost, path: "/runs/" + pe(runID) + "/cancel", body: body, out: &out})
}

// Retry retries a failed run from its failed step; completed steps aren't run again.
func (c *Client) Retry(ctx context.Context, runID string) (*WorkflowRun, error) {
	var out WorkflowRun
	return &out, c.do(ctx, request{method: http.MethodPost, path: "/runs/" + pe(runID) + "/retry", body: map[string]any{}, out: &out})
}

// Output decodes the run's output into into (409 run_not_completed until it completes).
func (c *Client) Output(ctx context.Context, runID string, into any) error {
	return c.do(ctx, request{method: http.MethodGet, path: "/runs/" + pe(runID) + "/payloads/output", out: into})
}

// Input decodes the run's input into into.
func (c *Client) Input(ctx context.Context, runID string, into any) error {
	return c.do(ctx, request{method: http.MethodGet, path: "/runs/" + pe(runID) + "/payloads/input", out: into})
}

// StepOutput decodes a step's output into into.
func (c *Client) StepOutput(ctx context.Context, runID, stepID string, into any) error {
	return c.do(ctx, request{method: http.MethodGet, path: "/runs/" + pe(runID) + "/steps/" + pe(stepID) + "/output", out: into})
}

// RunEvent is one event of a run's history.
type RunEvent struct {
	ID           string          `json:"id"`
	At           string          `json:"at"`
	Type         string          `json:"type"`
	StepID       *string         `json:"stepId"`
	InvocationID *string         `json:"invocationId"`
	Deployment   *string         `json:"deployment"`
	Detail       json.RawMessage `json:"detail"`
}

// EventsPage is a page of a run's events.
type EventsPage struct {
	Events     []RunEvent `json:"events"`
	NextCursor *string    `json:"nextCursor"`
}

// EventsOptions pages events.
type EventsOptions struct {
	Cursor string
	Limit  int
}

func query(cursor string, limit int) string {
	q := url.Values{}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if len(q) == 0 {
		return ""
	}
	return "?" + q.Encode()
}

// Events reads a page of the run's events.
func (c *Client) Events(ctx context.Context, runID string, o EventsOptions) (*EventsPage, error) {
	var out EventsPage
	return &out, c.do(ctx, request{method: http.MethodGet, path: "/runs/" + pe(runID) + "/events" + query(o.Cursor, o.Limit), out: &out})
}

// CompleteCallback completes a run's callback step with value (JSON, at most 1 MiB), as the
// caller (workflows.callbacks.complete). The first completion wins: a repeat with the same value
// succeeds, a different one is a 409 callback_completed, and after the timeout it's a 410
// callback_expired.
func (c *Client) CompleteCallback(ctx context.Context, runID, stepID string, value any) error {
	return c.do(ctx, request{method: http.MethodPost, path: "/runs/" + pe(runID) + "/callbacks/" + pe(stepID), body: jsonValue(value)})
}

// jsonValue makes a nil value JSON null (do sends no body for nil).
func jsonValue(v any) any {
	if v == nil {
		return json.RawMessage("null")
	}
	return v
}

// CompleteCallbackURL completes a callback by its URL (Callback.URL): the token in the URL is
// the only credential, so no API key is sent. Answers as CompleteCallback's: 404 for an unknown
// token, 409 callback_completed, 410 callback_expired or for an ended run.
func CompleteCallbackURL(ctx context.Context, httpc *http.Client, callbackURL string, value any) error {
	if httpc == nil {
		httpc = &http.Client{Timeout: 70 * time.Second}
	}
	body, err := jsonx.Marshal(jsonValue(value))
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callbackURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := httpc.Do(req)
	if err != nil {
		return err
	}
	data, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	res.Body.Close()
	if res.StatusCode/100 == 2 {
		return nil
	}
	return apiError(res, data)
}

// Schedule overlap policies.
const (
	OverlapSkip      = "skip"
	OverlapAllow     = "allow"
	OverlapBufferOne = "bufferOne"
)

// Schedule is a cron schedule that starts runs of a workflow.
type Schedule struct {
	ID                   string  `json:"id"`
	ResourceName         string  `json:"resourceName"`
	AppID                string  `json:"appId"`
	Workflow             string  `json:"workflow"`
	Name                 string  `json:"name"`
	Cron                 string  `json:"cron"`
	TimeZone             string  `json:"timeZone"`
	Overlap              string  `json:"overlap"`
	JitterSeconds        int     `json:"jitterSeconds"`
	CatchupWindowSeconds int     `json:"catchupWindowSeconds"`
	Paused               bool    `json:"paused"`
	Revision             int64   `json:"revision"`
	NextFireAt           *string `json:"nextFireAt"`
	NextScheduledTime    *string `json:"nextScheduledTime"`
	LastScheduledTime    *string `json:"lastScheduledTime"`
	ActiveRunID          *string `json:"activeRunId"`
	Missed               int64   `json:"missed"`
	Skipped              int64   `json:"skipped"`
	HasInput             bool    `json:"hasInput"`
	CreatedAt            string  `json:"createdAt"`
	UpdatedAt            string  `json:"updatedAt"`
}

// ScheduleSpec creates a schedule. Cron is 5 fields or @hourly, @daily, @weekly, @monthly or
// @yearly, at most once a minute, in TimeZone (an IANA zone, default UTC). Overlap is skip
// (default), allow or bufferOne; JitterSeconds 0–3,600; CatchupWindowSeconds 0–86,400 (default
// 3,600). Input is JSON (at most 1 MiB), nil for none.
type ScheduleSpec struct {
	AppID                string `json:"appId"`
	Workflow             string `json:"workflow"`
	Name                 string `json:"name"`
	Cron                 string `json:"cron"`
	TimeZone             string `json:"timeZone,omitempty"`
	Input                any    `json:"input,omitempty"`
	Overlap              string `json:"overlap,omitempty"`
	JitterSeconds        *int   `json:"jitterSeconds,omitempty"`
	CatchupWindowSeconds *int   `json:"catchupWindowSeconds,omitempty"`
	Paused               bool   `json:"paused,omitempty"`
	// IdempotencyKey makes the create idempotent: a retry with the same key reaches the same
	// schedule.
	IdempotencyKey string `json:"-"`
}

// ScheduleUpdate changes a schedule; nil fields are unchanged.
type ScheduleUpdate struct {
	Workflow             *string `json:"workflow,omitempty"`
	Name                 *string `json:"name,omitempty"`
	Cron                 *string `json:"cron,omitempty"`
	TimeZone             *string `json:"timeZone,omitempty"`
	Input                any     `json:"input,omitempty"`
	Overlap              *string `json:"overlap,omitempty"`
	JitterSeconds        *int    `json:"jitterSeconds,omitempty"`
	CatchupWindowSeconds *int    `json:"catchupWindowSeconds,omitempty"`
}

// ScheduleSummary is a schedule in a list.
type ScheduleSummary struct {
	ID           string `json:"id"`
	ResourceName string `json:"resourceName"`
	AppID        string `json:"appId"`
	Workflow     string `json:"workflow"`
	Name         string `json:"name"`
	Cron         string `json:"cron"`
	TimeZone     string `json:"timeZone"`
	Paused       bool   `json:"paused"`
	Revision     int64  `json:"revision"`
	UpdatedAt    string `json:"updatedAt"`
}

// SchedulesPage is a page of the project's schedules, newest first.
type SchedulesPage struct {
	Schedules  []ScheduleSummary `json:"schedules"`
	NextCursor *string           `json:"nextCursor"`
}

// ListOptions pages a list.
type ListOptions struct {
	Cursor string
	Limit  int
}

// CreateSchedule creates a schedule (workflows.schedules.create, and workflows.runs.start on its
// workflow).
func (c *Client) CreateSchedule(ctx context.Context, s ScheduleSpec) (*Schedule, error) {
	var out Schedule
	return &out, c.do(ctx, request{method: http.MethodPost, path: "/schedules", body: s, key: s.IdempotencyKey, out: &out})
}

// GetSchedule reads a schedule.
func (c *Client) GetSchedule(ctx context.Context, id string) (*Schedule, error) {
	var out Schedule
	return &out, c.do(ctx, request{method: http.MethodGet, path: "/schedules/" + pe(id), out: &out})
}

// ListSchedules reads a page of the project's schedules.
func (c *Client) ListSchedules(ctx context.Context, o ListOptions) (*SchedulesPage, error) {
	var out SchedulesPage
	return &out, c.do(ctx, request{method: http.MethodGet, path: "/schedules" + query(o.Cursor, o.Limit), out: &out})
}

// UpdateSchedule changes a schedule. With revision > 0 it applies only to that revision
// (If-Match): another change in between is a 412 precondition_failed.
func (c *Client) UpdateSchedule(ctx context.Context, id string, revision int64, s ScheduleUpdate) (*Schedule, error) {
	h := http.Header{}
	if revision > 0 {
		h.Set("If-Match", `"`+strconv.FormatInt(revision, 10)+`"`)
	}
	var out Schedule
	return &out, c.do(ctx, request{method: http.MethodPatch, path: "/schedules/" + pe(id), body: s, header: h, out: &out})
}

// PauseSchedule stops a schedule's firings until it's resumed.
func (c *Client) PauseSchedule(ctx context.Context, id string) (*Schedule, error) {
	var out Schedule
	return &out, c.do(ctx, request{method: http.MethodPost, path: "/schedules/" + pe(id) + "/pause", body: map[string]any{}, out: &out})
}

// ResumeSchedule resumes a paused schedule.
func (c *Client) ResumeSchedule(ctx context.Context, id string) (*Schedule, error) {
	var out Schedule
	return &out, c.do(ctx, request{method: http.MethodPost, path: "/schedules/" + pe(id) + "/resume", body: map[string]any{}, out: &out})
}

// TriggerSchedule starts a run of the schedule now. The idempotency key names the run: the same
// key returns the same run (empty: a new random key, resent unchanged on retries).
func (c *Client) TriggerSchedule(ctx context.Context, id string, idempotencyKey string) (*WorkflowRun, error) {
	var out WorkflowRun
	return &out, c.do(ctx, request{method: http.MethodPost, path: "/schedules/" + pe(id) + "/trigger", body: map[string]any{}, key: idempotencyKey, out: &out})
}

// DeleteSchedule deletes a schedule; its runs continue.
func (c *Client) DeleteSchedule(ctx context.Context, id string) error {
	return c.do(ctx, request{method: http.MethodDelete, path: "/schedules/" + pe(id)})
}
