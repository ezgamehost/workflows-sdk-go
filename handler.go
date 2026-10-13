package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/jsonx"
	"github.com/ezgamehost/workflows-sdk-go/internal/live"
	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// maxBody bounds an invocation's body; maxResultBytes bounds the result dispatch reads back.
const (
	maxBody        = 8 << 20
	maxResultBytes = 16 << 20
)

// HandlerOptions configures the push handler.
type HandlerOptions struct {
	Workflows []Workflow
	// SigningSecrets are the app's active signing secrets (whsec_…); pass two during a rotation.
	// Empty entries (an unset environment variable) are skipped; with none left, requests answer
	// 500.
	SigningSecrets []string
	// MaxInvocationSeconds is how long your platform lets a request run: 1–900, default 300. EZGH
	// sets each invocation's deadline inside it.
	MaxInvocationSeconds int
	// Deployment is recorded with each invocation; detected on Vercel, Lambda, Cloud Run, Fly.io
	// and Render.
	Deployment string
	// HTTPClient calls workflows-live (default: a client without a timeout; each call is bounded
	// by the invocation's deadline).
	HTTPClient *http.Client
	// MaxStepsPerInvocation is how many steps run inline before the invocation ends and EZGH
	// invokes the workflow again (default 50).
	MaxStepsPerInvocation int
	// Logger receives the SDK's own errors (default slog.Default()).
	Logger *slog.Logger
}

type handler struct {
	byName     map[string]*definition
	order      []*definition
	keys       [][]byte
	maxSeconds int
	deployment string
	httpc      *http.Client
	maxSteps   int
	log        *slog.Logger
	now        func() time.Time
}

func definitions(ws []Workflow, who string) (map[string]*definition, []*definition, error) {
	byName := map[string]*definition{}
	var order []*definition
	for _, w := range ws {
		if w == nil {
			return nil, nil, errors.New(who + ": a workflow is nil")
		}
		d := w.definition()
		if byName[d.name] != nil {
			return nil, nil, fmt.Errorf("%s: two workflows are named %q", who, d.name)
		}
		byName[d.name] = d
		order = append(order, d)
	}
	if len(order) == 0 {
		return nil, nil, errors.New(who + " needs at least one workflow")
	}
	return byName, order, nil
}

// Handler returns the push handler: an http.Handler that EZGH invokes with signed POSTs. Mount
// it at the app's endpoint. Answers: 200 with a sync reply or an invocation result, 401 for a
// bad signature, 400 for a bad invocation, 409 when the engine superseded the invocation, 503
// when workflows-live didn't take a checkpoint, 500 for an SDK error, 405 for anything but POST
// and 413 over 8 MiB.
func Handler(o HandlerOptions) (http.Handler, error) {
	byName, order, err := definitions(o.Workflows, "workflows.Handler")
	if err != nil {
		return nil, err
	}
	h := &handler{byName: byName, order: order, maxSeconds: o.MaxInvocationSeconds, deployment: o.Deployment,
		httpc: o.HTTPClient, maxSteps: o.MaxStepsPerInvocation, log: o.Logger, now: time.Now}
	if len(o.SigningSecrets) == 0 {
		return nil, errors.New("workflows.Handler needs at least one signing secret")
	}
	for _, s := range o.SigningSecrets {
		if strings.TrimSpace(s) == "" {
			continue
		}
		k, err := protocol.DecodeSecret(s)
		if err != nil {
			return nil, err
		}
		h.keys = append(h.keys, k)
	}
	if h.maxSeconds == 0 {
		h.maxSeconds = 300
	}
	if h.maxSeconds < 1 || h.maxSeconds > 900 {
		return nil, errors.New("workflows.Handler: MaxInvocationSeconds is 1 to 900")
	}
	for _, d := range h.order {
		if d.timeout > time.Duration(h.maxSeconds)*time.Second {
			return nil, fmt.Errorf("workflows.Handler: %q's step timeout (%v) is longer than MaxInvocationSeconds (%d s), so its steps couldn't finish; use a pull worker for longer steps", d.name, d.timeout, h.maxSeconds)
		}
	}
	if h.maxSteps < 0 {
		return nil, errors.New("workflows.Handler: MaxStepsPerInvocation is at least 1")
	}
	if h.deployment == "" {
		h.deployment = detectDeployment()
	}
	h.deployment = protocol.CleanDeployment(h.deployment)
	if h.httpc == nil {
		h.httpc = &http.Client{} // each call is bounded by the invocation's deadline
	}
	h.httpc = noRedirects(h.httpc)
	if h.maxSteps == 0 {
		h.maxSteps = 50
	}
	if h.log == nil {
		h.log = slog.Default()
	}
	return h, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := jsonx.Marshal(v)
	if err != nil {
		status, b = http.StatusInternalServerError, []byte(`{"code":"sdk_error"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func code(c string) map[string]string { return map[string]string{"code": c} }

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	received := h.now()
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, code("method_not_allowed"))
		return
	}
	if len(h.keys) == 0 {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"code": "sdk_error", "message": "no signing secret is configured"})
		return
	}
	// The header and its timestamp first: a stale or malformed signature costs no body read.
	sig, err := protocol.ParseSignature(r.Header.Get(protocol.SignatureHeader), received)
	if err != nil {
		c := "invalid_signature"
		var se *protocol.SignatureError
		if errors.As(err, &se) && se.Reason == "expired" {
			c = "expired_signature"
		}
		writeJSON(w, http.StatusUnauthorized, code(c))
		return
	}
	if r.ContentLength > maxBody {
		writeJSON(w, http.StatusRequestEntityTooLarge, code("payload_too_large"))
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeJSON(w, http.StatusRequestEntityTooLarge, code("payload_too_large"))
			return
		}
		writeJSON(w, http.StatusBadRequest, code("invalid_invocation"))
		return
	}
	if protocol.VerifyParsed(sig, h.keys, body) != nil {
		writeJSON(w, http.StatusUnauthorized, code("invalid_signature"))
		return
	}
	inv, err := parseInvocation(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_invocation", "message": err.Error()})
		return
	}
	// A push request carries the traceparent as a header (only while the setting is on).
	inv.Traceparent = r.Header.Get("traceparent")
	sdk := protocol.SDKInfo{Name: SDKName, Version: SDKVersion}
	if inv.Kind == protocol.InvocationSync {
		reply := protocol.SyncReply{ProtocolVersions: protocol.Versions, SDK: sdk, MaxInvocationSeconds: h.maxSeconds, Deployment: h.deployment}
		for _, d := range h.order {
			reply.Workflows = append(reply.Workflows, d.info())
		}
		writeJSON(w, http.StatusOK, reply)
		return
	}
	// The deadline on this machine's clock: the engine's budget (deadline − t) from receipt, and
	// never later than the deadline as this clock reads it.
	deadline := received.Add(inv.Deadline.Sub(time.Unix(sig.T, 0)))
	if inv.Deadline.Before(deadline) {
		deadline = inv.Deadline
	}
	total := max(0, deadline.Sub(received))
	lc := &live.Client{Base: inv.Endpoints.Live, Token: inv.Token, HTTP: h.httpc, Deadline: deadline, Reserve: max(5*time.Second, total/10), Now: h.now}
	cmds, err := run1(r.Context(), inv, h.byName, lc, execOptions{Deadline: deadline, MaxSteps: h.maxSteps, Now: h.now, Logf: logf(h.log), MaxResultBytes: maxResultBytes})
	var cf *live.CheckpointFailed
	switch {
	case errors.Is(err, live.ErrSuperseded):
		writeJSON(w, http.StatusConflict, code("invocation_superseded"))
	case errors.As(err, &cf):
		// The code goes top level and in the kit's error shape, which dispatch reads:
		// payload_unavailable costs the run no step attempt.
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": cf.Code, "error": map[string]string{"code": cf.Code, "message": cf.Message}})
	case err != nil:
		h.log.Error("workflows: the invocation failed in the SDK", "run", inv.Run.ID, "error", err)
		writeJSON(w, http.StatusInternalServerError, code("sdk_error"))
	default:
		res := protocol.InvocationResult{Commands: cmds, Deployment: h.deployment, SDK: sdk}
		if err := res.Validate(); err != nil {
			h.log.Error("workflows: the SDK built a result the engine would refuse", "run", inv.Run.ID, "error", err)
			writeJSON(w, http.StatusInternalServerError, code("sdk_error"))
			return
		}
		writeJSON(w, http.StatusOK, res)
	}
}

func logf(l *slog.Logger) func(string, ...any) {
	return func(format string, args ...any) { l.Warn(fmt.Sprintf(format, args...)) }
}

// run1 executes one orchestrate or step invocation: workflowNotFound for a workflow this app
// doesn't have.
func run1(ctx context.Context, inv *protocol.Invocation, byName map[string]*definition, io live.IO, o execOptions) ([]protocol.Command, error) {
	d := byName[inv.Run.Workflow]
	if d == nil {
		return []protocol.Command{{Type: protocol.CommandWorkflowNotFound, Workflow: inv.Run.Workflow}}, nil
	}
	cmds, err := execute(ctx, inv, d, io, o)
	if cmds == nil {
		cmds = []protocol.Command{}
	}
	return cmds, err
}

// parseInvocation checks what the runtime relies on and ignores unknown fields (minor protocol
// versions add fields).
func parseInvocation(b []byte) (*protocol.Invocation, error) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(b, &probe); err != nil || probe == nil {
		return nil, errors.New("the body isn't an invocation")
	}
	var inv protocol.Invocation
	if err := json.Unmarshal(b, &inv); err != nil {
		return nil, fmt.Errorf("the body isn't an invocation: %v", err)
	}
	if major, _, _ := strings.Cut(inv.ProtocolVersion, "."); major != "1" {
		return nil, fmt.Errorf("unsupported protocol version %q", inv.ProtocolVersion)
	}
	switch inv.Kind {
	case protocol.InvocationOrchestrate, protocol.InvocationStep, protocol.InvocationSync:
	default:
		return nil, fmt.Errorf("unknown invocation kind %q", inv.Kind)
	}
	if inv.InvocationID == "" || inv.Deadline.IsZero() {
		return nil, errors.New("missing invocationId or deadline")
	}
	if inv.Kind != protocol.InvocationSync {
		if inv.Run == nil || inv.Run.ID == "" || inv.Run.Workflow == "" {
			return nil, errors.New("missing run")
		}
		if inv.Journal == nil {
			return nil, errors.New("missing journal")
		}
		for _, e := range inv.Journal.Entries {
			if e.StepID == "" || e.Status == "" {
				return nil, errors.New("a journal entry needs seq, stepId and status")
			}
			if e.Kind == protocol.KindMessage && (e.QueueID == "" || e.Key == "") {
				return nil, errors.New("a message entry needs queueId and key")
			}
		}
		// createdSeq, when present, is a seq: at least 1 (0 would decode as absent).
		var probe struct {
			Journal struct {
				Entries []struct {
					CreatedSeq *int64 `json:"createdSeq"`
				} `json:"entries"`
			} `json:"journal"`
		}
		if json.Unmarshal(b, &probe) == nil {
			for _, e := range probe.Journal.Entries {
				if e.CreatedSeq != nil && *e.CreatedSeq < 1 {
					return nil, errors.New("a journal entry's createdSeq is at least 1")
				}
			}
		}
		if inv.Token == "" || inv.Endpoints.Live == "" {
			return nil, errors.New("missing token or endpoints.live")
		}
		if inv.Kind == protocol.InvocationStep && inv.TargetStepID == "" {
			return nil, errors.New("a step invocation needs targetStepId")
		}
	}
	return &inv, nil
}
