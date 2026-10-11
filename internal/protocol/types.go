// Package protocol is the EZGH Workflows wire protocol (version 1.1): the messages EZGH and an
// app exchange, signatures, step IDs and tokens. It mirrors workflows-stack's internal/protocol
// and is checked against the same vectors (testdata/vectors). Customers may use it to verify
// signatures themselves.
package protocol

import (
	"encoding/json"
	"time"
)

// Version is the newest protocol version this SDK speaks.
const Version = "1.1"

// Versions are the protocol versions this SDK declares in sync replies and polls; the service
// speaks the newest it shares. Every SDK declares ["1.0", "1.1"].
var Versions = []string{"1.0", "1.1"}

// SDKName names this SDK in sync replies, invocation results and polls.
const SDKName = "github.com/ezgamehost/workflows-sdk-go"

// Journal entry kinds.
const (
	KindRun      = "run"
	KindSleep    = "sleep"
	KindValue    = "value"
	KindCallback = "callback"
	KindInvoke   = "invoke"
)

// Journal entry statuses.
const (
	StatusRunning   = "running"
	StatusRetrying  = "retrying"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
	StatusSleeping  = "sleeping"
	StatusWaiting   = "waiting"
)

// Invocation kinds.
const (
	InvocationOrchestrate = "orchestrate"
	InvocationStep        = "step"
	InvocationSync        = "sync"
)

// Command types.
const (
	CommandStepStarted      = "stepStarted"
	CommandStepCompleted    = "stepCompleted"
	CommandStepFailed       = "stepFailed"
	CommandValueRecorded    = "valueRecorded"
	CommandSleep            = "sleep"
	CommandRunCompleted     = "runCompleted"
	CommandRunFailed        = "runFailed"
	CommandWorkflowNotFound = "workflowNotFound"
	CommandCallbackCreated  = "callbackCreated"
	CommandWaitForCallback  = "waitForCallback"
	CommandInvoke           = "invoke"
	CommandPlanParallel     = "planParallel"
)

// RetryPolicy is a step's retry policy (see Backoff).
type RetryPolicy struct {
	MaxAttempts int     `json:"maxAttempts"`
	InitialMs   int64   `json:"initialMs"`
	MaxMs       int64   `json:"maxMs"`
	Multiplier  float64 `json:"multiplier"`
}

// DefaultRetry is the policy of a step that sets none.
var DefaultRetry = RetryPolicy{MaxAttempts: 5, InitialMs: 1000, MaxMs: 600000, Multiplier: 2}

// WireError is a workflow or step error on the wire.
type WireError struct {
	Code    string          `json:"code"`
	Name    string          `json:"name"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
	Stack   string          `json:"stack,omitempty"`
	Cause   *WireError      `json:"cause,omitempty"`
}

// JournalEntry is one recorded step; replay resolves entries in Seq order.
type JournalEntry struct {
	Seq       int64           `json:"seq"`
	StepID    string          `json:"stepId"`
	Name      string          `json:"name"`
	Kind      string          `json:"kind"`
	Status    string          `json:"status"`
	Attempt   int             `json:"attempt"`
	Output    json.RawMessage `json:"output,omitempty"`
	OutputRef string          `json:"outputRef,omitempty"`
	Error     *WireError      `json:"error,omitempty"`
	Token     string          `json:"token,omitempty"` // kind callback: the callback token (1.1)
}

// Journal is a run's recorded steps; Complete is false when the SDK must page the rest by seq.
type Journal struct {
	Complete bool           `json:"complete"`
	Entries  []JournalEntry `json:"entries"`
}

// ScheduleInfo is set on a run a schedule started: the schedule and the scheduled time (the
// cron instant, not the jittered fire time).
type ScheduleInfo struct {
	ID            string    `json:"id"`
	ScheduledTime time.Time `json:"scheduledTime"`
}

// RunInfo describes the run an invocation is for.
type RunInfo struct {
	ID        string          `json:"id"`
	RunKey    string          `json:"runKey,omitempty"`
	Workflow  string          `json:"workflow"`
	Input     json.RawMessage `json:"input,omitempty"`
	InputRef  string          `json:"inputRef,omitempty"`
	Attempt   int             `json:"attempt"`
	StartedAt time.Time       `json:"startedAt"`
	Schedule  *ScheduleInfo   `json:"schedule,omitempty"`
}

// Cancel is set on an invocation of a run being cancelled.
type Cancel struct {
	RequestedAt time.Time `json:"requestedAt"`
	GraceUntil  time.Time `json:"graceUntil"`
}

// Endpoints: Live is workflows-live; API is the public API base, on which callback URLs are
// built as API + "/public/v1/callbacks/" + token (1.1).
type Endpoints struct {
	Live string `json:"live"`
	API  string `json:"api,omitempty"`
}

// Invocation is what EZGH sends an app.
type Invocation struct {
	ProtocolVersion string    `json:"protocolVersion"`
	Kind            string    `json:"kind"`
	InvocationID    string    `json:"invocationId"`
	Token           string    `json:"token"`
	Endpoints       Endpoints `json:"endpoints"`
	Run             *RunInfo  `json:"run,omitempty"`
	Journal         *Journal  `json:"journal,omitempty"`
	TargetStepID    string    `json:"targetStepId,omitempty"`
	Deadline        time.Time `json:"deadline"`
	Cancel          *Cancel   `json:"cancel"`
}

// PlannedStep is one step of a planParallel command.
type PlannedStep struct {
	StepID    string       `json:"stepId"`
	Name      string       `json:"name"`
	Retry     *RetryPolicy `json:"retry,omitempty"`
	TimeoutMs int64        `json:"timeoutMs,omitempty"`
}

// Command is one thing an SDK reports: a tagged union on Type.
type Command struct {
	Type         string          `json:"type"`
	StepID       string          `json:"stepId,omitempty"`
	Name         string          `json:"name,omitempty"`
	Attempt      int             `json:"attempt,omitempty"`
	Retry        *RetryPolicy    `json:"retry,omitempty"`
	TimeoutMs    int64           `json:"timeoutMs,omitempty"`
	Output       json.RawMessage `json:"output,omitempty"`
	OutputRef    string          `json:"outputRef,omitempty"`
	Value        json.RawMessage `json:"value,omitempty"`
	Error        *WireError      `json:"error,omitempty"`
	Retryable    bool            `json:"retryable,omitempty"`
	RetryAfterMs int64           `json:"retryAfterMs,omitempty"`
	Until        *time.Time      `json:"until,omitempty"`
	CauseStepID  string          `json:"causeStepId,omitempty"`
	Workflow     string          `json:"workflow,omitempty"`
	Token        string          `json:"token,omitempty"`    // callbackCreated (the engine hashes it)
	Input        json.RawMessage `json:"input,omitempty"`    // invoke
	InputRef     string          `json:"inputRef,omitempty"` // invoke
	Steps        []PlannedStep   `json:"steps,omitempty"`    // planParallel
}

// Checkpoint is a batch of commands sent during an invocation; Seq counts from 1 per invocation.
type Checkpoint struct {
	InvocationID string    `json:"invocationId"`
	Seq          int       `json:"seq"`
	Commands     []Command `json:"commands"`
}

// SDKInfo names the SDK that answered.
type SDKInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// InvocationResult is a push invocation's response body, and a pull worker's complete body.
type InvocationResult struct {
	Commands   []Command `json:"commands"`
	Deployment string    `json:"deployment,omitempty"`
	SDK        SDKInfo   `json:"sdk"`
}

// WorkflowInfo is one workflow an app declares in its sync reply or poll.
type WorkflowInfo struct {
	Name      string       `json:"name"`
	Retry     *RetryPolicy `json:"retry,omitempty"`
	TimeoutMs int64        `json:"timeoutMs,omitempty"`
}

// SyncReply is an app's answer to a sync invocation.
type SyncReply struct {
	ProtocolVersions     []string       `json:"protocolVersions"`
	SDK                  SDKInfo        `json:"sdk"`
	MaxInvocationSeconds int            `json:"maxInvocationSeconds"`
	Workflows            []WorkflowInfo `json:"workflows"`
	Deployment           string         `json:"deployment,omitempty"`
}

// PollRequest is a pull worker's poll (POST …/workers/poll on the live host).
type PollRequest struct {
	App              string         `json:"app"`
	Queue            string         `json:"queue"`
	MaxTasks         int            `json:"maxTasks"`
	WorkerID         string         `json:"workerId"`
	ProtocolVersions []string       `json:"protocolVersions"`
	SDK              SDKInfo        `json:"sdk"`
	Workflows        []WorkflowInfo `json:"workflows,omitempty"`
}

// PollResponse carries started invocations (none when the poll timed out).
type PollResponse struct {
	Invocations []Invocation `json:"invocations"`
}

// HeartbeatResponse answers a pull worker's heartbeat: non-nil Cancel while the run is
// cancelling, so a long step can stop early.
type HeartbeatResponse struct {
	Cancel *Cancel `json:"cancel"`
}
