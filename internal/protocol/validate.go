package protocol

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalid wraps every validation failure. These checks are workflows-stack's
// internal/protocol/validate.go, byte for byte: the engine refuses what fails them, so the SDK
// checks every checkpoint, result and poll before sending it.
var ErrInvalid = errors.New("protocol: invalid")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func hasNUL(s string) bool { return strings.IndexByte(s, 0) >= 0 }

// Validate checks the error and its causes: code user or workflows.*, a name and a message of at
// most 8 KiB each, a stack of at most 8 KiB, data of at most 64 KiB, at most 8 levels deep.
func (e *WireError) Validate() error {
	for depth, cur := 0, e; cur != nil; depth, cur = depth+1, cur.Cause {
		if depth >= MaxErrorDepth {
			return invalid("error causes nest deeper than %d", MaxErrorDepth)
		}
		if !errorCodeRe.MatchString(cur.Code) {
			return invalid("error code %q isn't user or workflows.*", cur.Code)
		}
		if len(cur.Stack) > MaxStackBytes {
			return invalid("error stack longer than %d bytes", MaxStackBytes)
		}
		if len(cur.Name) > MaxErrorTextBytes || len(cur.Message) > MaxErrorTextBytes {
			return invalid("error name or message longer than %d bytes", MaxErrorTextBytes)
		}
		if len(cur.Data) > MaxErrorDataBytes {
			return invalid("error data longer than %d bytes", MaxErrorDataBytes)
		}
	}
	return nil
}

var commandTypes = map[string]bool{
	CommandStepStarted: true, CommandStepCompleted: true, CommandStepFailed: true, CommandValueRecorded: true,
	CommandSleep: true, CommandRunCompleted: true, CommandRunFailed: true, CommandWorkflowNotFound: true,
	CommandCallbackCreated: true, CommandWaitForCallback: true, CommandInvoke: true, CommandPlanParallel: true,
	CommandMessageWaitCreated: true, CommandWaitForMessage: true,
}

// Validate checks the command: its type, each present field's limits, and the fields its type
// requires. A planParallel's step IDs must also be distinct.
func (c Command) Validate() error {
	if !commandTypes[c.Type] {
		return invalid("unknown command type %q", c.Type)
	}
	if c.StepID != "" && !stepIDRe.MatchString(c.StepID) {
		return invalid("stepId %q", c.StepID)
	}
	if len(c.Name) > MaxStepNameBytes {
		return invalid("name longer than %d", MaxStepNameBytes)
	}
	if hasNUL(c.Name) {
		return invalid("name contains NUL")
	}
	if c.CauseStepID != "" && !stepIDRe.MatchString(c.CauseStepID) {
		return invalid("causeStepId %q", c.CauseStepID)
	}
	if len(c.Value) > MaxValueBytes {
		return invalid("value longer than %d bytes", MaxValueBytes)
	}
	if c.Attempt < 0 {
		return invalid("attempt %d", c.Attempt)
	}
	if c.Retry != nil {
		if err := c.Retry.Validate(); err != nil {
			return err
		}
	}
	if c.TimeoutMs < 0 {
		return invalid("timeoutMs %d is negative", c.TimeoutMs)
	}
	if c.RetryAfterMs < 0 {
		return invalid("retryAfterMs %d is negative", c.RetryAfterMs)
	}
	if c.OutputRef != "" && !payloadIDRe.MatchString(c.OutputRef) {
		return invalid("outputRef %q", c.OutputRef)
	}
	if c.Error != nil {
		if err := c.Error.Validate(); err != nil {
			return err
		}
	}
	if c.Workflow != "" && !workflowNameRe.MatchString(c.Workflow) {
		return invalid("workflow %q", c.Workflow)
	}
	if c.Token != "" {
		if c.Type != CommandCallbackCreated {
			return invalid("token on %s", c.Type)
		}
		if _, err := ParseCallbackToken(c.Token); err != nil {
			return invalid("token: %v", err)
		}
	}
	if c.InputRef != "" && !payloadIDRe.MatchString(c.InputRef) {
		return invalid("inputRef %q", c.InputRef)
	}
	if len(c.Input) > 0 && c.InputRef != "" {
		return invalid("input and inputRef are exclusive")
	}
	if (len(c.Input) > 0 || c.InputRef != "") && c.Type != CommandInvoke {
		return invalid("input on %s", c.Type)
	}
	if len(c.Steps) > 0 && c.Type != CommandPlanParallel {
		return invalid("steps on %s", c.Type)
	}
	if (c.QueueID != "" || c.Key != "") && c.Type != CommandMessageWaitCreated {
		return invalid("queueId or key on %s", c.Type)
	}
	step := func() error {
		if c.StepID == "" || c.Name == "" {
			return invalid("%s needs stepId and name", c.Type)
		}
		return nil
	}
	switch c.Type {
	case CommandStepStarted, CommandStepFailed:
		if err := step(); err != nil {
			return err
		}
		if c.Attempt < 1 {
			return invalid("%s needs attempt ≥ 1", c.Type)
		}
		if c.Type == CommandStepFailed && c.Error == nil {
			return invalid("stepFailed needs error")
		}
	case CommandStepCompleted, CommandValueRecorded:
		return step()
	case CommandSleep:
		if err := step(); err != nil {
			return err
		}
		if c.Until == nil {
			return invalid("sleep needs until")
		}
	case CommandRunFailed:
		if c.Error == nil {
			return invalid("runFailed needs error")
		}
	case CommandWorkflowNotFound:
		if c.Workflow == "" {
			return invalid("workflowNotFound needs workflow")
		}
	case CommandCallbackCreated:
		if err := step(); err != nil {
			return err
		}
		if c.Token == "" || c.TimeoutMs < 1 {
			return invalid("callbackCreated needs token and timeoutMs ≥ 1")
		}
	case CommandWaitForCallback, CommandWaitForMessage:
		if c.StepID == "" {
			return invalid("%s needs stepId", c.Type)
		}
	case CommandMessageWaitCreated:
		if err := step(); err != nil {
			return err
		}
		if !ValidQueueID(c.QueueID) {
			return invalid("messageWaitCreated needs queueId (a UUID), got %q", c.QueueID)
		}
		if !ValidMessageKey(c.Key) {
			return invalid("messageWaitCreated needs key (^[A-Za-z0-9_.:-]{1,256}$), got %q", c.Key)
		}
		if c.TimeoutMs < MinMessageTimeoutMs || c.TimeoutMs > MaxMessageTimeoutMs {
			return invalid("messageWaitCreated needs timeoutMs from 1,000 to 2,592,000,000, got %d", c.TimeoutMs)
		}
	case CommandInvoke:
		if err := step(); err != nil {
			return err
		}
		if c.Workflow == "" {
			return invalid("invoke needs workflow")
		}
	case CommandPlanParallel:
		if len(c.Steps) == 0 || len(c.Steps) > MaxPlannedSteps {
			return invalid("planParallel needs 1-%d steps", MaxPlannedSteps)
		}
		seen := make(map[string]bool, len(c.Steps))
		for i, s := range c.Steps {
			if !stepIDRe.MatchString(s.StepID) || s.Name == "" || len(s.Name) > MaxStepNameBytes || hasNUL(s.Name) {
				return invalid("steps[%d]: stepId and name", i)
			}
			if seen[s.StepID] {
				return invalid("steps[%d]: stepId %s twice", i, s.StepID)
			}
			seen[s.StepID] = true
			if s.Retry != nil {
				if err := s.Retry.Validate(); err != nil {
					return err
				}
			}
			if s.TimeoutMs < 0 {
				return invalid("steps[%d]: timeoutMs", i)
			}
		}
	}
	return nil
}

// Validate checks the checkpoint: its invocation ID, seq ≥ 1, and every command.
func (c Checkpoint) Validate() error {
	if !invocationIDRe.MatchString(c.InvocationID) {
		return invalid("invocationId %q", c.InvocationID)
	}
	if c.Seq < 1 {
		return invalid("seq %d", c.Seq)
	}
	for i, cmd := range c.Commands {
		if err := cmd.Validate(); err != nil {
			return fmt.Errorf("commands[%d]: %w", i, err)
		}
	}
	return nil
}

// Validate checks every command of the result, and its deployment label.
func (r InvocationResult) Validate() error {
	if len(r.Deployment) > MaxDeploymentLen || hasNUL(r.Deployment) {
		return invalid("deployment longer than %d bytes or containing NUL", MaxDeploymentLen)
	}
	for i, cmd := range r.Commands {
		if err := cmd.Validate(); err != nil {
			return fmt.Errorf("commands[%d]: %w", i, err)
		}
	}
	return nil
}

// Validate checks a poll request: the app ID, a queue name, 1–10 tasks, a worker ID, at least
// one protocol version, and the announced workflows' names and policies.
func (r PollRequest) Validate() error {
	switch {
	case !uuidRe.MatchString(r.App):
		return invalid("app %q", r.App)
	case !queueRe.MatchString(r.Queue):
		return invalid("queue %q", r.Queue)
	case r.MaxTasks < 1 || r.MaxTasks > MaxPollTasks:
		return invalid("maxTasks %d is outside 1-%d", r.MaxTasks, MaxPollTasks)
	case !workerIDRe.MatchString(r.WorkerID):
		return invalid("workerId %q", r.WorkerID)
	case len(r.ProtocolVersions) == 0:
		return invalid("protocolVersions is empty")
	}
	if err := ValidateSDK(r.SDK); err != nil {
		return err
	}
	return ValidateWorkflows(r.Workflows)
}

// Limits on what an app reports about itself, in a sync reply or a pull worker's announcement.
const (
	MaxWorkflows       = 1000
	MaxSDKNameBytes    = 200
	MaxSDKVersionBytes = 100
)

// ValidateSDK checks an SDK's name and version: present, at most 200 and 100 bytes.
func ValidateSDK(s SDKInfo) error {
	switch {
	case strings.TrimSpace(s.Name) == "" || strings.TrimSpace(s.Version) == "":
		return invalid("sdk name and version are required")
	case len(s.Name) > MaxSDKNameBytes || len(s.Version) > MaxSDKVersionBytes:
		return invalid("sdk name (at most %d bytes) or version (at most %d) is too long", MaxSDKNameBytes, MaxSDKVersionBytes)
	}
	return nil
}

// ValidateWorkflows checks the workflows an app serves (sync replies and pull announcements): at
// most 1,000, each name valid and listed once, timeoutMs not negative, retry policies valid.
func ValidateWorkflows(ws []WorkflowInfo) error {
	if len(ws) > MaxWorkflows {
		return invalid("at most %d workflows", MaxWorkflows)
	}
	seen := make(map[string]bool, len(ws))
	for i, w := range ws {
		if !workflowNameRe.MatchString(w.Name) || seen[w.Name] {
			return invalid("workflows[%d].name %q is invalid or repeated", i, w.Name)
		}
		seen[w.Name] = true
		if w.TimeoutMs < 0 {
			return invalid("workflow %q has a negative timeoutMs", w.Name)
		}
		if w.Retry != nil {
			if err := w.Retry.Validate(); err != nil {
				return fmt.Errorf("workflow %q: %w", w.Name, err)
			}
		}
	}
	return nil
}

// CleanDeployment makes a deployment label valid: NUL removed, cut to 256 bytes of UTF-8.
func CleanDeployment(s string) string {
	return TruncateUTF8(strings.ReplaceAll(s, "\x00", ""), MaxDeploymentLen)
}
