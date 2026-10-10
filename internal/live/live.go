// Package live is the SDK's client for workflows-live: checkpoints, journal pages and payloads
// during an invocation, and a pull worker's heartbeat and complete. Every call carries the
// invocation token.
package live

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/jsonx"
	"github.com/ezgamehost/workflows-sdk-go/protocol"
)

// ErrSuperseded: workflows-live answered 409, so the engine has moved on. Stop at once.
var ErrSuperseded = errors.New("the engine superseded this invocation")

// CheckpointFailed: workflows-live refused a call, or didn't take it before the deadline. Code is
// what the push handler tells dispatch: "payload_unavailable" when the last answer was 503
// payload_unavailable (no step attempt is charged), else "checkpoint_failed".
type CheckpointFailed struct{ Code, Message string }

func (e *CheckpointFailed) Error() string { return e.Message }

// IO is what an invocation calls on workflows-live while it runs.
type IO interface {
	Checkpoint(ctx context.Context, cp protocol.Checkpoint) error
	JournalPage(ctx context.Context, afterSeq int64) (protocol.Journal, error)
	Payload(ctx context.Context, id string) (json.RawMessage, error)
}

// Client talks to workflows-live. 409 is ErrSuperseded; 429, 5xx and network errors are retried
// after max(Retry-After, backoff) while more than Reserve would remain before Deadline; any other
// answer fails at once.
type Client struct {
	Base     string
	Token    string
	HTTP     *http.Client
	Deadline time.Time
	Reserve  time.Duration
	Now      func() time.Time
	// FirstDelay is the first retry's backoff (default 200 ms), doubling to at most 5 s.
	FirstDelay time.Duration
}

const maxBackoff = 5 * time.Second

func (c *Client) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *Client) do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	what, _, _ := strings.Cut(path, "?")
	payloadDown := false
	last := "the deadline passed"
	failed := func(msg string) error {
		code := "checkpoint_failed"
		if payloadDown {
			code = "payload_unavailable"
		}
		return &CheckpointFailed{Code: code, Message: msg}
	}
	first := c.FirstDelay
	if first <= 0 {
		first = 200 * time.Millisecond
	}
	httpc := c.HTTP
	if httpc == nil {
		httpc = http.DefaultClient
	}
	for attempt := 0; ; attempt++ {
		left := c.Deadline.Sub(c.now())
		if left <= 0 {
			return nil, failed("no time left for " + what + ": " + last)
		}
		rctx, cancel := context.WithTimeout(ctx, left)
		var rd io.Reader
		if body != nil {
			rd = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(rctx, method, strings.TrimRight(c.Base, "/")+path, rd)
		if err != nil {
			cancel()
			return nil, &CheckpointFailed{Code: "checkpoint_failed", Message: err.Error()}
		}
		req.Header.Set("Authorization", "Bearer "+c.Token)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := httpc.Do(req)
		var wait time.Duration
		if err != nil {
			last, payloadDown = "network error", false
		} else {
			data, rerr := io.ReadAll(io.LimitReader(res.Body, 64<<20))
			res.Body.Close()
			switch {
			case res.StatusCode/100 == 2 && rerr == nil:
				cancel()
				return data, nil
			case res.StatusCode/100 == 2:
				last, payloadDown = "the answer was cut off", false
			case res.StatusCode == http.StatusConflict:
				cancel()
				return nil, ErrSuperseded
			case res.StatusCode != http.StatusTooManyRequests && res.StatusCode < 500:
				cancel()
				return nil, &CheckpointFailed{Code: "checkpoint_failed", Message: fmt.Sprintf("workflows-live answered %d for %s", res.StatusCode, what)}
			default:
				code := errorCode(data)
				last = "HTTP " + strconv.Itoa(res.StatusCode)
				if code != "" {
					last += " " + code
				}
				payloadDown = res.StatusCode == http.StatusServiceUnavailable && code == "payload_unavailable"
				wait = retryAfter(res.Header.Get("Retry-After"), c.now())
			}
		}
		cancel()
		// Retry-After never shortens the backoff: a 0 or a past date mustn't spin.
		delay := min(maxBackoff, first<<min(attempt, 10))
		delay = max(delay, wait)
		if c.Deadline.Sub(c.now())-delay <= c.Reserve {
			return nil, failed("workflows-live didn't take " + what + ": " + last)
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, failed(what + ": " + ctx.Err().Error())
		case <-t.C:
		}
	}
}

func errorCode(b []byte) string {
	var v struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if len(b) > 64<<10 || json.Unmarshal(b, &v) != nil {
		return ""
	}
	return v.Error.Code
}

func retryAfter(h string, now time.Time) time.Duration {
	if h == "" {
		return 0
	}
	if n, err := strconv.Atoi(h); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

// Checkpoint sends cp after checking it as the engine will: the SDK never sends a checkpoint
// the engine refuses.
func (c *Client) Checkpoint(ctx context.Context, cp protocol.Checkpoint) error {
	if err := cp.Validate(); err != nil {
		return fmt.Errorf("workflows: the SDK built an invalid checkpoint: %w", err)
	}
	b, err := jsonx.Marshal(cp)
	if err != nil {
		return err
	}
	_, err = c.do(ctx, http.MethodPost, "/v1/invocations/checkpoint", b)
	return err
}

// JournalPage fetches the entries after afterSeq.
func (c *Client) JournalPage(ctx context.Context, afterSeq int64) (protocol.Journal, error) {
	b, err := c.do(ctx, http.MethodGet, "/v1/invocations/journal?afterSeq="+strconv.FormatInt(afterSeq, 10), nil)
	if err != nil {
		return protocol.Journal{}, err
	}
	var j protocol.Journal
	if err := json.Unmarshal(b, &j); err != nil {
		return protocol.Journal{}, &CheckpointFailed{Code: "checkpoint_failed", Message: "workflows-live sent a journal page that isn't JSON"}
	}
	return j, nil
}

// Payload fetches an offloaded output or input.
func (c *Client) Payload(ctx context.Context, id string) (json.RawMessage, error) {
	b, err := c.do(ctx, http.MethodGet, "/v1/invocations/payloads/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	if !json.Valid(b) {
		return nil, &CheckpointFailed{Code: "checkpoint_failed", Message: "workflows-live sent a payload that isn't JSON"}
	}
	return b, nil
}

// Complete sends a pull invocation's result.
func (c *Client) Complete(ctx context.Context, r protocol.InvocationResult) error {
	if err := r.Validate(); err != nil {
		return fmt.Errorf("workflows: the SDK built an invalid result: %w", err)
	}
	b, err := jsonx.Marshal(r)
	if err != nil {
		return err
	}
	_, err = c.do(ctx, http.MethodPost, "/v1/invocations/complete", b)
	return err
}

// Heartbeat extends a pull invocation's heartbeat timer; the answer says whether the run is
// cancelling.
func (c *Client) Heartbeat(ctx context.Context) (protocol.HeartbeatResponse, error) {
	b, err := c.do(ctx, http.MethodPost, "/v1/invocations/heartbeat", []byte("{}"))
	if err != nil {
		return protocol.HeartbeatResponse{}, err
	}
	var h protocol.HeartbeatResponse
	if len(b) == 0 {
		return h, nil
	}
	return h, json.Unmarshal(b, &h)
}
