package workflows

import (
	"bytes"
	"context"
	"crypto/sha256"
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
)

// DefaultQueuesBaseURL is the EZGH Queues API, which takes sends.
const DefaultQueuesBaseURL = "https://queues-api.ezghcloud.com"

// QueuesClientOptions configures a QueuesClient for one project.
type QueuesClientOptions struct {
	// APIKey is an EZGH API key (or another bearer credential) allowed queues.messages.send on the
	// queues the client sends to. Omitted on an EZGH compute instance with a Bot attached: the
	// client then uses the Bot's short-lived key from the metadata service.
	APIKey         string
	OrganizationID string
	ProjectID      string
	BaseURL        string       // default DefaultQueuesBaseURL
	HTTPClient     *http.Client // default: a client with a 70 s timeout
	// Retries is how many times a 429 (but not quota_exceeded), a 5xx, a 409
	// idempotency_in_progress or a network error is resent, after Retry-After, at most
	// MaxRetryDelay (default 10 s), always with the same Idempotency-Key. nil means the default,
	// 3; a pointer to 0 means none.
	Retries       *int
	MaxRetryDelay time.Duration
}

// QueuesClient sends messages to EZGH Queues for one project: pass it to SendToQueue, or call
// Send directly outside a workflow. Its API key never appears when the client is printed, logged
// or marshalled.
type QueuesClient struct {
	cred     credential
	base     string
	httpc    *http.Client
	retries  int
	maxDelay time.Duration
}

func (c QueuesClient) String() string             { return "workflows.QueuesClient{" + c.base + "}" }
func (c QueuesClient) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, c.String()) }
func (c QueuesClient) GoString() string           { return c.String() }
func (c QueuesClient) LogValue() slog.Value       { return slog.StringValue(c.String()) }
func (c QueuesClient) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"base": c.base})
}

// NewQueuesClient checks the options and returns a client.
func NewQueuesClient(o QueuesClientOptions) (*QueuesClient, error) {
	cred, ok := newCredential(o.APIKey, o.HTTPClient)
	if !ok || o.OrganizationID == "" || o.ProjectID == "" {
		return nil, errors.New("workflows.NewQueuesClient needs APIKey, OrganizationID and ProjectID")
	}
	base := strings.TrimRight(o.BaseURL, "/")
	if base == "" {
		base = DefaultQueuesBaseURL
	}
	if _, err := url.Parse(base); err != nil {
		return nil, fmt.Errorf("workflows.NewQueuesClient: BaseURL: %w", err)
	}
	c := &QueuesClient{cred: cred, base: base + "/v1/organizations/" + url.PathEscape(o.OrganizationID) + "/projects/" + url.PathEscape(o.ProjectID),
		httpc: noRedirects(o.HTTPClient), retries: 3, maxDelay: 10 * time.Second}
	if o.Retries != nil {
		if *o.Retries < 0 {
			return nil, errors.New("workflows.NewQueuesClient: Retries is at least 0")
		}
		c.retries = *o.Retries
	}
	if o.MaxRetryDelay > 0 {
		c.maxDelay = o.MaxRetryDelay
	}
	return c, nil
}

// QueueMessage is a message to send. Body is UTF-8 text; Body and Attributes together are at
// most 256 KiB. GroupID is required on FIFO queues and refused on standard ones; DedupID is FIFO
// only. DelaySeconds (0–900) is standard queues only: nil uses the queue's delay.
type QueueMessage struct {
	Body         string            `json:"body"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	DelaySeconds *int              `json:"delaySeconds,omitempty"`
	GroupID      string            `json:"groupId,omitempty"`
	DedupID      string            `json:"dedupId,omitempty"`
}

// SentMessage is one message's result. Seq is FIFO only. Duplicate is true when a FIFO DedupID
// was seen in the last 5 minutes, so the message wasn't stored again (MessageID is the
// original's).
type SentMessage struct {
	MessageID string `json:"messageId"`
	Seq       string `json:"seq,omitempty"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

// SentMessages holds one result per message, in order.
type SentMessages struct {
	Messages []SentMessage `json:"messages"`
}

// SendOptions: IdempotencyKey (1–200 printable ASCII characters) names the send; without one,
// Send uses a random key, the same on each of its resends.
type SendOptions struct {
	IdempotencyKey string
}

// QueuesAPIError is an error answer from the Queues API: the HTTP status, the platform's error
// code and Retry-After. Status 0 with Code "network_error": every attempt failed in transit.
type QueuesAPIError struct {
	Status     int
	Code       string
	Message    string
	RetryAfter time.Duration
}

func (e *QueuesAPIError) Error() string {
	return fmt.Sprintf("queues API %d %s: %s", e.Status, e.Code, e.Message)
}

// resendable: answers a resend with the same Idempotency-Key may fix. A 429 quota_exceeded
// isn't one: it isn't a failure to retry on a timer.
func (e *QueuesAPIError) resendable() bool {
	switch {
	case e.Status == http.StatusTooManyRequests:
		return e.Code != "quota_exceeded"
	case e.Status == http.StatusConflict:
		return e.Code == "idempotency_in_progress"
	}
	return e.Status >= 500
}

// refused: a 3xx or 4xx that retrying the step won't fix (all but 408, 429 and 409
// idempotency_in_progress).
func (e *QueuesAPIError) refused() bool {
	return e.Status >= 300 && e.Status < 500 && e.Status != http.StatusRequestTimeout && e.Status != http.StatusTooManyRequests &&
		!(e.Status == http.StatusConflict && e.Code == "idempotency_in_progress")
}

// wireData is what a step failure records, so a replay's *Error keeps the status and code.
func (e *QueuesAPIError) wireData() json.RawMessage {
	b, _ := jsonx.Marshal(map[string]any{"status": e.Status, "code": e.Code})
	return b
}

func validIdempotencyKey(k string) bool {
	if len(k) < 1 || len(k) > 200 {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return false
		}
	}
	return true
}

func queuesAPIError(res *http.Response, data []byte) *QueuesAPIError {
	if res.StatusCode >= 300 && res.StatusCode < 400 {
		return &QueuesAPIError{Status: res.StatusCode, Code: "redirect", Message: "the Queues API answered a redirect; requests are never redirected, so check BaseURL"}
	}
	var eb struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &eb)
	e := &QueuesAPIError{Status: res.StatusCode, Code: eb.Error.Code, Message: eb.Error.Message}
	if e.Code == "" {
		e.Code = "unknown"
	}
	if e.Message == "" {
		e.Message = http.StatusText(res.StatusCode)
	}
	if s, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && s >= 0 {
		e.RetryAfter = time.Duration(s) * time.Second
	}
	return e
}

// Send sends 1–10 messages to a queue (queues.messages.send) in one request: all are stored, or
// none. Every attempt carries the same Idempotency-Key, which the API honors for 48 hours: a
// resend gets the first answer instead of sending again.
func (c *QueuesClient) Send(ctx context.Context, queueID string, msgs []QueueMessage, o SendOptions) (*SentMessages, error) {
	if queueID == "" {
		return nil, errors.New("workflows.QueuesClient.Send needs a queue ID")
	}
	if len(msgs) < 1 || len(msgs) > 10 {
		return nil, fmt.Errorf("workflows.QueuesClient.Send takes 1 to 10 messages, got %d", len(msgs))
	}
	key := o.IdempotencyKey
	if key == "" {
		key = newKey()
	} else if !validIdempotencyKey(key) {
		return nil, errors.New("workflows.QueuesClient.Send: an IdempotencyKey is 1–200 printable ASCII characters")
	}
	payload, err := jsonx.Marshal(map[string]any{"messages": msgs})
	if err != nil {
		return nil, err
	}
	target := c.base + "/queues/" + url.PathEscape(queueID) + "/messages"
	reauth := false // an instance key the API refused is replaced once per call
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		apiKey, err := c.cred.key(ctx)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		res, err := c.httpc.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if attempt >= c.retries {
				return nil, &QueuesAPIError{Code: "network_error", Message: "sending to the queue failed in transit: " + err.Error()}
			}
			sleepCtx(ctx, min(c.maxDelay, 500*time.Millisecond<<min(attempt, 10)))
			continue
		}
		data, rerr := io.ReadAll(io.LimitReader(res.Body, 4<<20))
		res.Body.Close()
		if res.StatusCode/100 == 2 {
			if rerr != nil {
				return nil, rerr
			}
			var out SentMessages
			if err := json.Unmarshal(data, &out); err != nil || out.Messages == nil {
				return nil, &QueuesAPIError{Status: res.StatusCode, Code: "invalid_response", Message: "the Queues API answered without a messages list"}
			}
			return &out, nil
		}
		ae := queuesAPIError(res, data)
		if res.StatusCode == http.StatusUnauthorized && !reauth && c.cred.refused(apiKey) {
			reauth = true
			attempt--
			continue
		}
		if !ae.resendable() || attempt >= c.retries {
			return nil, ae
		}
		wait := 500 * time.Millisecond << min(attempt, 10)
		if s, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && s >= 0 {
			wait = time.Duration(s) * time.Second
		}
		sleepCtx(ctx, min(c.maxDelay, wait))
		if ctx.Err() != nil {
			return nil, ae
		}
	}
}

// SendToQueueDigest is SendToQueue's key material: lowercase hex of SHA-256(runID + ":" +
// stepID). The step sends the Idempotency-Key "wfq_" + digest, and gives each FIFO message (one
// with a GroupID) without a DedupID the DedupID digest + ":" + its index.
func SendToQueueDigest(runID, stepID string) string {
	sum := sha256.Sum256([]byte(runID + ":" + stepID))
	return hex.EncodeToString(sum[:])
}

// SendToQueue sends 1–10 messages to an EZGH queue as a step (Run around c.Send) and returns the
// recorded result: a replay returns it without sending again. Every attempt sends the
// Idempotency-Key "wfq_" + SendToQueueDigest(runID, stepID), and each message with a GroupID
// (FIFO) and no DedupID of its own gets the DedupID <digest>:<index>, so the step sends its
// messages once; standard queues refuse DedupID, so messages without a GroupID go as they are. A
// 4xx refusal (other than 408, 429 and 409 idempotency_in_progress) fails the step without
// retries; the error matches *QueuesAPIError with errors.As.
func SendToQueue(ctx Context, name string, c *QueuesClient, queueID string, msgs []QueueMessage) (SentMessages, error) {
	if _, err := branchOf(ctx); err != nil {
		return SentMessages{}, err
	}
	if c == nil {
		return SentMessages{}, argumentError("workflows.SendToQueue needs a QueuesClient")
	}
	if queueID == "" {
		return SentMessages{}, argumentError("workflows.SendToQueue needs a queue ID")
	}
	if len(msgs) < 1 || len(msgs) > 10 {
		return SentMessages{}, argumentError("workflows.SendToQueue takes 1 to 10 messages, got %d", len(msgs))
	}
	runID := ctx.Run().ID
	return Run(ctx, name, func(sc StepContext) (SentMessages, error) {
		// IdempotencyKey is "<runId>:<stepId>:<attempt>": without the attempt, it's the same on every
		// attempt of this step (and different for every other step and run).
		k := sc.IdempotencyKey()
		stepID := strings.TrimPrefix(k[:strings.LastIndexByte(k, ':')], runID+":")
		digest := SendToQueueDigest(runID, stepID)
		out, err := c.Send(sc, queueID, withDedupIDs(msgs, digest), SendOptions{IdempotencyKey: "wfq_" + digest})
		if err != nil {
			var qe *QueuesAPIError
			if errors.As(err, &qe) && qe.refused() {
				return SentMessages{}, NonRetriable(err)
			}
			return SentMessages{}, err
		}
		return *out, nil
	})
}

// withDedupIDs is a copy of msgs in which each message with a GroupID and no DedupID has the
// DedupID digest:index.
func withDedupIDs(msgs []QueueMessage, digest string) []QueueMessage {
	out := make([]QueueMessage, len(msgs))
	for i, m := range msgs {
		if m.GroupID != "" && m.DedupID == "" {
			m.DedupID = digest + ":" + strconv.Itoa(i)
		}
		out[i] = m
	}
	return out
}
