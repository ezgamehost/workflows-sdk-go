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
	"testing"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// fakeQueues is a local queues-api: it records every request and answers from a script (the
// last answer repeats; none answers a message ID per message).
type fakeQueues struct {
	*httptest.Server
	mu      sync.Mutex
	seen    []seenRequest
	answers []func(w http.ResponseWriter)
}

type seenRequest struct {
	method, path string
	header       http.Header
	body         map[string]any
}

func newFakeQueues(t *testing.T, answers ...func(w http.ResponseWriter)) *fakeQueues {
	f := &fakeQueues{answers: answers}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(data, &body)
		f.mu.Lock()
		f.seen = append(f.seen, seenRequest{r.Method, r.URL.EscapedPath(), r.Header.Clone(), body})
		n := len(f.seen) - 1
		var answer func(http.ResponseWriter)
		if n < len(f.answers) {
			answer = f.answers[n]
		} else if len(f.answers) > 0 {
			answer = f.answers[len(f.answers)-1]
		}
		f.mu.Unlock()
		if answer != nil {
			answer(w)
			return
		}
		msgs, _ := body["messages"].([]any)
		okAnswer(len(msgs))(w)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeQueues) requests() []seenRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seenRequest(nil), f.seen...)
}

func okAnswer(n int) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		msgs := make([]map[string]any, n)
		for i := range msgs {
			msgs[i] = map[string]any{"messageId": fmt.Sprintf("0199a3d0-0000-7000-8000-00000000000%d", i)}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"messages": msgs})
	}
}

func errAnswer(status int, code string, retryAfter string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": code + " here"}})
	}
}

const testQueuesKey = "ezgh_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const queuePath = "/v1/organizations/org-1/projects/prj-1/queues/que_abc/messages"

func queuesClient(t *testing.T, base string, retries ...int) *QueuesClient {
	t.Helper()
	o := QueuesClientOptions{APIKey: testQueuesKey, OrganizationID: "org-1", ProjectID: "prj-1", BaseURL: base, MaxRetryDelay: 20 * time.Millisecond}
	if len(retries) > 0 {
		o.Retries = &retries[0]
	}
	c, err := NewQueuesClient(o)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestQueuesSendPostsTheMessagesWithTheKeyAndIdempotencyKey(t *testing.T) {
	f := newFakeQueues(t)
	out, err := queuesClient(t, f.URL+"/").Send(context.Background(), "que_abc", []QueueMessage{{Body: "hi", Attributes: map[string]string{"orderId": "o-1"}}}, SendOptions{IdempotencyKey: "k-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Messages) != 1 || out.Messages[0].MessageID != "0199a3d0-0000-7000-8000-000000000000" {
		t.Fatalf("answer %s", show(out))
	}
	r := f.requests()
	if len(r) != 1 || r[0].method != http.MethodPost || r[0].path != queuePath {
		t.Fatalf("requests %+v", r)
	}
	if r[0].header.Get("Authorization") != "Bearer "+testQueuesKey || r[0].header.Get("Content-Type") != "application/json" || r[0].header.Get("Idempotency-Key") != "k-1" {
		t.Fatalf("headers %v", r[0].header)
	}
	if got := show(r[0].body); got != `{"messages":[{"attributes":{"orderId":"o-1"},"body":"hi"}]}` {
		t.Fatalf("body %s", got)
	}
}

func TestQueuesSendDefaultsAndEscaping(t *testing.T) {
	var urls []string
	httpc := &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		urls = append(urls, r.URL.String())
		rec := httptest.NewRecorder()
		okAnswer(1)(rec)
		return rec.Result(), nil
	})}
	c, err := NewQueuesClient(QueuesClientOptions{APIKey: "k", OrganizationID: "o/1", ProjectID: "p", HTTPClient: httpc})
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	if _, err := c.Send(context.Background(), "que_a b", []QueueMessage{{Body: "x", DelaySeconds: &zero}}, SendOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(urls) != 1 || urls[0] != "https://queues-api.ezghcloud.com/v1/organizations/o%2F1/projects/p/queues/que_a%20b/messages" {
		t.Fatalf("urls %v", urls)
	}
	if _, err := NewQueuesClient(QueuesClientOptions{OrganizationID: "o", ProjectID: "p"}); err == nil {
		t.Fatal("a client without an API key")
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestQueuesSendResendsWithTheSameKeyAfterRetryAfter(t *testing.T) {
	f := newFakeQueues(t, errAnswer(429, "too_many_requests", "1"), errAnswer(502, "bad_gateway", ""), errAnswer(409, "idempotency_in_progress", "0"), okAnswer(1))
	c, _ := NewQueuesClient(QueuesClientOptions{APIKey: "k", OrganizationID: "org-1", ProjectID: "prj-1", BaseURL: f.URL})
	start := time.Now()
	if _, err := c.Send(context.Background(), "que_abc", []QueueMessage{{Body: "x"}}, SendOptions{}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 950*time.Millisecond {
		t.Fatalf("Retry-After: 1 wasn't honored (%v)", d)
	}
	r := f.requests()
	if len(r) != 4 {
		t.Fatalf("%d requests", len(r))
	}
	key := r[0].header.Get("Idempotency-Key")
	if len(key) != 32 {
		t.Fatalf("random key %q", key)
	}
	for _, x := range r {
		if x.header.Get("Idempotency-Key") != key {
			t.Fatalf("the key changed between attempts")
		}
	}
}

func TestQueuesSendErrors(t *testing.T) {
	ctx := context.Background()
	msgs := []QueueMessage{{Body: "x"}}
	cases := []struct {
		answer        func(http.ResponseWriter)
		retries       int
		status        int
		code          string
		requests      int
		retryAfter    time.Duration
		checkResponse bool
	}{
		{errAnswer(400, "invalid_request", ""), 3, 400, "invalid_request", 1, 0, true},
		{errAnswer(429, "quota_exceeded", ""), 3, 429, "quota_exceeded", 1, 0, true},
		{errAnswer(429, "too_many_requests", "0"), 2, 429, "too_many_requests", 3, 0, true},
		{errAnswer(500, "internal_error", ""), 1, 500, "internal_error", 2, 0, true},
		{func(w http.ResponseWriter) { w.WriteHeader(404) }, 3, 404, "unknown", 1, 0, true},
		{func(w http.ResponseWriter) { w.Header().Set("Location", "https://elsewhere.test/"); w.WriteHeader(302) }, 3, 302, "redirect", 1, 0, true},
	}
	for _, c := range cases {
		f := newFakeQueues(t, c.answer)
		_, err := queuesClient(t, f.URL, c.retries).Send(ctx, "que_abc", msgs, SendOptions{})
		var qe *QueuesAPIError
		if !errors.As(err, &qe) || qe.Status != c.status || qe.Code != c.code || qe.RetryAfter != c.retryAfter {
			t.Fatalf("%d %s: got %v", c.status, c.code, err)
		}
		if n := len(f.requests()); n != c.requests {
			t.Fatalf("%d %s: %d requests, want %d", c.status, c.code, n, c.requests)
		}
	}
	// Network errors are resent, then reported as network_error.
	f := newFakeQueues(t)
	base := f.URL
	f.Close()
	_, err := queuesClient(t, base, 1).Send(ctx, "que_abc", msgs, SendOptions{})
	var qe *QueuesAPIError
	if !errors.As(err, &qe) || qe.Status != 0 || qe.Code != "network_error" {
		t.Fatalf("network error: %v", err)
	}
	// Bad arguments are refused before sending.
	g := newFakeQueues(t)
	c := queuesClient(t, g.URL)
	for _, bad := range [][]QueueMessage{nil, make([]QueueMessage, 11)} {
		if _, err := c.Send(ctx, "que_abc", bad, SendOptions{}); err == nil {
			t.Fatalf("%d messages were accepted", len(bad))
		}
	}
	if _, err := c.Send(ctx, "que_abc", msgs, SendOptions{IdempotencyKey: "has space"}); err == nil {
		t.Fatal("a key with a space was accepted")
	}
	if len(g.requests()) != 0 {
		t.Fatal("a refused call reached the server")
	}
}

func TestQueuesClientRedactsItsKey(t *testing.T) {
	c := queuesClient(t, "https://q.test")
	b, _ := json.Marshal(c)
	for _, s := range []string{fmt.Sprint(c), fmt.Sprintf("%+v", *c), fmt.Sprintf("%#v", c), string(b)} {
		if strings.Contains(s, testQueuesKey) {
			t.Fatalf("the key shows: %s", s)
		}
	}
}

// The key derivation vector, the same in every EZGH Workflows SDK.
func TestSendToQueueDigestVector(t *testing.T) {
	if got := protocol.StepID("notify", 0); got != "70d6d439c310423a9f3007a881987180" {
		t.Fatalf("step ID %s", got)
	}
	if got := SendToQueueDigest("0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a", "70d6d439c310423a9f3007a881987180"); got != "ac73d9ecbcab73fa5c63cae08e17eac274aca628299cc68bd17498109cb268c0" {
		t.Fatalf("digest %s", got)
	}
}

func notifyWF(c *QueuesClient) Workflow {
	return New("notify", func(ctx Context, _ none) (int, error) {
		sent, err := SendToQueue(ctx, "notify", c, "que_abc", []QueueMessage{
			{Body: "a", GroupID: "acct-9"},
			{Body: "b"},
			{Body: "c", GroupID: "acct-9", DedupID: "mine"},
		})
		return len(sent.Messages), err
	})
}

func TestSendToQueueSendsOnceWithDerivedKeysAndRecordsTheResult(t *testing.T) {
	f := newFakeQueues(t)
	digest := SendToQueueDigest(testRunID, protocol.StepID("notify", 0))
	cmds := run(t, notifyWF(queuesClient(t, f.URL)), invocation("notify", nil), &fakeIO{})
	r := f.requests()
	if len(r) != 1 || r[0].path != queuePath || r[0].header.Get("Idempotency-Key") != "wfq_"+digest {
		t.Fatalf("requests %+v", r)
	}
	want := fmt.Sprintf(`{"messages":[{"body":"a","dedupId":"%s:0","groupId":"acct-9"},{"body":"b"},{"body":"c","dedupId":"mine","groupId":"acct-9"}]}`, digest)
	if got := show(r[0].body); got != want {
		t.Fatalf("body %s\nwant %s", got, want)
	}
	if got := types(cmds); len(got) != 2 || got[0] != "stepCompleted" || got[1] != "runCompleted" || string(cmds[1].Output) != "3" {
		t.Fatalf("commands %s", show(cmds))
	}
	var rec SentMessages
	if err := json.Unmarshal(cmds[0].Output, &rec); err != nil || len(rec.Messages) != 3 || cmds[0].StepID != protocol.StepID("notify", 0) {
		t.Fatalf("recorded %s", cmds[0].Output)
	}

	// Memoized: a replay returns the recorded result without calling the server.
	replay := run(t, notifyWF(queuesClient(t, f.URL)), invocation("notify", nil, entry(1, "notify", "run", "completed", 1, string(cmds[0].Output))), &fakeIO{})
	if len(replay) != 1 || replay[0].Type != "runCompleted" || string(replay[0].Output) != "3" {
		t.Fatalf("replay %s", show(replay))
	}
	if n := len(f.requests()); n != 1 {
		t.Fatalf("the replay sent again: %d requests", n)
	}
}

func TestSendToQueueRetriedAttemptSendsTheSameKey(t *testing.T) {
	f := newFakeQueues(t)
	retrying := entry(1, "notify", "run", "retrying", 1, "")
	retrying.Error = &protocol.WireError{Code: "user", Name: "QueuesAPIError", Message: "unavailable"}
	cmds := run(t, notifyWF(queuesClient(t, f.URL)), invocation("notify", nil, retrying), &fakeIO{})
	if cmds[len(cmds)-1].Type != "runCompleted" {
		t.Fatalf("commands %s", show(cmds))
	}
	if got := f.requests()[0].header.Get("Idempotency-Key"); got != "wfq_"+SendToQueueDigest(testRunID, protocol.StepID("notify", 0)) {
		t.Fatalf("key %s", got)
	}
}

func TestSendToQueueOtherStepsGetOtherKeys(t *testing.T) {
	f := newFakeQueues(t)
	c := queuesClient(t, f.URL)
	wf := New("two", func(ctx Context, _ none) (int, error) {
		for _, b := range []string{"1", "2"} {
			if _, err := SendToQueue(ctx, "n", c, "que_abc", []QueueMessage{{Body: b}}); err != nil {
				return 0, err
			}
		}
		return 0, nil
	})
	run(t, wf, invocation("two", nil), &fakeIO{})
	r := f.requests()
	if len(r) != 2 || r[1].header.Get("Idempotency-Key") != "wfq_"+SendToQueueDigest(testRunID, protocol.StepID("n", 1)) || r[0].header.Get("Idempotency-Key") == r[1].header.Get("Idempotency-Key") {
		t.Fatalf("requests %+v", r)
	}
	if _, ok := r[0].body["messages"].([]any)[0].(map[string]any)["dedupId"]; ok {
		t.Fatal("a standard queue's message got a dedupId")
	}
}

func TestSendToQueueRefusalIsNonRetriableAndOutageRetryable(t *testing.T) {
	refused := newFakeQueues(t, errAnswer(403, "access_denied", ""))
	cmds := run(t, notifyWF(queuesClient(t, refused.URL)), invocation("notify", nil), &fakeIO{})
	if cmds[0].Type != "stepFailed" || cmds[0].Retryable || cmds[0].Error.Code != "workflows.non_retriable" || cmds[0].Error.Name != "QueuesAPIError" ||
		string(cmds[0].Error.Data) != `{"code":"access_denied","status":403}` {
		t.Fatalf("refused %s", show(cmds))
	}
	down := newFakeQueues(t, errAnswer(503, "unavailable", "0"))
	cmds = run(t, notifyWF(queuesClient(t, down.URL)), invocation("notify", nil), &fakeIO{})
	if cmds[0].Type != "stepFailed" || !cmds[0].Retryable || cmds[0].Error.Code != "user" || string(cmds[0].Error.Data) != `{"code":"unavailable","status":503}` {
		t.Fatalf("down %s", show(cmds))
	}
}

func TestSendToQueueRefusesBadArgumentsBeforeAnyStep(t *testing.T) {
	f := newFakeQueues(t)
	c := queuesClient(t, f.URL)
	wf := New("bad", func(ctx Context, _ none) (SentMessages, error) { return SendToQueue(ctx, "n", c, "que_abc", nil) })
	cmds := run(t, wf, invocation("bad", nil), &fakeIO{})
	if len(cmds) != 1 || cmds[0].Type != "runFailed" || cmds[0].Error.Name != "TypeError" || len(f.requests()) != 0 {
		t.Fatalf("commands %s", show(cmds))
	}
}
