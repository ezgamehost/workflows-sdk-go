package workflows

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

const (
	testSecret  = "whsec_AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA"
	otherSecret = "whsec_ISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0-P0A"
)

func mustHandler(t *testing.T, o HandlerOptions) http.Handler {
	t.Helper()
	if o.Workflows == nil {
		o.Workflows = []Workflow{sequenceWF(), New("tuned", func(ctx Context, _ none) (int, error) { return step(ctx, "t", 1) }, WithRetry(attempts(2)))}
	}
	if o.SigningSecrets == nil {
		o.SigningSecrets = []string{"", testSecret}
	}
	if o.Deployment == "" {
		o.Deployment = "dpl_test"
	}
	h, err := Handler(o)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func signed(t *testing.T, secret string, at time.Time, body []byte) *http.Request {
	t.Helper()
	key, err := protocol.DecodeSecret(secret)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set(protocol.SignatureHeader, protocol.Sign([][]byte{key}, at, body))
	return req
}

func serve(h http.Handler, req *http.Request) (int, map[string]any, string) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out, rec.Body.String()
}

func syncBody() []byte {
	return []byte(`{"protocolVersion":"1.1","kind":"sync","invocationId":"inv_aaaqeayeaudaocajbifqydiob4","token":"","endpoints":{"live":"http://x"},"deadline":"2030-01-01T00:00:00Z","cancel":null}`)
}

func TestHandlerRefusals(t *testing.T) {
	h := mustHandler(t, HandlerOptions{})
	if code, out, _ := serve(h, httptest.NewRequest(http.MethodGet, "/", nil)); code != 405 || out["code"] != "method_not_allowed" {
		t.Fatalf("GET: %d %v", code, out)
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	if code, out, _ := serve(h, req); code != 401 || out["code"] != "invalid_signature" {
		t.Fatalf("unsigned: %d %v", code, out)
	}
	req = signed(t, testSecret, time.Now(), syncBody())
	req.Body = io.NopCloser(strings.NewReader(`{"tampered":true}`))
	if code, out, _ := serve(h, req); code != 401 || out["code"] != "invalid_signature" {
		t.Fatalf("tampered: %d %v", code, out)
	}
	for _, at := range []time.Time{time.Now().Add(-10 * time.Minute), time.Now().Add(10 * time.Minute)} {
		if code, out, _ := serve(h, signed(t, testSecret, at, syncBody())); code != 401 || out["code"] != "expired_signature" {
			t.Fatalf("expired: %d %v", code, out)
		}
	}
	if code, out, _ := serve(h, signed(t, testSecret, time.Now(), []byte(`{"protocolVersion":"2.0","kind":"sync"}`))); code != 400 || out["code"] != "invalid_invocation" {
		t.Fatalf("bad invocation: %d %v", code, out)
	}
}

type failingReader struct{ t *testing.T }

func (f failingReader) Read([]byte) (int, error) { f.t.Error("the body was read"); return 0, io.EOF }

func TestExpiredSignatureIsRefusedBeforeTheBodyIsRead(t *testing.T) {
	h := mustHandler(t, HandlerOptions{})
	key, _ := protocol.DecodeSecret(testSecret)
	req := httptest.NewRequest(http.MethodPost, "/", failingReader{t})
	req.Header.Set(protocol.SignatureHeader, protocol.Sign([][]byte{key}, time.Now().Add(-10*time.Minute), []byte("{}")))
	if code, out, _ := serve(h, req); code != 401 || out["code"] != "expired_signature" {
		t.Fatalf("%d %v", code, out)
	}
}

func TestBodyOver8MiB(t *testing.T) {
	h := mustHandler(t, HandlerOptions{})
	big := bytes.Repeat([]byte("x"), 8<<20+1)
	req := signed(t, testSecret, time.Now(), big)
	if code, out, _ := serve(h, req); code != 413 || out["code"] != "payload_too_large" {
		t.Fatalf("declared: %d %v", code, out)
	}
	req = signed(t, testSecret, time.Now(), big)
	req.ContentLength = -1
	req.Body = io.NopCloser(bytes.NewReader(big))
	if code, out, _ := serve(h, req); code != 413 || out["code"] != "payload_too_large" {
		t.Fatalf("streamed: %d %v", code, out)
	}
}

func TestSyncReply(t *testing.T) {
	h := mustHandler(t, HandlerOptions{MaxInvocationSeconds: 120})
	code, _, _ := serve(h, signed(t, otherSecret, time.Now(), syncBody()))
	if code != 401 {
		t.Fatalf("an unknown secret: %d", code)
	}
	h = mustHandler(t, HandlerOptions{SigningSecrets: []string{testSecret, otherSecret}, MaxInvocationSeconds: 120})
	code, _, body := serve(h, signed(t, otherSecret, time.Now(), syncBody()))
	want := `{"protocolVersions":["1.0","1.1","1.2"],"sdk":{"name":"github.com/ezgamehost/workflows-sdk-go","version":"` + SDKVersion + `"},"maxInvocationSeconds":120,"workflows":[{"name":"sequence"},{"name":"tuned","retry":{"maxAttempts":2,"initialMs":1000,"maxMs":600000,"multiplier":2}}],"deployment":"dpl_test"}`
	if code != 200 || body != want {
		t.Fatalf("%d\n got %s\nwant %s", code, body, want)
	}
}

// liveServer is a fake workflows-live answering checkpoints with status.
func liveServer(t *testing.T, status func(n int) (int, string)) (*httptest.Server, *atomic.Int32) {
	var n atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st, body := status(int(n.Add(1)))
		w.WriteHeader(st)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s, &n
}

func orchestrateBody(t *testing.T, liveURL, workflow string, deadline time.Time) []byte {
	inv := invocation(workflow, map[string]int{"N": 1})
	inv.Endpoints.Live = liveURL
	inv.Deadline = deadline
	b, err := json.Marshal(inv)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestOrchestrateAnswers(t *testing.T) {
	ok, _ := liveServer(t, func(int) (int, string) { return 204, "" })
	h := mustHandler(t, HandlerOptions{})
	code, _, body := serve(h, signed(t, testSecret, time.Now(), orchestrateBody(t, ok.URL, "sequence", time.Now().Add(time.Minute))))
	if code != 200 || !strings.Contains(body, `"runCompleted"`) || !strings.Contains(body, `"deployment":"dpl_test"`) || !strings.Contains(body, SDKName) {
		t.Fatalf("%d %s", code, body)
	}
	code, _, body = serve(h, signed(t, testSecret, time.Now(), orchestrateBody(t, ok.URL, "missing", time.Now().Add(time.Minute))))
	if code != 200 || !strings.Contains(body, `{"type":"workflowNotFound","workflow":"missing"}`) {
		t.Fatalf("not found: %d %s", code, body)
	}
	conflict, _ := liveServer(t, func(int) (int, string) { return 409, `{"error":{"code":"invocation_superseded"}}` })
	if code, out, _ := serve(h, signed(t, testSecret, time.Now(), orchestrateBody(t, conflict.URL, "sequence", time.Now().Add(time.Minute)))); code != 409 || out["code"] != "invocation_superseded" {
		t.Fatalf("superseded: %d %v", code, out)
	}
	refused, n := liveServer(t, func(int) (int, string) { return 400, "" })
	if code, out, _ := serve(h, signed(t, testSecret, time.Now(), orchestrateBody(t, refused.URL, "sequence", time.Now().Add(time.Minute)))); code != 503 || out["code"] != "checkpoint_failed" || n.Load() != 1 {
		t.Fatalf("refused: %d %v (%d)", code, out, n.Load())
	}
	down, _ := liveServer(t, func(int) (int, string) {
		return 503, `{"error":{"code":"payload_unavailable","message":"down"}}`
	})
	code, out, _ := serve(h, signed(t, testSecret, time.Now(), orchestrateBody(t, down.URL, "sequence", time.Now().Add(7*time.Second))))
	if e, _ := out["error"].(map[string]any); code != 503 || out["code"] != "payload_unavailable" || e["code"] != "payload_unavailable" {
		t.Fatalf("payload down: %d %v", code, out)
	}
}

func TestHandlerOptionsChecked(t *testing.T) {
	wf := []Workflow{sequenceWF()}
	for name, o := range map[string]HandlerOptions{
		"no secrets":     {Workflows: wf},
		"bad secret":     {Workflows: wf, SigningSecrets: []string{"nope"}},
		"seconds":        {Workflows: wf, SigningSecrets: []string{testSecret}, MaxInvocationSeconds: 901},
		"duplicate name": {Workflows: []Workflow{sequenceWF(), sequenceWF()}, SigningSecrets: []string{testSecret}},
		"no workflows":   {SigningSecrets: []string{testSecret}},
	} {
		if _, err := Handler(o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	h, err := Handler(HandlerOptions{Workflows: wf, SigningSecrets: []string{""}})
	if err != nil {
		t.Fatal(err)
	}
	if code, out, _ := serve(h, httptest.NewRequest(http.MethodPost, "/", nil)); code != 500 || out["code"] != "sdk_error" {
		t.Fatalf("no usable secret: %d %v", code, out)
	}
}

func TestSDKNameMatchesTheProtocol(t *testing.T) {
	if SDKName != protocol.SDKName {
		t.Fatal(SDKName)
	}
}

// ctx.Run().Traceparent is the push request's traceparent header, exactly as received; "" without
// it. A body field isn't read on push: the engine sends the header.
func TestHandlerTraceparentFromHeader(t *testing.T) {
	ok, _ := liveServer(t, func(int) (int, string) { return 204, "" })
	traced := New("traced", func(ctx Context, _ none) (string, error) { return ctx.Run().Traceparent, nil })
	h := mustHandler(t, HandlerOptions{Workflows: []Workflow{traced}})
	const tp = "00-0199a3d02b3c7d4e9f506b7c8d9e0f1a-0123456789abcdef-01"
	output := func(header, field string) string {
		inv := invocation("traced", nil)
		inv.Endpoints.Live = ok.URL
		inv.Deadline = time.Now().Add(time.Minute)
		inv.Traceparent = field
		b, _ := json.Marshal(inv)
		req := signed(t, testSecret, time.Now(), b)
		if header != "" {
			req.Header.Set("traceparent", header)
		}
		code, out, body := serve(h, req)
		cmds, _ := out["commands"].([]any)
		if code != 200 || len(cmds) != 1 {
			t.Fatalf("%d %s", code, body)
		}
		return cmds[0].(map[string]any)["output"].(string)
	}
	if got := output(tp, ""); got != tp {
		t.Fatalf("with the header: %q", got)
	}
	if got := output("", ""); got != "" {
		t.Fatalf("without it: %q", got)
	}
	if got := output("", tp); got != "" {
		t.Fatalf("a push body's field: %q", got)
	}
}
