package live

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

var cp = protocol.Checkpoint{InvocationID: "inv_aaaqeayeaudaocajbifqydiob4", Seq: 1, Commands: []protocol.Command{}}

func server(t *testing.T, h func(w http.ResponseWriter, r *http.Request, n int)) (*httptest.Server, *atomic.Int32) {
	var n atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("authorization %q", r.Header.Get("Authorization"))
		}
		h(w, r, int(n.Add(1)))
	}))
	t.Cleanup(s.Close)
	return s, &n
}

func client(s *httptest.Server, d time.Duration) *Client {
	return &Client{Base: s.URL, Token: "tok", HTTP: s.Client(), Deadline: time.Now().Add(d), FirstDelay: 20 * time.Millisecond}
}

func TestConflictIsSuperseded(t *testing.T) {
	s, _ := server(t, func(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(409) })
	if err := client(s, time.Minute).Checkpoint(context.Background(), cp); !errors.Is(err, ErrSuperseded) {
		t.Fatal(err)
	}
}

func TestPayloadUnavailableUntilTheDeadline(t *testing.T) {
	s, n := server(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.WriteHeader(503)
		_, _ = w.Write([]byte(`{"error":{"code":"payload_unavailable","message":"down"}}`))
	})
	err := client(s, 500*time.Millisecond).Checkpoint(context.Background(), cp)
	var cf *CheckpointFailed
	if !errors.As(err, &cf) || cf.Code != "payload_unavailable" || n.Load() < 2 {
		t.Fatalf("%v after %d requests", err, n.Load())
	}
}

func TestPlain503IsCheckpointFailed(t *testing.T) {
	s, _ := server(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		w.WriteHeader(503)
		if n == 1 {
			_, _ = w.Write([]byte(`{"error":{"code":"payload_unavailable"}}`))
		}
	})
	err := client(s, 500*time.Millisecond).Checkpoint(context.Background(), cp)
	var cf *CheckpointFailed
	if !errors.As(err, &cf) || cf.Code != "checkpoint_failed" {
		t.Fatal(err)
	}
}

func TestClientErrorFailsAtOnce(t *testing.T) {
	s, n := server(t, func(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(400) })
	err := client(s, time.Minute).Checkpoint(context.Background(), cp)
	var cf *CheckpointFailed
	if !errors.As(err, &cf) || cf.Code != "checkpoint_failed" || n.Load() != 1 {
		t.Fatalf("%v after %d requests", err, n.Load())
	}
}

func TestRetryAfterZeroNeverHotLoops(t *testing.T) {
	s, n := server(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(429)
	})
	c := client(s, time.Second)
	c.FirstDelay = 200 * time.Millisecond
	_ = c.Checkpoint(context.Background(), cp)
	if got := n.Load(); got > 5 {
		t.Fatalf("%d requests in a second", got)
	}
}

func TestRetriedThenAccepted(t *testing.T) {
	s, n := server(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		if n < 3 {
			w.WriteHeader(502)
			return
		}
		w.WriteHeader(204)
	})
	if err := client(s, time.Minute).Checkpoint(context.Background(), cp); err != nil || n.Load() != 3 {
		t.Fatalf("%v after %d", err, n.Load())
	}
}

func TestInvalidCheckpointIsNeverSent(t *testing.T) {
	s, n := server(t, func(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(204) })
	bad := protocol.Checkpoint{InvocationID: cp.InvocationID, Seq: 1, Commands: []protocol.Command{{Type: "stepCompleted", Name: "x"}}}
	if err := client(s, time.Minute).Checkpoint(context.Background(), bad); err == nil || n.Load() != 0 {
		t.Fatalf("%v, %d requests", err, n.Load())
	}
}

func TestHeartbeatReadsCancel(t *testing.T) {
	s, _ := server(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		if r.URL.Path != "/v1/invocations/heartbeat" {
			t.Errorf("path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"cancel":{"requestedAt":"2026-10-10T00:00:00Z","graceUntil":"2026-10-10T00:05:00Z"}}`))
	})
	hb, err := client(s, time.Minute).Heartbeat(context.Background())
	if err != nil || hb.Cancel == nil {
		t.Fatalf("%+v %v", hb, err)
	}
}
