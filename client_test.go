package workflows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recorded struct {
	method, path, auth, key, ifMatch string
	body                             string
}

func apiServer(t *testing.T, answer func(n int, r *http.Request, w http.ResponseWriter)) (*Client, *[]recorded) {
	var mu sync.Mutex
	var reqs []recorded
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		reqs = append(reqs, recorded{r.Method, r.URL.RequestURI(), r.Header.Get("Authorization"), r.Header.Get("Idempotency-Key"), r.Header.Get("If-Match"), string(b)})
		n := len(reqs)
		mu.Unlock()
		answer(n, r, w)
	}))
	t.Cleanup(s.Close)
	c, err := NewClient(ClientOptions{APIKey: "ezgh_SECRETSECRET", OrganizationID: "org", ProjectID: "proj", BaseURL: s.URL, HTTPClient: s.Client(), MaxRetryDelay: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return c, &reqs
}

func TestClientSendsTheKeyAndResendsWithTheSameIdempotencyKey(t *testing.T) {
	c, reqs := apiServer(t, func(n int, _ *http.Request, w http.ResponseWriter) {
		if n == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"id":"r1","status":"running","steps":[]}`))
	})
	r, err := c.Start(context.Background(), "app", "wf", map[string]int{"a": 1}, StartOptions{RunKey: "k"})
	if err != nil || r.ID != "r1" {
		t.Fatalf("%+v %v", r, err)
	}
	rs := *reqs
	if len(rs) != 2 || rs[0].auth != "Bearer ezgh_SECRETSECRET" || rs[0].key == "" || rs[0].key != rs[1].key || rs[0].path != "/v1/organizations/org/projects/proj/runs" {
		t.Fatalf("%+v", rs)
	}
	if rs[0].body != `{"appId":"app","input":{"a":1},"runKey":"k","workflow":"wf"}` {
		t.Fatalf("body %s", rs[0].body)
	}
}

func TestClientErrors(t *testing.T) {
	c, reqs := apiServer(t, func(_ int, _ *http.Request, w http.ResponseWriter) {
		w.WriteHeader(409)
		_, _ = w.Write([]byte(`{"error":{"code":"run_key_conflict","message":"different input"}}`))
	})
	_, err := c.Start(context.Background(), "app", "wf", nil, StartOptions{})
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != 409 || ae.Code != "run_key_conflict" || ae.Message != "different input" || len(*reqs) != 1 {
		t.Fatalf("%v", err)
	}
	c, reqs = apiServer(t, func(_ int, _ *http.Request, w http.ResponseWriter) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(429)
	})
	start := time.Now()
	_, err = c.Get(context.Background(), "r")
	if !errors.As(err, &ae) || ae.Status != 429 || ae.RetryAfter != time.Minute || len(*reqs) != 4 || time.Since(start) > 2*time.Second {
		t.Fatalf("%v after %d requests in %v", err, len(*reqs), time.Since(start))
	}
}

func TestCreateAppIsNeverResentOnANetworkError(t *testing.T) {
	var hits atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		conn.Close()
	}))
	defer s.Close()
	c, _ := NewClient(ClientOptions{APIKey: "k", OrganizationID: "o", ProjectID: "p", BaseURL: s.URL})
	for _, call := range []func() error{
		func() error { _, err := c.CreateApp(context.Background(), "a", CreateAppOptions{}); return err },
		func() error { _, err := c.RotateSecret(context.Background(), "a"); return err },
	} {
		hits.Store(0)
		var ae *APIError
		if err := call(); !errors.As(err, &ae) || ae.Status != 0 || ae.Code != "outcome_unknown" || hits.Load() != 1 {
			t.Fatalf("%v after %d", err, hits.Load())
		}
	}
}

func TestClientRoutes(t *testing.T) {
	c, reqs := apiServer(t, func(_ int, r *http.Request, w http.ResponseWriter) {
		switch {
		case r.Method == http.MethodDelete:
			w.WriteHeader(204)
		case strings.HasSuffix(r.URL.Path, "/output"):
			_, _ = w.Write([]byte(`{"b":1,"a":2}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	})
	ctx := context.Background()
	two := 2
	_, _ = c.CreateApp(ctx, "puller", CreateAppOptions{Kind: AppKindPull, Queue: "e2e", Partitions: 2})
	_, _ = c.Sync(ctx, "a1", SyncOptions{Deployment: "d"})
	_, _ = c.Cancel(ctx, "r1", CancelOptions{GracePeriodSeconds: &two})
	var raw json.RawMessage
	_ = c.Output(ctx, "r1", &raw)
	_, _ = c.CompleteCallback(ctx, "r1", "s1", "yes")
	_, _ = c.CreateSchedule(ctx, ScheduleSpec{AppID: "a1", Workflow: "w", Name: "n", Cron: "* * * * *", IdempotencyKey: "ck"})
	cron := "0 * * * *"
	_, _ = c.UpdateSchedule(ctx, "s1", 3, ScheduleUpdate{Cron: &cron})
	_, _ = c.TriggerSchedule(ctx, "s1", "t-1")
	_, _ = c.ListSchedules(ctx, ListOptions{Limit: 2, Cursor: "abc"})
	_ = c.DeleteSchedule(ctx, "s1")
	_, _ = c.Events(ctx, "r1", EventsOptions{Limit: 5})
	if string(raw) != `{"b":1,"a":2}` {
		t.Fatalf("raw output %s", raw)
	}
	want := []string{
		`POST /apps {"kind":"pull","name":"puller","partitions":2,"queue":"e2e"}`,
		`POST /apps/a1/sync {"deployment":"d"}`,
		`POST /runs/r1/cancel {"gracePeriodSeconds":2}`,
		`GET /runs/r1/payloads/output `,
		`POST /runs/r1/callbacks/s1 "yes"`,
		`POST /schedules {"appId":"a1","workflow":"w","name":"n","cron":"* * * * *"}`,
		`PATCH /schedules/s1 {"cron":"0 * * * *"}`,
		`POST /schedules/s1/trigger {}`,
		`GET /schedules?cursor=abc&limit=2 `,
		`DELETE /schedules/s1 `,
		`GET /runs/r1/events?limit=5 `,
	}
	rs := *reqs
	for i, w := range want {
		got := rs[i].method + " " + strings.TrimPrefix(rs[i].path, "/v1/organizations/org/projects/proj") + " " + rs[i].body
		if got != w {
			t.Errorf("request %d:\n got %s\nwant %s", i, got, w)
		}
	}
	if rs[5].key != "ck" || rs[6].ifMatch != `"3"` || rs[7].key != "t-1" {
		t.Fatalf("headers %+v", rs)
	}
}

func TestCompleteCallbackURL(t *testing.T) {
	var auth string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		if string(b) == `"rejected"` {
			w.WriteHeader(409)
			_, _ = w.Write([]byte(`{"error":{"code":"callback_completed","message":"x"}}`))
			return
		}
		w.WriteHeader(200)
	}))
	defer s.Close()
	if err := CompleteCallbackURL(context.Background(), s.Client(), s.URL+"/public/v1/callbacks/tok", "approved"); err != nil || auth != "" {
		t.Fatalf("%v %q", err, auth)
	}
	var ae *APIError
	if err := CompleteCallbackURL(context.Background(), s.Client(), s.URL+"/public/v1/callbacks/tok", "rejected"); !errors.As(err, &ae) || ae.Status != 409 || ae.Code != "callback_completed" {
		t.Fatalf("%v", err)
	}
}

func TestAPIKeyNeverPrints(t *testing.T) {
	c, _ := NewClient(ClientOptions{APIKey: "ezgh_SECRETSECRET", OrganizationID: "o", ProjectID: "p"})
	for _, s := range []string{fmt.Sprint(c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), fmt.Sprintf("%v", *c), fmt.Sprintf("%s", c), fmt.Sprintf("%#v", *c), fmt.Sprintf("%v", c.key)} {
		if strings.Contains(s, "SECRETSECRET") {
			t.Fatalf("printed: %s", s)
		}
	}
	b, _ := json.Marshal(c)
	if strings.Contains(string(b), "SECRETSECRET") {
		t.Fatal("marshalled")
	}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("x", "client", c, "deref", *c)
	slog.New(slog.NewTextHandler(&buf, nil)).Info("x", "client", c)
	if strings.Contains(buf.String(), "SECRETSECRET") {
		t.Fatal("logged")
	}
}
