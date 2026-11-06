package workflows

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const podKey = "ezgh_pppppppppppppppppppppppppppppppppppppppp"

// agentFake serves the pod identity agent and an API on one server.
type agentFake struct {
	mu         sync.Mutex
	srv        *httptest.Server
	gets       int
	tokens     []string // the bearer tokens the agent saw
	keys       []string // handed out in order, the last repeated
	status     int
	retryAfter string
	apiRefuses map[string]bool
	apiAuth    []string
	now        func() time.Time
}

func newAgentFake(t *testing.T) *agentFake {
	f := &agentFake{keys: []string{podKey}, apiRefuses: map[string]bool{}, now: time.Now}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.URL.Path != "/v1/credentials" {
			auth := r.Header.Get("Authorization")
			f.apiAuth = append(f.apiAuth, auth)
			if f.apiRefuses[auth] {
				w.WriteHeader(401)
				_, _ = w.Write([]byte(`{"error":{"code":"unauthorized"}}`))
				return
			}
			_, _ = w.Write([]byte(`{}`))
			return
		}
		f.gets++
		f.tokens = append(f.tokens, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		if f.status != 0 {
			if f.retryAfter != "" {
				w.Header().Set("Retry-After", f.retryAfter)
			}
			w.WriteHeader(f.status)
			return
		}
		k := f.keys[0]
		if len(f.keys) > 1 {
			f.keys = f.keys[1:]
		}
		_, _ = fmt.Fprintf(w, `{"apiKey":%q,"expiresAt":%q,"bot":"bot_pod"}`, k, f.now().Add(10*time.Minute).Format(time.RFC3339))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func writeToken(t *testing.T, token string) string {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// inPod sets the variables EZGH Kubernetes injects.
func inPod(t *testing.T, f *agentFake, tokenFile string) {
	t.Setenv(envContainerURI, f.srv.URL+"/v1/credentials")
	t.Setenv(envContainerTokenFile, tokenFile)
}

func TestContainerCredentialsWinOverTheInstance(t *testing.T) {
	inst, _ := newInstanceFake(t, true)
	f := newAgentFake(t)
	inPod(t, f, writeToken(t, "sa"))
	c, err := NewClient(ClientOptions{OrganizationID: "o", ProjectID: "p", BaseURL: f.srv.URL, HTTPClient: f.srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "run_1"); err != nil {
		t.Fatal(err)
	}
	if f.apiAuth[0] != "Bearer "+podKey || f.tokens[0] != "sa" || inst.metadataHit != 0 {
		t.Errorf("api %v, agent %v, metadata %d", f.apiAuth, f.tokens, inst.metadataHit)
	}
}

func TestExplicitAPIKeyBeatsTheContainer(t *testing.T) {
	f := newAgentFake(t)
	inPod(t, f, writeToken(t, "sa"))
	c, err := NewClient(ClientOptions{APIKey: "ezgh_explicit", OrganizationID: "o", ProjectID: "p", BaseURL: f.srv.URL, HTTPClient: f.srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "run_1"); err != nil || f.gets != 0 || f.apiAuth[0] != "Bearer ezgh_explicit" {
		t.Errorf("%v %d %v", err, f.gets, f.apiAuth)
	}
}

func TestContainerRefreshesBeforeExpiry(t *testing.T) {
	f := newAgentFake(t)
	f.keys = []string{instKey1, instKey2}
	now := time.Now()
	f.now = func() time.Time { return now }
	start := now
	p := &containerProvider{uri: f.srv.URL + "/v1/credentials", tokenFile: writeToken(t, "sa"), httpc: f.srv.Client(), now: func() time.Time { return now }}
	ctx := context.Background()
	if k, _ := p.key(ctx); k != instKey1 {
		t.Fatal(k)
	}
	now = start.Add(4*time.Minute + 59*time.Second) // instKey1 expires at +10 min
	if k, _ := p.key(ctx); k != instKey1 || f.gets != 1 {
		t.Fatalf("%s after %d gets", k, f.gets)
	}
	now = start.Add(5 * time.Minute)
	if k, _ := p.key(ctx); k != instKey2 || f.gets != 2 {
		t.Errorf("%s after %d gets", k, f.gets)
	}
}

func TestContainerFailedFetchKeepsTheKey(t *testing.T) {
	f := newAgentFake(t)
	now := time.Now()
	f.now = func() time.Time { return now }
	start := now
	p := &containerProvider{uri: f.srv.URL + "/v1/credentials", tokenFile: writeToken(t, "sa"), httpc: f.srv.Client(), now: func() time.Time { return now }}
	ctx := context.Background()
	if k, _ := p.key(ctx); k != podKey {
		t.Fatal(k)
	}
	f.status, f.retryAfter = 503, "30"
	now = start.Add(6 * time.Minute)
	if k, err := p.key(ctx); k != podKey || err != nil || f.gets != 2 {
		t.Fatalf("%s %v after %d gets", k, err, f.gets)
	}
	now = start.Add(6*time.Minute + 20*time.Second) // Retry-After 30 s: not asked again yet
	if k, _ := p.key(ctx); k != podKey || f.gets != 2 {
		t.Fatalf("%s after %d gets", k, f.gets)
	}
	now = start.Add(10 * time.Minute)
	if k, err := p.key(ctx); k != "" || err == nil || !strings.Contains(err.Error(), "503") {
		t.Errorf("%q %v", k, err)
	}
}

func TestContainerRefetchesOnceAfter401(t *testing.T) {
	f := newAgentFake(t)
	f.keys = []string{instKey1, instKey2}
	inPod(t, f, writeToken(t, "sa"))
	c, err := NewClient(ClientOptions{OrganizationID: "o", ProjectID: "p", BaseURL: f.srv.URL, HTTPClient: f.srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	f.apiRefuses["Bearer "+instKey1] = true
	ctx := context.Background()
	if _, err := c.Get(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.apiAuth, ",") != "Bearer "+instKey1+",Bearer "+instKey2 || f.gets != 2 {
		t.Errorf("api %v, %d gets", f.apiAuth, f.gets)
	}
	// A second 401 is returned, not looped on.
	f.apiRefuses["Bearer "+instKey2] = true
	n := len(f.apiAuth)
	var ae *APIError
	if _, err := c.Get(ctx, "run_1"); !errors.As(err, &ae) || ae.Status != 401 || len(f.apiAuth)-n != 2 {
		t.Errorf("%v, %d requests", err, len(f.apiAuth)-n)
	}
}

func TestContainerReadsARotatedToken(t *testing.T) {
	f := newAgentFake(t)
	file := writeToken(t, "first")
	p := &containerProvider{uri: f.srv.URL + "/v1/credentials", tokenFile: file, httpc: f.srv.Client(), now: time.Now}
	ctx := context.Background()
	k, _ := p.key(ctx)
	_ = os.WriteFile(file, []byte("second"), 0o600)
	p.invalidate(k)
	_, _ = p.key(ctx)
	if strings.Join(f.tokens, ",") != "first,second" {
		t.Errorf("agent saw %q", f.tokens)
	}
}

func TestContainerRefusesOtherURIs(t *testing.T) {
	f := newAgentFake(t)
	port := f.srv.URL[strings.LastIndex(f.srv.URL, ":"):]
	for _, uri := range []string{
		"http://localhost" + port + "/v1/credentials", // a name, not an address
		"https://127.0.0.1" + port + "/v1/credentials",
		"http://169.254.169.254/v1/credentials",
		"http://10.0.0.23/v1/credentials",
		"http://user@127.0.0.1" + port + "/v1/credentials",
	} {
		p := &containerProvider{uri: uri, tokenFile: writeToken(t, "sa"), httpc: f.srv.Client(), now: time.Now}
		if _, err := p.key(context.Background()); err == nil || !strings.Contains(err.Error(), envContainerURI) {
			t.Errorf("%s: %v", uri, err)
		}
	}
	if f.gets != 0 {
		t.Errorf("agent asked %d times", f.gets)
	}
	for _, uri := range []string{"http://169.254.170.23/v1/credentials", "http://[fd65:7a67:6800::23]/v1/credentials", "http://[::1]:80/x"} {
		if err := checkContainerURI(uri); err != nil {
			t.Errorf("%s: %v", uri, err)
		}
	}
}

func TestContainerMissingTokenIsAnError(t *testing.T) {
	inst, _ := newInstanceFake(t, true)
	f := newAgentFake(t)
	inPod(t, f, filepath.Join(t.TempDir(), "missing"))
	c, err := NewClient(ClientOptions{OrganizationID: "o", ProjectID: "p", BaseURL: f.srv.URL, HTTPClient: f.srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "run_1"); err == nil || !strings.Contains(err.Error(), envContainerTokenFile) {
		t.Errorf("%v", err)
	}
	if inst.metadataHit != 0 || f.gets != 0 || len(f.apiAuth) != 0 {
		t.Errorf("metadata %d, agent %d, api %v", inst.metadataHit, f.gets, f.apiAuth)
	}
}

// A Retry-After from an earlier answer doesn't stretch the backoff after a connection failure.
func TestContainerRetryAfterResets(t *testing.T) {
	f := newAgentFake(t)
	f.status, f.retryAfter = 503, "60"
	now := time.Now()
	p := &containerProvider{uri: f.srv.URL + "/v1/credentials", tokenFile: writeToken(t, "sa"), httpc: f.srv.Client(), now: func() time.Time { return now }}
	ctx := context.Background()
	if _, err := p.key(ctx); err == nil {
		t.Fatal("no error")
	}
	now = now.Add(time.Minute)
	f.srv.Close() // the agent is gone: unreachable, no Retry-After
	_, _ = p.key(ctx)
	if p.nextTry != now.Add(metadataRetryAfter) {
		t.Errorf("next try in %s", p.nextTry.Sub(now))
	}
}

// A fetch that outlives the key: no expired key is returned, and the backoff runs from its end.
func TestContainerSlowFailedFetchRereadsTheClock(t *testing.T) {
	f := newAgentFake(t)
	start := time.Now()
	f.now = func() time.Time { return start }
	p := &containerProvider{uri: f.srv.URL + "/v1/credentials", tokenFile: writeToken(t, "sa"), httpc: f.srv.Client(), now: func() time.Time { return start }}
	ctx := context.Background()
	if k, _ := p.key(ctx); k != podKey {
		t.Fatal(k)
	}
	f.status = 503
	calls := 0
	p.now = func() time.Time { // 6 min in when the fetch starts (refresh due), 11 min when it ends
		if calls++; calls == 1 {
			return start.Add(6 * time.Minute)
		}
		return start.Add(11 * time.Minute)
	}
	if k, err := p.key(ctx); k != "" || err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("%q %v", k, err)
	}
	gets := f.gets
	if _, err := p.key(ctx); err == nil || f.gets != gets { // within 5 s of the fetch's end
		t.Errorf("%v, asked again: %v", err, f.gets != gets)
	}
}
