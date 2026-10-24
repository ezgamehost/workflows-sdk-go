package workflows

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

const (
	instKey1 = "ezgh_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	instKey2 = "ezgh_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// instanceFake serves the metadata service and an API on one server; instanceClient sends every
// request there.
type instanceFake struct {
	mu          sync.Mutex
	srv         *httptest.Server
	puts, gets  int
	token       string
	keys        []string // handed out in order, the last repeated
	ttl         time.Duration
	status      int // credentials answer when not 0
	apiRefuses  map[string]bool
	apiAuth     []string
	metadataHit int
	now         func() time.Time
}

type rewrite struct{ to *url.URL }

func (r rewrite) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host = r.to.Scheme, r.to.Host
	return http.DefaultTransport.RoundTrip(req)
}

func newInstanceFake(t *testing.T, onInstance bool) (*instanceFake, *http.Client) {
	f := &instanceFake{keys: []string{instKey1}, ttl: 10 * time.Minute, apiRefuses: map[string]bool{}, now: time.Now}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.URL.Path {
		case "/ezgh/v1/token":
			f.metadataHit++
			f.puts++
			f.token = fmt.Sprintf("tok%d", f.puts)
			_, _ = w.Write([]byte(f.token))
		case "/ezgh/v1/credentials":
			f.metadataHit++
			f.gets++
			switch {
			case r.Header.Get("EZGH-Metadata-Token") != f.token:
				w.WriteHeader(401)
				_, _ = w.Write([]byte(`{"error":{"code":"invalid_token"}}`))
			case f.status != 0:
				w.WriteHeader(f.status)
			default:
				k := f.keys[0]
				if len(f.keys) > 1 {
					f.keys = f.keys[1:]
				}
				_, _ = fmt.Fprintf(w, `{"apiKey":%q,"expiresAt":%q,"bot":"bot_abc"}`, k, f.now().Add(f.ttl).Format(time.RFC3339))
			}
		default:
			auth := r.Header.Get("Authorization")
			f.apiAuth = append(f.apiAuth, auth)
			if f.apiRefuses[auth] {
				w.WriteHeader(401)
				_, _ = w.Write([]byte(`{"error":{"code":"unauthorized"}}`))
				return
			}
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(f.srv.Close)
	u, _ := url.Parse(f.srv.URL)
	old := productNamePath
	productNamePath = filepath.Join(t.TempDir(), "product_name")
	if onInstance {
		_ = os.WriteFile(productNamePath, []byte("EZGH Compute\n"), 0o600)
	}
	t.Cleanup(func() { productNamePath = old })
	return f, &http.Client{Transport: rewrite{u}}
}

func TestInstanceCredentialsRecover(t *testing.T) {
	f, hc := newInstanceFake(t, true)
	f.keys = []string{instKey1, instKey2}
	c, err := NewClient(ClientOptions{OrganizationID: "o", ProjectID: "p", BaseURL: "http://api.test", HTTPClient: hc})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.Get(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	// The API refuses key1 (a long sleep, a Bot swap): one refetch and one retry.
	f.apiRefuses["Bearer "+instKey1] = true
	f.token = "forgotten" // and the metadata token is gone too: one new PUT
	if _, err := c.Get(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	if got := f.apiAuth[len(f.apiAuth)-1]; got != "Bearer "+instKey2 || f.puts != 2 {
		t.Errorf("auth %q puts %d", got, f.puts)
	}
	// A second 401 is returned, not looped on.
	f.apiRefuses["Bearer "+instKey2] = true
	n := len(f.apiAuth)
	var ae *APIError
	if _, err := c.Get(ctx, "run_1"); !errors.As(err, &ae) || ae.Status != 401 || len(f.apiAuth)-n != 2 {
		t.Errorf("%v, %d requests", err, len(f.apiAuth)-n)
	}
}

func TestInstanceProviderRefreshWindow(t *testing.T) {
	f, hc := newInstanceFake(t, true)
	f.keys = []string{instKey1, instKey2}
	now := time.Now()
	f.now = func() time.Time { return now }
	p := &instanceProvider{endpoint: "http://m", httpc: hc, now: func() time.Time { return now }}
	ctx := context.Background()
	get := func() (string, error) { return p.key(ctx) }
	if k, _ := get(); k != instKey1 {
		t.Fatal(k)
	}
	now = now.Add(4 * time.Minute)
	if k, _ := get(); k != instKey1 || f.gets != 1 {
		t.Errorf("refetched too early: %s %d", k, f.gets)
	}
	now = now.Add(2 * time.Minute) // 6 min in: within 5 min of the 10 min expiry
	if k, _ := get(); k != instKey2 {
		t.Errorf("didn't refetch: %s", k)
	}
	// Metadata down: the old key is kept until it really expires.
	f.status = 503
	now = now.Add(6 * time.Minute) // key2 expires at about +16 min; now is +12
	if k, err := get(); err != nil || k != instKey2 {
		t.Errorf("%s %v", k, err)
	}
	now = now.Add(5 * time.Minute)
	if _, err := get(); err == nil {
		t.Error("served an expired key")
	}
	f.status = 404
	now = now.Add(time.Minute)
	if _, err := get(); !errors.Is(err, ErrNoBot) {
		t.Errorf("%v", err)
	}
}

func TestExplicitAPIKeyBeatsTheInstance(t *testing.T) {
	f, hc := newInstanceFake(t, true)
	c, err := NewClient(ClientOptions{APIKey: "ezgh_explicit", OrganizationID: "o", ProjectID: "p", BaseURL: "http://api.test", HTTPClient: hc})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "run_1"); err != nil || f.metadataHit != 0 || f.apiAuth[0] != "Bearer ezgh_explicit" {
		t.Errorf("%v %d %v", err, f.metadataHit, f.apiAuth)
	}
}

func TestNoAPIKeyOffInstance(t *testing.T) {
	f, hc := newInstanceFake(t, false)
	if _, err := NewClient(ClientOptions{OrganizationID: "o", ProjectID: "p", HTTPClient: hc}); err == nil {
		t.Error("accepted no key")
	}
	if _, err := NewWorker(WorkerOptions{OrganizationID: "o", ProjectID: "p", App: "app_x", HTTPClient: hc}); err == nil {
		t.Error("worker accepted no key")
	}
	if f.metadataHit != 0 {
		t.Error("probed the network off an instance")
	}
}
