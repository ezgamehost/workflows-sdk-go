package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	instanceRefreshAhead = 5 * time.Minute
	metadataTokenTTL     = 6 * time.Hour
	metadataRetryAfter   = 5 * time.Second
)

// metadataEndpoint and productNamePath are where an EZGH compute instance's metadata service and
// SMBIOS product name are; tests point them elsewhere.
var metadataEndpoint = "http://169.254.169.254"

// productNamePath is where an EZGH compute instance's SMBIOS product name is; tests point it
// elsewhere.
var productNamePath = "/sys/class/dmi/id/product_name"

// ErrNoBot means the compute instance the code runs on has no Bot attached, so there are no
// instance credentials.
var ErrNoBot = errors.New("this instance has no Bot attached")

// credential is what a client authenticates with: its API key, or, when none was given and the
// code runs on an EZGH compute instance, the key of the instance's Bot from the metadata service.
type credential struct {
	static secret
	inst   *instanceProvider
}

// newCredential returns the API key if there is one, else the instance's credentials when this
// machine is an EZGH compute instance, else ok is false. No network is touched here.
func newCredential(apiKey string) (c credential, ok bool) {
	if apiKey != "" {
		return credential{static: secret(apiKey)}, true
	}
	if b, err := os.ReadFile(productNamePath); err != nil || strings.TrimSpace(string(b)) != "EZGH Compute" {
		return credential{}, false
	}
	return credential{inst: &instanceProvider{endpoint: metadataEndpoint, httpc: newMetadataClient(), now: time.Now}}, true
}

// newMetadataClient is the provider's own client, never the caller's: no proxy (the session
// token and the Bot's key must not cross one, and hostd refuses forwarded requests), a 1 s dial,
// no redirects.
func newMetadataClient() *http.Client {
	return &http.Client{
		Timeout:       10 * time.Second,
		Transport:     &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Second}).DialContext},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// key is the key for the next request.
func (c credential) key(ctx context.Context) (string, error) {
	if c.inst == nil {
		return c.static.reveal(), nil
	}
	return c.inst.key(ctx)
}

// refused is called with the key a request used when the API answered 401. It reports whether
// the request is worth one retry (only instance credentials change).
func (c credential) refused(key string) bool {
	if c.inst == nil {
		return false
	}
	c.inst.invalidate(key)
	return true
}

type instanceProvider struct {
	endpoint string
	httpc    *http.Client
	now      func() time.Time

	mu        sync.Mutex
	token     string
	tokenExp  time.Time
	apiKey    string
	expiresAt time.Time
	nextTry   time.Time
	lastErr   error
	retry     time.Duration // the metadata service's Retry-After (503, 429) on the last answer
}

func (p *instanceProvider) key(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if p.apiKey != "" && now.Add(instanceRefreshAhead).Before(p.expiresAt) {
		return p.apiKey, nil
	}
	var err error
	if !now.Before(p.nextTry) {
		if err = p.fetch(ctx); err == nil {
			return p.apiKey, nil
		}
		p.lastErr = err
		p.nextTry = now.Add(max(metadataRetryAfter, p.retry))
		if errors.Is(err, ErrNoBot) {
			p.apiKey = ""
		}
	}
	// A failed fetch keeps the old key until it actually expires.
	if p.apiKey != "" && now.Before(p.expiresAt) {
		return p.apiKey, nil
	}
	if err == nil {
		// Backing off: report the failure that started it, without asking again.
		if err = p.lastErr; err == nil {
			err = errors.New("instance credentials expired and the metadata service is not answering")
		}
	}
	return "", err
}

func (p *instanceProvider) invalidate(refused string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.apiKey == refused {
		p.apiKey, p.expiresAt, p.nextTry = "", time.Time{}, time.Time{}
	}
}

func (p *instanceProvider) fetch(ctx context.Context) error {
	for attempt := 0; ; attempt++ {
		if p.token == "" || !p.now().Before(p.tokenExp) {
			if err := p.newToken(ctx); err != nil {
				return err
			}
		}
		status, body, err := p.do(ctx, http.MethodGet, "/ezgh/v1/credentials", nil)
		if err != nil {
			return err
		}
		switch {
		case status == http.StatusOK:
			var c struct {
				APIKey    string    `json:"apiKey"`
				ExpiresAt time.Time `json:"expiresAt"`
			}
			if err := json.Unmarshal(body, &c); err != nil || c.APIKey == "" || c.ExpiresAt.IsZero() {
				return errors.New("instance metadata sent an unreadable answer")
			}
			p.apiKey, p.expiresAt = c.APIKey, c.ExpiresAt
			return nil
		case status == http.StatusUnauthorized && attempt == 0:
			p.token = "" // the token is gone (migration, metadata restart): get one new one
		case status == http.StatusNotFound && errorCode(body) == "no_bot":
			return ErrNoBot
		default:
			return fmt.Errorf("instance metadata answered %d", status)
		}
	}
}

func (p *instanceProvider) newToken(ctx context.Context) error {
	status, body, err := p.do(ctx, http.MethodPut, "/ezgh/v1/token", http.Header{"Ezgh-Metadata-Token-Ttl-Seconds": {strconv.Itoa(int(metadataTokenTTL.Seconds()))}})
	if err != nil {
		return err
	}
	if status != http.StatusOK || len(body) == 0 {
		return fmt.Errorf("instance metadata answered %d to the token request", status)
	}
	p.token = strings.TrimSpace(string(body))
	p.tokenExp = p.now().Add(metadataTokenTTL - instanceRefreshAhead)
	return nil
}

func (p *instanceProvider) do(ctx context.Context, method, path string, h http.Header) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, p.endpoint+path, nil)
	if err != nil {
		return 0, nil, err
	}
	for k, v := range h {
		req.Header[k] = v
	}
	req.Header.Set("EZGH-Metadata", "true")
	if method == http.MethodGet {
		req.Header.Set("EZGH-Metadata-Token", p.token)
	}
	res, err := p.httpc.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("instance metadata unreachable: %w", err)
	}
	defer res.Body.Close()
	p.retry = 0
	if n, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && n > 0 {
		p.retry = time.Duration(n) * time.Second
	}
	b, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	return res.StatusCode, b, nil
}

// errorCode is the platform error code in a JSON error body, or "".
func errorCode(body []byte) string {
	var e struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &e)
	return e.Error.Code
}
