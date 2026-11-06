package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The variables EZGH Kubernetes injects into a pod whose service account has a pod identity
// association.
const (
	envContainerURI       = "EZGH_CONTAINER_CREDENTIALS_FULL_URI"
	envContainerTokenFile = "EZGH_CONTAINER_AUTHORIZATION_TOKEN_FILE"
)

var (
	containerAgentIPv4 = net.ParseIP("169.254.170.23")
	containerAgentIPv6 = net.ParseIP("fd65:7a67:6800::23")
)

// containerProvider fetches the temporary key of the Bot a pod's service account is associated
// with, from the pod identity agent, presenting the pod's projected service account token. The
// key is used until 5 minutes before it expires.
type containerProvider struct {
	uri       string
	tokenFile string
	httpc     *http.Client
	now       func() time.Time

	mu        sync.Mutex
	apiKey    string
	expiresAt time.Time
	nextTry   time.Time
	lastErr   error
	retry     time.Duration // the agent's Retry-After on the last answer
}

func (p *containerProvider) key(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if p.apiKey != "" && now.Add(instanceRefreshAhead).Before(p.expiresAt) {
		return p.apiKey, nil
	}
	var err error
	if !now.Before(p.nextTry) {
		if err = ctx.Err(); err != nil {
			return "", err // the caller gave up: not a failed fetch, so no backoff for anyone else
		}
		// Not the caller's context: under the lock the fetch serves every waiting caller, so one
		// caller's cancellation mustn't fail it. The client's 10 s timeout bounds it.
		if err = p.fetch(context.WithoutCancel(ctx)); err == nil {
			return p.apiKey, nil
		}
		now = p.now() // the fetch may have taken seconds
		p.lastErr = err
		p.nextTry = now.Add(max(metadataRetryAfter, p.retry))
	}
	// A failed fetch keeps the old key until it actually expires.
	if p.apiKey != "" && now.Before(p.expiresAt) {
		return p.apiKey, nil
	}
	if err == nil {
		if err = p.lastErr; err == nil {
			err = errors.New("container credentials expired and the pod identity agent is not answering")
		}
	}
	return "", err
}

func (p *containerProvider) invalidate(refused string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.apiKey == refused {
		p.apiKey, p.expiresAt, p.nextTry = "", time.Time{}, time.Time{}
	}
}

// checkContainerURI allows only the pod identity agent's addresses, or loopback, over plain
// http, so the service account token never leaves the node.
func checkContainerURI(raw string) error {
	u, err := url.Parse(raw)
	if err == nil && u.Scheme == "http" && u.User == nil {
		if ip := net.ParseIP(u.Hostname()); ip != nil && (ip.IsLoopback() || ip.Equal(containerAgentIPv4) || ip.Equal(containerAgentIPv6)) {
			return nil
		}
	}
	return fmt.Errorf("%s must be http://169.254.170.23, http://[fd65:7a67:6800::23] or a loopback address", envContainerURI)
}

func (p *containerProvider) fetch(ctx context.Context) error {
	p.retry = 0 // a past Retry-After mustn't stretch the backoff after a refusal or a dead agent
	if err := checkContainerURI(p.uri); err != nil {
		return err
	}
	if p.tokenFile == "" {
		return fmt.Errorf("%s is set but %s is not", envContainerURI, envContainerTokenFile)
	}
	// Read afresh every time: kubelet rotates the projected token.
	b, err := os.ReadFile(p.tokenFile)
	if err != nil {
		return fmt.Errorf("reading the pod's service account token (%s): %w", envContainerTokenFile, err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return fmt.Errorf("the pod's service account token (%s) is empty", envContainerTokenFile)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.uri, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := p.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("pod identity agent unreachable: %w", err)
	}
	defer res.Body.Close()
	if n, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && n > 0 {
		p.retry = time.Duration(min(n, maxRetryAfter)) * time.Second
	}
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	if res.StatusCode != http.StatusOK {
		if code := errorCode(body); code != "" {
			return fmt.Errorf("pod identity agent answered %d (%s)", res.StatusCode, code)
		}
		return fmt.Errorf("pod identity agent answered %d", res.StatusCode)
	}
	var c struct {
		APIKey    string    `json:"apiKey"`
		ExpiresAt time.Time `json:"expiresAt"`
	}
	if err := json.Unmarshal(body, &c); err != nil || c.APIKey == "" || c.ExpiresAt.IsZero() {
		return errors.New("pod identity agent sent an unreadable answer")
	}
	p.apiKey, p.expiresAt = c.APIKey, c.ExpiresAt
	return nil
}
