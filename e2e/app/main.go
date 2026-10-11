// The end-to-end app, under a supervisor (scripts/e2e.sh), driven by e2e/run through the real
// workflows-stack. E2E_ROLE=push (the default) serves the push handler on 127.0.0.1:$PORT with
// /__e2e/state and /__e2e/fix; E2E_ROLE=pull runs a pull worker for the pull app.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	workflows "github.com/ezgamehost/workflows-sdk-go"
)

type none = json.RawMessage

// State is what the app records for the driver (a file, so it survives the supervisor's
// restarts).
type State struct {
	Fixed       bool        `json:"fixed"`
	Prepare     int         `json:"prepare"`
	Cleanup     int         `json:"cleanup"`
	CallbackURL string      `json:"callbackUrl"`
	Scheduled   []Scheduled `json:"scheduled"`
}

// Scheduled is one run of e2e-scheduled and the schedule metadata it saw.
type Scheduled struct {
	RunID         string    `json:"runId"`
	ScheduleID    string    `json:"scheduleId"`
	ScheduledTime time.Time `json:"scheduledTime"`
}

var (
	stateFile = envOr("E2E_STATE", "/tmp/workflows-sdk-go-e2e-state.json")
	stateMu   sync.Mutex
)

func read() State {
	var s State
	if b, err := os.ReadFile(stateFile); err == nil {
		_ = json.Unmarshal(b, &s)
	}
	return s
}

func update(fn func(*State)) {
	stateMu.Lock()
	defer stateMu.Unlock()
	s := read()
	fn(&s)
	b, _ := json.Marshal(s)
	tmp := stateFile + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err == nil {
		_ = os.Rename(tmp, stateFile)
	}
}

func attemptOf(sc workflows.StepContext) int {
	k := sc.IdempotencyKey()
	n, _ := strconv.Atoi(k[strings.LastIndexByte(k, ':')+1:])
	return n
}

func step[T any](ctx workflows.Context, name string, v T) (T, error) {
	return workflows.Run(ctx, name, func(workflows.StepContext) (T, error) { return v, nil })
}

// retry is DefaultRetry with maxAttempts and a 200 ms first delay.
func retry(maxAttempts int) workflows.RetryPolicy {
	p := workflows.DefaultRetry
	p.MaxAttempts, p.Initial = maxAttempts, 200*time.Millisecond
	return p
}

func pushWorkflows() []workflows.Workflow {
	return []workflows.Workflow{
		workflows.New("e2e-complete", func(ctx workflows.Context, _ none) (map[string]int, error) {
			a, err := step(ctx, "one", 1)
			if err != nil {
				return nil, err
			}
			b, err := step(ctx, "two", 2)
			if err != nil {
				return nil, err
			}
			return map[string]int{"sum": a + b}, nil
		}),
		workflows.New("e2e-sleep", func(ctx workflows.Context, _ none) (int, error) {
			large, err := step(ctx, "large", strings.Repeat("y", 100_000))
			if err != nil {
				return 0, err
			}
			if err := workflows.Sleep(ctx, "nap", 2*time.Second); err != nil {
				return 0, err
			}
			return step(ctx, "after", len(large))
		}),
		workflows.New("e2e-retry", func(ctx workflows.Context, _ none) (int, error) {
			return workflows.Run(ctx, "flaky", func(sc workflows.StepContext) (int, error) {
				if n := attemptOf(sc); n < 3 {
					return 0, fmt.Errorf("attempt %d", n)
				}
				return attemptOf(sc), nil
			}, workflows.WithStepRetry(retry(5)))
		}),
		workflows.New("e2e-crash", func(ctx workflows.Context, _ none) (string, error) {
			return workflows.Run(ctx, "crashy", func(sc workflows.StepContext) (string, error) {
				if attemptOf(sc) == 1 {
					os.Exit(1)
				}
				return "survived", nil
			}, workflows.WithStepRetry(retry(3)))
		}),
		workflows.New("e2e-cancel", func(ctx workflows.Context, _ none) (string, error) {
			err := workflows.Sleep(ctx, "wait", time.Hour)
			if _, cerr := workflows.Run(ctx, "cleanup", func(workflows.StepContext) (string, error) {
				update(func(s *State) { s.Cleanup++ })
				return "cleaned", nil
			}); cerr != nil {
				return "", cerr
			}
			if err != nil {
				return "", err
			}
			return "finished", nil
		}),
		workflows.New("e2e-fixable", func(ctx workflows.Context, _ none) (string, error) {
			if _, err := workflows.Run(ctx, "prepare", func(workflows.StepContext) (bool, error) {
				update(func(s *State) { s.Prepare++ })
				return true, nil
			}); err != nil {
				return "", err
			}
			return workflows.Run(ctx, "work", func(workflows.StepContext) (string, error) {
				if !read().Fixed {
					return "", workflows.NonRetriable(errors.New("not fixed yet"))
				}
				return "fixed", nil
			})
		}),
		// Recorded JSON replays exactly: key order and NUL characters survive the journal (a sleep
		// forces the replay).
		workflows.New("e2e-exact", func(ctx workflows.Context, _ none) (json.RawMessage, error) {
			v, err := step(ctx, "make", json.RawMessage(`{"b":1,"a":"x\u0000y","c":{"z":[2,1],"y":null}}`))
			if err != nil {
				return nil, err
			}
			if err := workflows.Sleep(ctx, "nap", time.Second); err != nil {
				return nil, err
			}
			return step(ctx, "echo", v)
		}),
		// Phase 2.
		workflows.New("e2e-parallel", func(ctx workflows.Context, _ none) (int, error) {
			vals := make([]int, 3)
			branches := make([]func(workflows.Context) error, 3)
			for i := range branches {
				branches[i] = func(c workflows.Context) (err error) {
					vals[i], err = step(c, "p"+strconv.Itoa(i), i+1)
					return
				}
			}
			if err := workflows.Parallel(ctx, branches...); err != nil {
				return 0, err
			}
			return vals[0] + vals[1] + vals[2], nil
		}),
		workflows.New("e2e-callback", func(ctx workflows.Context, _ none) (string, error) {
			cb, err := workflows.CreateCallback(ctx, "approval", 10*time.Minute)
			if err != nil {
				return "", err
			}
			if _, err := workflows.Run(ctx, "publish", func(workflows.StepContext) (bool, error) {
				update(func(s *State) { s.CallbackURL = cb.URL })
				return true, nil
			}); err != nil {
				return "", err
			}
			return workflows.WaitCallback[string](ctx, cb)
		}),
		workflows.New("e2e-parent", func(ctx workflows.Context, _ none) (int, error) {
			v, err := workflows.Invoke[int](ctx, "child", "e2e-child", 20)
			return v + 1, err
		}),
		workflows.New("e2e-child", func(ctx workflows.Context, in int) (int, error) {
			return step(ctx, "double", in*2)
		}),
		workflows.New("e2e-scheduled", func(ctx workflows.Context, _ none) (string, error) {
			info := ctx.Run()
			if _, err := workflows.Run(ctx, "record", func(workflows.StepContext) (bool, error) {
				if s := info.Schedule; s != nil {
					update(func(st *State) {
						st.Scheduled = append(st.Scheduled, Scheduled{RunID: info.ID, ScheduleID: s.ID, ScheduledTime: s.ScheduledTime})
					})
				}
				return info.Schedule != nil, nil
			}); err != nil {
				return "", err
			}
			return "ran", nil
		}),
	}
}

func pullWorkflows() []workflows.Workflow {
	return []workflows.Workflow{
		workflows.New("e2e-pull", func(ctx workflows.Context, in struct {
			X int `json:"x"`
		}) (int, error) {
			a, err := step(ctx, "first", in.X+1)
			if err != nil {
				return 0, err
			}
			return step(ctx, "second", a*2)
		}),
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if os.Getenv("E2E_ROLE") == "pull" {
		w, err := workflows.NewWorker(workflows.WorkerOptions{
			APIKey: os.Getenv("E2E_API_KEY"), OrganizationID: os.Getenv("E2E_ORG"), ProjectID: os.Getenv("E2E_PROJECT"),
			App: os.Getenv("E2E_PULL_APP_ID"), Queue: "e2e", Workflows: pullWorkflows(),
			LiveURL: envOr("E2E_LIVE", "http://127.0.0.1:6150"), Deployment: "e2e-pull", MaxPollBackoff: 2 * time.Second,
			HeartbeatInterval: 5 * time.Second,
		})
		if err != nil {
			log.Fatal(err)
		}
		if err := w.Run(ctx); err != nil {
			log.Fatal(err)
		}
		return
	}
	h, err := workflows.Handler(workflows.HandlerOptions{Workflows: pushWorkflows(), SigningSecrets: []string{os.Getenv("EZGH_WORKFLOWS_SIGNING_SECRET")},
		MaxInvocationSeconds: 60, Deployment: "e2e"})
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/__e2e/fix", func(w http.ResponseWriter, _ *http.Request) {
		update(func(s *State) { s.Fixed = true })
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/__e2e/state", func(w http.ResponseWriter, _ *http.Request) {
		stateMu.Lock()
		s := read()
		stateMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s)
	})
	mux.Handle("/", h)
	srv := &http.Server{Addr: "127.0.0.1:" + envOr("PORT", "3200"), Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
