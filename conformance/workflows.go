package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	workflows "github.com/ezgamehost/workflows-sdk-go"
)

type none = json.RawMessage

func step[T any](ctx workflows.Context, name string, v T, opts ...workflows.StepOption) (T, error) {
	return workflows.Run(ctx, name, func(workflows.StepContext) (T, error) { return v, nil }, opts...)
}

// all is protocol/conformance/workflows.md in Go, in its order (the order of the sync reply).
func all() []workflows.Workflow {
	return []workflows.Workflow{
		workflows.New("sequence", func(ctx workflows.Context, in struct {
			N int `json:"n"`
		}) (int, error) {
			sum := 0
			for i := 0; i < in.N; i++ {
				v, err := step(ctx, "add", i)
				if err != nil {
					return 0, err
				}
				sum += v
			}
			return sum, nil
		}),
		workflows.New("echo", func(ctx workflows.Context, in json.RawMessage) (map[string]any, error) {
			same, err := step(ctx, "echo", in)
			if err != nil {
				return nil, err
			}
			// Date(0) as JavaScript encodes it: the harness compares exact JSON.
			when, err := step(ctx, "date", time.Unix(0, 0).UTC().Format("2006-01-02T15:04:05.000Z07:00"))
			if err != nil {
				return nil, err
			}
			nothing, err := step[any](ctx, "nothing", nil)
			if err != nil {
				return nil, err
			}
			return map[string]any{"same": same, "when": when, "nothing": nothing}, nil
		}),
		workflows.New("race", func(ctx workflows.Context, _ none) (string, error) {
			var a, b string
			w, err := workflows.Race(ctx,
				func(c workflows.Context) (err error) { a, err = step(c, "a", "a"); return },
				func(c workflows.Context) (err error) { b, err = step(c, "b", "b"); return },
			)
			if err != nil {
				return "", err
			}
			win := []string{a, b}[w]
			return step(ctx, "after-"+win, win)
		}),
		workflows.New("flaky", func(ctx workflows.Context, _ none) (none, error) {
			_, err := workflows.Run(ctx, "flaky", func(workflows.StepContext) (int, error) {
				return 0, &workflows.Error{Code: "user", Name: "Error", Message: "boom", Data: json.RawMessage(`{"n":1}`)}
			}, workflows.WithStepRetry(workflows.RetryPolicy{MaxAttempts: 3, InitialMs: 10}))
			return nil, err
		}),
		workflows.New("fatal", func(ctx workflows.Context, _ none) (none, error) {
			_, err := workflows.Run(ctx, "fatal", func(workflows.StepContext) (int, error) { return 0, workflows.NonRetriable(errors.New("nope")) })
			return nil, err
		}),
		workflows.New("slow", func(ctx workflows.Context, _ none) (none, error) {
			_, err := workflows.Run(ctx, "slow", func(workflows.StepContext) (int, error) {
				return 0, workflows.RetryAfter(errors.New("slow down"), 1500*time.Millisecond)
			})
			return nil, err
		}),
		workflows.New("sleepy", func(ctx workflows.Context, _ none) (string, error) {
			if err := workflows.Sleep(ctx, "nap", 2*time.Second); err != nil {
				return "", err
			}
			return step(ctx, "after", "woke")
		}),
		workflows.New("values", func(ctx workflows.Context, _ none) (map[string]any, error) {
			t, err := workflows.Now(ctx, "t")
			if err != nil {
				return nil, err
			}
			id, err := workflows.UUID(ctx, "id")
			if err != nil {
				return nil, err
			}
			return step(ctx, "use", map[string]any{"year": t.Year() >= 2026, "idLength": len(id)})
		}),
		workflows.New("dupe", func(ctx workflows.Context, _ none) (none, error) {
			return nil, workflows.Parallel(ctx,
				func(c workflows.Context) error { _, err := step(c, "x", 1); return err },
				func(c workflows.Context) error { _, err := step(c, "x", 2); return err },
			)
		}),
		workflows.New("big", func(ctx workflows.Context, _ none) (string, error) {
			return step(ctx, "big", strings.Repeat("x", 1_048_577))
		}),
		workflows.New("cancellable", func(ctx workflows.Context, _ none) (string, error) {
			err := workflows.Sleep(ctx, "wait", time.Hour)
			if _, cerr := step(ctx, "cleanup", "cleaned"); cerr != nil {
				return "", cerr
			}
			if err != nil {
				return "", err
			}
			return "finished", nil
		}),
		workflows.New("caught", func(ctx workflows.Context, _ none) (map[string]any, error) {
			if _, err := step(ctx, "bad", 1); err != nil {
				var we *workflows.Error
				out := map[string]any{"isWorkflowError": errors.As(err, &we)}
				if we != nil {
					out["code"], out["name"], out["data"] = we.Code, we.Name, we.Data
					var cause *workflows.Error
					if errors.As(we.Cause, &cause) {
						out["causeName"], out["causeMessage"] = cause.Name, cause.Message
					}
				}
				return out, nil
			}
			return map[string]any{"unreachable": true}, nil
		}),
		workflows.New("explicitid", func(ctx workflows.Context, _ none) (int, error) {
			return step(ctx, "x", 1, workflows.WithID("custom-1"))
		}),
		workflows.New("tuned", func(ctx workflows.Context, _ none) (int, error) {
			return step(ctx, "t", 1)
		}, workflows.WithRetry(workflows.RetryPolicy{MaxAttempts: 2})),
		workflows.New("effect", func(ctx workflows.Context, in struct {
			URL string `json:"url"`
		}) (string, error) {
			return workflows.Run(ctx, "effect", func(sc workflows.StepContext) (string, error) {
				req, err := http.NewRequestWithContext(sc, http.MethodPost, in.URL, nil)
				if err == nil {
					if res, err := http.DefaultClient.Do(req); err == nil {
						res.Body.Close()
					}
				}
				return "done", nil
			})
		}),
		workflows.New("coded", func(ctx workflows.Context, _ none) (none, error) {
			_, err := workflows.Run(ctx, "coded", func(workflows.StepContext) (int, error) {
				return 0, &workflows.Error{Code: "http.418", Message: "teapot", Data: json.RawMessage(`{"status":418}`)}
			})
			return nil, err
		}),
		workflows.New("widestack", func(ctx workflows.Context, _ none) (none, error) {
			_, err := workflows.Run(ctx, "wide", func(workflows.StepContext) (int, error) {
				return 0, &workflows.Error{Code: "user", Name: "Error", Message: "wide", Stack: strings.Repeat("é", 5000)}
			})
			return nil, err
		}),
		workflows.New("widename", func(ctx workflows.Context, _ none) (int, error) {
			return step(ctx, strings.Repeat("é", 200), 1)
		}),
		workflows.New("dupeid", func(ctx workflows.Context, _ none) (none, error) {
			return nil, workflows.Parallel(ctx,
				func(c workflows.Context) error { _, err := step(c, "a", 1, workflows.WithID("same")); return err },
				func(c workflows.Context) error { _, err := step(c, "b", 2, workflows.WithID("same")); return err },
			)
		}),
		workflows.New("approval", func(ctx workflows.Context, in struct {
			APIBase string `json:"apiBase"`
		}) (map[string]any, error) {
			cb, err := workflows.CreateCallback(ctx, "approval", time.Minute)
			if err != nil {
				return nil, err
			}
			urlOK, err := workflows.Run(ctx, "notify", func(workflows.StepContext) (bool, error) {
				return cb.URL == in.APIBase+"/public/v1/callbacks/"+cb.Token, nil
			})
			if err != nil {
				return nil, err
			}
			v, err := workflows.WaitCallback[any](ctx, cb)
			if err != nil {
				return nil, err
			}
			return map[string]any{"value": v, "urlOk": urlOK}, nil
		}),
		workflows.New("approvaltimeout", func(ctx workflows.Context, _ none) (any, error) {
			cb, err := workflows.CreateCallback(ctx, "approval", time.Second)
			if err != nil {
				return nil, err
			}
			if _, err := workflows.WaitCallback[any](ctx, cb); err != nil {
				var we *workflows.Error
				if !errors.As(err, &we) {
					return nil, err
				}
				return map[string]any{"code": we.Code}, nil
			}
			return "completed", nil
		}),
		workflows.New("parent", func(ctx workflows.Context, _ none) (any, error) {
			return workflows.Invoke[any](ctx, "child", "sequence", map[string]int{"n": 3})
		}),
		workflows.New("parentcatch", func(ctx workflows.Context, _ none) (any, error) {
			if _, err := workflows.Invoke[any](ctx, "child", "fatal", nil); err != nil {
				var we, cause *workflows.Error
				if !errors.As(err, &we) || !errors.As(we.Cause, &cause) {
					return nil, err
				}
				return map[string]any{"code": we.Code, "causeCode": cause.Code}, nil
			}
			return "unreachable", nil
		}),
		workflows.New("fanout", func(ctx workflows.Context, _ none) (int, error) {
			vals := make([]int, 3)
			branches := make([]func(workflows.Context) error, 3)
			for i := range branches {
				branches[i] = func(c workflows.Context) (err error) {
					vals[i], err = step(c, "p"+string(rune('0'+i)), i)
					return
				}
			}
			if err := workflows.Parallel(ctx, branches...); err != nil {
				return 0, err
			}
			return vals[0] + vals[1] + vals[2], nil
		}),
		workflows.New("racetimeout", func(ctx workflows.Context, _ none) (string, error) {
			var out [2]string
			w, err := workflows.Race(ctx,
				func(c workflows.Context) (err error) { out[0], err = step(c, "work", "work"); return },
				func(c workflows.Context) error {
					if err := workflows.Sleep(c, "limit", time.Minute); err != nil {
						return err
					}
					out[1] = "limit"
					return nil
				},
			)
			if err != nil {
				return "", err
			}
			return out[w], nil
		}),
		workflows.New("scheduled", func(ctx workflows.Context, _ none) (*workflows.ScheduleInfo, error) {
			return ctx.Run().Schedule, nil
		}),
		workflows.New("pulled", func(ctx workflows.Context, in struct {
			X float64 `json:"x"`
		}) (float64, error) {
			return workflows.Run(ctx, "compute", func(workflows.StepContext) (float64, error) { return in.X * 2, nil })
		}),
		workflows.New("longstep", func(ctx workflows.Context, _ none) (string, error) {
			return workflows.Run(ctx, "long", func(sc workflows.StepContext) (string, error) {
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					select {
					case <-sc.Done():
						return "aborted", nil
					case <-time.After(50 * time.Millisecond):
					}
				}
				return "done", nil
			})
		}),
	}
}
