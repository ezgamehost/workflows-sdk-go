# EZGH Workflows Go SDK

Durable workflows in your own Go app. You write a workflow as an ordinary Go function made of
steps. EZGH Workflows records each step's result, retries failures, sleeps for as long as you
like, waits for callbacks and child workflows, and resumes the workflow where it left off. It
invokes your app over HTTP (the push handler) or hands work to a pull worker you run.

The SDK uses only the Go standard library.

```sh
go get github.com/ezgamehost/workflows-sdk-go
```

**Status:** in development. Verified end to end against workflows-stack `1a97cec`: the
conformance harness (50 scenarios, push and pull) and the end-to-end suite (14 checks, phase 1 and
phase 2); see [Verification](#verification).

## Quick start

```go
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	workflows "github.com/ezgamehost/workflows-sdk-go"
)

type Signup struct {
	UserID string `json:"userId"`
}

var onboarding = workflows.New("onboarding", func(ctx workflows.Context, in Signup) (string, error) {
	user, err := workflows.Run(ctx, "load-user", func(sc workflows.StepContext) (User, error) {
		return db.LoadUser(sc, in.UserID)
	})
	if err != nil {
		return "", err
	}
	if _, err := workflows.Run(ctx, "welcome-email", func(sc workflows.StepContext) (bool, error) {
		return true, mail.Send(sc, user.Email, "welcome", sc.IdempotencyKey())
	}); err != nil {
		return "", err
	}
	if err := workflows.Sleep(ctx, "cooldown", 72*time.Hour); err != nil {
		return "", err
	}
	return "onboarded", nil
})

func main() {
	h, err := workflows.Handler(workflows.HandlerOptions{
		Workflows:      []workflows.Workflow{onboarding},
		SigningSecrets: []string{os.Getenv("EZGH_WORKFLOWS_SIGNING_SECRET")},
	})
	if err != nil {
		log.Fatal(err)
	}
	http.Handle("/workflows", h)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Create the app, sync it after each deploy, and start runs with the client:

```go
c, err := workflows.NewClient(workflows.ClientOptions{
	APIKey: os.Getenv("EZGH_API_KEY"), OrganizationID: orgID, ProjectID: projectID,
})
created, err := c.CreateApp(ctx, "my-app", workflows.CreateAppOptions{Endpoint: "https://example.com/workflows"})
// Store *created.SigningSecret: it's returned once.
_, err = c.Sync(ctx, created.App.ID, workflows.SyncOptions{})
run, err := c.Start(ctx, created.App.ID, "onboarding", Signup{UserID: "u_1"}, workflows.StartOptions{RunKey: "signup-u_1"})
```

## Steps

Steps are package functions that take the workflow's `Context`:

| Step | What it does |
| --- | --- |
| `Run[T](ctx, name, fn, opts...)` | Runs `fn` at least once and records its JSON result; on replay the result is returned without running `fn` |
| `Sleep(ctx, name, d)`, `SleepUntil(ctx, name, t)` | A durable timer (at most a year) |
| `Now(ctx, name)`, `UUID(ctx, name)` | A time or UUID recorded once, the same on every replay |
| `CreateCallback(ctx, name, timeout)`, `WaitCallback[T](ctx, cb)` | A single-use callback URL, and waiting for its JSON body |
| `Invoke[T](ctx, name, workflow, input)` | Starts a child run of a workflow in the same app and waits for its output |
| `Parallel(ctx, branches...)`, `Race(ctx, branches...)` | Concurrent branches whose steps are planned together |

- **Results are JSON.** A step's result is encoded to JSON and decoded back into `T`, live and on
  replay alike, so both are the same value. More than 1 MiB fails the step with
  `workflows.output_too_large`.
- **Names identify steps.** A step's ID is derived from its name and how many times the name was
  used before, so a loop calling `Run(ctx, "send", …)` gets distinct steps. `WithID("…")` sets an
  explicit ID, which names one step for the whole run. Two steps pending at once need different
  names (or IDs), or the run fails with `workflows.duplicate_step_name`.
- **Delivery is at least once.** A step can run again after a crash. Use
  `sc.IdempotencyKey()` (`<runId>:<stepId>:<attempt>`) with the systems you call.
- **Retries:** 5 attempts, 1 s apart doubling to at most 10 minutes, by default.
  `WithStepRetry(RetryPolicy{…})` and `WithStepTimeout(d)` change one step; `WithRetry` and
  `WithTimeout` on `New` change a workflow's default. Zero fields take the defaults.
- **The step's context** (`StepContext`) is cancelled at the step's timeout, at the invocation's
  deadline, and, in a pull worker, when the run is being cancelled.

## Errors

A step's error is retried until its policy is used up; then `Run` returns it, and the workflow
can handle it or return it (failing the run). Errors cross the wire as
`{code, name, message, data, stack, cause}`, and a replay rebuilds them as `*workflows.Error`:

```go
var we *workflows.Error
if errors.As(err, &we) { log.Print(we.Code, we.Name, string(we.Data)) }
if errors.Is(err, workflows.ErrCallbackTimeout) { /* … */ }
```

- `workflows.NonRetriable(err)` fails the step at once (`workflows.non_retriable`).
- `workflows.RetryAfter(err, d)` retries after `d` (at most 24 hours), for example after an
  upstream 429.
- `&workflows.Error{Code: "billing.declined", Message: "…", Data: json.RawMessage(…)}` carries
  your own code. Codes on the wire are `user` or the platform's `workflows.*`: a custom code is
  sent as `user`, with the code as the error's name and in `data.code`.
- The sentinels `ErrCancelled`, `ErrStepTimeout`, `ErrCallbackTimeout`, `ErrChildFailed`,
  `ErrNonRetriable`, `ErrOutputTooLarge` and `ErrDuplicateStepName` match by code with
  `errors.Is`.
- A panic in a workflow fails the run; a panic in a step fails that attempt.

## Cancellation

`Client.Cancel` cancels a run. The step the workflow is waiting on returns an error matching
`ErrCancelled`, and `ctx.Done()` is closed. The workflow may then run `Run` cleanup steps, which
retry as usual, within the grace period (default 5 minutes); any other step returns
`ErrCancelled` at once. The run ends `cancelled` whatever the workflow returns.

## Parallel and Race

```go
var crm, billing string
err := workflows.Parallel(ctx,
	func(c workflows.Context) (err error) { crm, err = workflows.Run(c, "crm", upsertCRM); return },
	func(c workflows.Context) (err error) { billing, err = workflows.Run(c, "billing", createCustomer); return },
)
```

- Each branch gets its own `Context` and must call steps with it.
- Steps that the branches reach together are planned together, and EZGH runs each as its own
  invocation (at most 32 at once per run).
- `Parallel` returns once every branch has returned: nil, the one error, or the errors joined in
  branch order.
- `Race` returns the index and error of the first branch to return, in the order EZGH recorded
  their steps, so a replay picks the same winner. The other branches keep running.

## Callbacks

```go
cb, err := workflows.CreateCallback(ctx, "manager-approval", 7*24*time.Hour)
_, err = workflows.Run(ctx, "ask-manager", func(sc workflows.StepContext) (bool, error) {
	return true, slack.Ask(sc, manager, cb.URL)
})
decision, err := workflows.WaitCallback[Decision](ctx, cb) // ErrCallbackTimeout after 7 days
```

- The callback is recorded before `CreateCallback` returns, so its URL can be handed out at once.
  Hand it out from a step.
- The URL's token is its only credential: treat it as a secret.
- POSTing JSON (at most 1 MiB) to the URL completes the callback, once. A repeat with the same
  body succeeds; a different body is `409 callback_completed`; after the timeout it's
  `410 callback_expired`.
- `workflows.CompleteCallbackURL(ctx, httpClient, url, value)` posts to a callback URL without an
  API key. `Client.CompleteCallback(ctx, runID, stepID, value)` completes one as an
  authenticated caller.

## Child workflows

```go
report, err := workflows.Invoke[Report](ctx, "provision", "provision-account", ProvisionInput{UserID: id})
```

The child runs in the same app, at most 10 levels deep, and counts as one of the parent's
steps. Its failure is an error matching `ErrChildFailed`, whose cause is the child's own error.

## Schedules

```go
s, err := c.CreateSchedule(ctx, workflows.ScheduleSpec{
	AppID: appID, Workflow: "nightly-report", Name: "nightly", Cron: "0 2 * * *", TimeZone: "Europe/London",
})
run, err := c.TriggerSchedule(ctx, s.ID, "manual-2026-10-10") // the key names the run
```

- Cron is 5 fields or `@hourly`, `@daily`, `@weekly`, `@monthly` and `@yearly`, at most once a
  minute, in an IANA time zone (default UTC).
- A run a schedule started has `ctx.Run().Schedule` set (`ID` and the cron `ScheduledTime`).
- `UpdateSchedule(ctx, id, revision, …)` applies only to that revision (`412` otherwise).
  `PauseSchedule`, `ResumeSchedule`, `ListSchedules` and `DeleteSchedule` do what they say.

## Pull workers and dev mode

A pull app's runs are polled by workers you run anywhere, with no public endpoint:

```go
c.CreateApp(ctx, "batch", workflows.CreateAppOptions{Kind: workflows.AppKindPull, Queue: "default"})

w, err := workflows.NewWorker(workflows.WorkerOptions{
	APIKey: key, OrganizationID: orgID, ProjectID: projectID, App: pullAppID,
	Workflows: []workflows.Workflow{onboarding}, MaxConcurrent: 10,
})
err = w.Run(ctx) // until ctx is done; then it drains running invocations
```

- The worker long-polls workflows-live and heartbeats while an invocation runs. A heartbeat that
  reports the run is cancelling cancels the running step's context.
- It retries 429, 5xx and network errors with backoff. It returns an `*APIError` when the poll is
  refused for good: 401 or 403 (the API key or its permissions), or another 4xx (the app or the
  queue).
- `DevMode: true` polls the queue `dev-<user>`, so developers sharing a project never take each
  other's runs.

## What a credential can do

- Run methods (`Start`, `Get`, `Cancel`, `Retry`, `Output`, `Input`, `StepOutput`, `Events`,
  `CompleteCallback`) take an API key or a `workflows:runs` token.
- App methods need an API key with `workflows.apps.*`.
- Schedule methods need an API key with `workflows.schedules.*` (creating one also needs
  `workflows.runs.start` on its workflow).
- A worker's key needs `workflows.workers.poll`, plus `workflows.apps.sync` to announce its
  workflows.
- The client never prints, logs or marshals its API key.

## Determinism rules for Go

Code outside steps runs again on every invocation, so keep it deterministic and free of side
effects.

- **Call steps only from the workflow's goroutine, or from a `Parallel`/`Race` branch with the
  branch's `Context`.** Calling steps from goroutines you start yourself fails the run.
- **Deferred functions run when an invocation suspends.** At a sleep, a wait or planned parallel
  steps, the workflow's goroutine ends with `runtime.Goexit`, so a step call never returns and
  `defer`s run. Keep side effects in steps; a step called from a `defer` while suspending records
  nothing and ends that deferred function.
- Use `Now` and `UUID` instead of `time.Now` and random numbers, and don't depend on map
  iteration order.

## Limits

| What | Limit |
| --- | --- |
| A step's result, a workflow's output, a child's input, a callback body | 1 MiB of JSON |
| A step name | 1–256 bytes of UTF-8, without NUL |
| An explicit step ID | `^[A-Za-z0-9._:-]{1,128}$` |
| A workflow name | `^[A-Za-z0-9_-]{1,64}$` |
| A sleep or a callback timeout | at most a year (a callback: at least 1 s) |
| A step timeout | 1 ms to 15 minutes |
| Retries | 1–100 attempts, at most 24 hours apart, multiplier 1–10 |
| An invocation body (push) | 8 MiB |
| Steps run inline per invocation | 50 (`MaxStepsPerInvocation`) |
| Concurrent step invocations per run | 32 |
| Child workflow depth | 10 |
| Error messages and stacks | cut to 8 KiB; data over 64 KiB is dropped; 8 levels of causes |

## The protocol package

`github.com/ezgamehost/workflows-sdk-go/protocol` holds the wire protocol (version 1.1):
messages, signature signing and verification (`Verify`), step IDs and tokens. It's checked
against workflows-stack's protocol vectors (`testdata/vectors`), and `Validate` methods mirror the
engine's limits; the SDK checks every message it sends with them.

## Verification

- `go test -race ./...` runs the unit tests, including every vendored protocol vector.
- `scripts/conformance.sh <workflows-stack checkout>` runs workflows-stack's conformance harness
  against `conformance/` (the push handler, and the same workflows as a pull worker).
- `scripts/e2e.sh <workflows-stack checkout>` runs the end-to-end suite against a local
  workflows-stack (it needs `DATABASE_URL`).

CI runs all three; the conformance and end-to-end jobs use workflows-stack at the commit in
`.workflows-stack-rev`.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
