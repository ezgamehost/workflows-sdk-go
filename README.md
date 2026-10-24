# EZGH Workflows Go SDK

Durable workflows in your own Go app. You write a workflow as an ordinary Go function made of
steps. EZGH Workflows records each step's result, retries failures, sleeps for as long as you
like, waits for callbacks and child workflows, and resumes the workflow where it left off. It
invokes your app over HTTP (the push handler) or hands work to a pull worker you run.

The SDK uses only the Go standard library.

```sh
go get github.com/ezgamehost/workflows-sdk-go
```

**Status:** in development. Verified end to end against workflows-stack `f717543`: the
conformance harness (61 scenarios, protocol 1.2's message waits included, push and pull) and the
end-to-end suite (14 checks, phase 1 and phase 2); see [Verification](#verification).

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
| `Parallel(ctx, branches...)`, `All(ctx, branches...)`, `Race(ctx, branches...)` | Concurrent branches whose steps are planned together |

- **Results are JSON.** A step's result is encoded to JSON and decoded back into `T`, live and on
  replay alike, so both are the same value. More than 1 MiB fails the step with
  `workflows.output_too_large`.
- **Names identify steps.** A step's ID is derived from its name and how many times the name was
  used before, so a loop calling `Run(ctx, "send", …)` gets distinct steps. `WithID("…")` sets an
  explicit ID, which names one step for the whole run (on `Run`, `Sleep`, `CreateCallback` and
  `Invoke`, whose child run ID derives from it, as in the other SDKs). Two steps pending at once need different
  names (or IDs), or the run fails with `workflows.duplicate_step_name`.
- **Delivery is at least once.** A step can run again after a crash. Use
  `sc.IdempotencyKey()` (`<runId>:<stepId>:<attempt>`) with the systems you call.
- **Retries:** 5 attempts, 1 s apart doubling to at most 10 minutes, by default
  (`DefaultRetry`). `WithStepRetry(p)` and `WithStepTimeout(d)` change one step; `WithRetry` and
  `WithTimeout` on `New` change a workflow's default. A policy is used as given, zero fields
  included, so start from the default:

  ```go
  p := workflows.DefaultRetry
  p.MaxAttempts, p.Initial = 3, 0 // retry at once, twice
  workflows.Run(ctx, "charge", charge, workflows.WithStepRetry(p))
  ```
- **The step's context** (`StepContext`) is cancelled at the step's timeout, at the invocation's
  deadline, and, in a pull worker, when the run is being cancelled (`context.Cause` is then
  `ErrCancelled`).
- **A step's function can't call steps.** Calling one with the workflow's `Context` from inside a
  step's function returns an error, which fails the step non-retriably.

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

## Parallel, All and Race

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
- `All` returns at the first error (the other branches keep running), or nil once every branch has
  returned: `Promise.all` and `asyncio.gather`. Use it when a failure, or a cancel, should reach the
  workflow at once, for example to run cleanup while siblings are still in flight.
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
  API key. `Client.CompleteCallback(ctx, runID, stepID, value)` (which returns the run) completes one as an
  authenticated caller.

## Child workflows

```go
report, err := workflows.Invoke[Report](ctx, "provision", "provision-account", ProvisionInput{UserID: id})
```

The child runs in the same app, at most 10 levels deep, and counts as one of the parent's
steps. Its failure is an error matching `ErrChildFailed`, whose cause is the child's own error.

## Sending to a queue

`SendToQueue` sends 1–10 messages to an [EZGH Queues](https://ezghcloud.com) queue as a step,
and returns the recorded result (one `SentMessage{MessageID, Seq, Duplicate}` per message, in
order). It's `Run` around `QueuesClient.Send`, so a replay returns the result without sending
again. The client carries your own API key (it needs `queues.messages.send` on the queue): push
apps hold no API key, so the step never borrows a credential.

```go
queues, err := workflows.NewQueuesClient(workflows.QueuesClientOptions{
	APIKey: apiKey, OrganizationID: orgID, ProjectID: projectID, // BaseURL: https://queues-api.ezghcloud.com
})

// in a workflow:
sent, err := workflows.SendToQueue(ctx, "notify", queues, "que_…", []workflows.QueueMessage{
	{Body: string(order), Attributes: map[string]string{"orderId": id}, GroupID: accountID},
})
```

- **Sent once.** Every attempt of the step sends the `Idempotency-Key`
  `wfq_<hex(SHA-256(runID + ":" + stepID))>` (`SendToQueueDigest`), which the Queues API honors
  for 48 hours: an attempt that resends after a crash (between the send and the step's record)
  gets the first answer instead of a second copy, on standard and FIFO queues alike. So keep the
  messages deterministic: the same key with a different body is a `409 idempotency_conflict`.
- **FIFO dedup IDs.** A message with a `GroupID` (a FIFO queue's message) and no `DedupID` of its
  own is sent with `DedupID` `<that hex>:<index>`; FIFO queues also drop a repeat within their
  5-minute window. Messages without a `GroupID` are sent as they are, since standard queues
  refuse `DedupID`.
- **Errors.** The client resends a 429 (but not `quota_exceeded`), any 5xx, a 409
  `idempotency_in_progress` and network errors after `Retry-After` (`Retries`, default 3, at most
  `MaxRetryDelay`, default 10 s, apart), then returns a `*QueuesAPIError` with the HTTP `Status`,
  the platform `Code` and `RetryAfter`. In the step, what's still failing is retried by the step's
  retry policy, but another 4xx (a bad message, a missing queue, a refused permission) fails the
  step at once (non-retriable; recorded with the name `QueuesAPIError` and data `{status, code}`).

`queues.Send(ctx, queueID, msgs, workflows.SendOptions{IdempotencyKey: …})` works outside
workflows too (without a key, each call gets a random one, kept across its resends).

## Waiting for a message

`WaitForMessage` suspends the workflow until a message for a key arrives on an EZGH queue, and
returns it with `ok` true; once the timeout (1 second to 30 days) passes without one, `ok` is false
and the error nil. A timeout isn't an error.

```go
paid, ok, err := workflows.WaitForMessage(ctx, "paid", workflows.WaitForMessageOptions{
	QueueID: queueID, Key: "order:" + in.OrderID, Timeout: 24 * time.Hour,
})
if err != nil {
	return err
}
if !ok {
	return cancelOrder(ctx, in.OrderID) // nobody paid in time
}
```

- **Delivery.** The queue needs a subscription that targets the app's workflow in `resume` mode
  with a `correlationAttribute` (say `orderId`). Each message whose attribute equals a wait's `Key`
  resumes every run waiting on that queue and key at that moment. A message nobody waits for is
  retried with the subscription's backoff, then dead-lettered.
- **The message** is a `ReceivedMessage{MessageID, Body, Attributes, SentAt, QueueID, GroupID,
  Seq}` (the last two FIFO only). It's recorded, so replays return the same one.
- `QueueID` is the queue's ID (a lowercase UUID, not its `que_` slug); `Key` matches
  `^[A-Za-z0-9_.:-]{1,256}$`. It takes `WithID`. Cancelling the run returns an error matching
  `ErrCancelled` from a pending wait.
- Waits need a workflows-stack serving protocol 1.2; at 1.0 or 1.1 the run fails non-retriably.

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

## Tracing

While the project's Workflows observability setting is on, EZGH sends each invocation a W3C
`traceparent` (the push request's header, or a pulled invocation's field), and
`ctx.Run().Traceparent` is that value exactly as received. It's `""` while the setting is off, so a
parent-based sampler never waits for a parent that won't arrive. The SDK has no OpenTelemetry
dependency; with `go.opentelemetry.io/otel`, parent your own spans on it. Start them inside step
functions: code outside steps re-runs on every replay.

```go
import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var tracer = otel.Tracer("billing")

var charge = workflows.New("charge", func(ctx workflows.Context, in Order) (string, error) {
	return workflows.Run(ctx, "charge-card", func(sc workflows.StepContext) (string, error) {
		parent := propagation.TraceContext{}.Extract(sc, propagation.MapCarrier{"traceparent": ctx.Run().Traceparent})
		spanCtx, span := tracer.Start(parent, "charge card")
		defer span.End()
		return payments.Charge(spanCtx, in)
	})
})
```

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

- The worker long-polls workflows-live and heartbeats while an invocation runs, until its
  result is in (`HeartbeatInterval`, default 20 s, at most 30 s; the engine's heartbeat timeout
  is 60 s). A heartbeat that reports the run is cancelling cancels the running step's context.
- **A pull invocation's deadline** is the app's longest announced workflow timeout (`WithTimeout`):
  at least 15 minutes, at most 24 hours. Pull is for long steps; a push handler refuses a
  workflow timeout longer than its `MaxInvocationSeconds`.
- It retries 429, 5xx and network errors with backoff. It returns an `*APIError` when the poll is
  refused for good: 401 or 403 (the API key or its permissions), or another 4xx (the app or the
  queue).
- On shutdown it stops polling, except that a poll whose answer has started arriving is read and
  its tasks run, and waits up to `ShutdownTimeout` for running invocations.
- **Dev mode:** `DevMode: true` polls the queue `dev-<user>`, so developers sharing a project
  never take each other's runs. workflows-live serves only an app's own queue, so each developer
  needs a pull app of their own: `Client.DevApp` creates it, or finds it, named and queued
  `dev-<user>`:

  ```go
  app, err := c.DevApp(ctx, workflows.DevAppOptions{})
  w, err := workflows.NewWorker(workflows.WorkerOptions{ /* … */ App: app.ID, DevMode: true})
  ```

## The client

- **Retries:** a 429, a 503, a 409 `idempotency_in_progress` or a network error is retried after
  its `Retry-After` (at most `MaxRetryDelay`, default 10 s). `ClientOptions.Retries` is how many
  times: nil means the default, 3, and a pointer to 0 means none (`Retries: new(0)`). Writes are
  resent with the same `Idempotency-Key`; `CreateApp` and `RotateSecret` never are, and answer
  `outcome_unknown` instead, since a resend would return no secret.
- **Redirects** are never followed, by the client, the push handler's calls to workflows-live or
  the worker: a 3xx is an error answer, so a credential is never sent elsewhere.
- **Times in inputs.** Go writes a `time.Time` as RFC 3339 with the fractional seconds it has
  (`2026-10-10T12:00:00Z`); the TypeScript and Python SDKs write milliseconds
  (`2026-10-10T12:00:00.000Z`). A run key's input is compared by its hash, so the same start from
  two languages with a time in its input differs. When a time is part of a keyed start's input
  (or a schedule's), send it as a string in one form, for example
  `t.UTC().Format("2006-01-02T15:04:05.000Z07:00")`. The SDK doesn't rewrite your input.

## What a credential can do

- Run methods (`Start`, `Get`, `Cancel`, `Retry`, `Output`, `Input`, `StepOutput`, `Events`,
  `CompleteCallback`) take an API key or a `workflows:runs` token.
- App methods need an API key with `workflows.apps.*`.
- Schedule methods need an API key with `workflows.schedules.*` (creating one also needs
  `workflows.runs.start` on its workflow).
- A worker's key needs `workflows.workers.poll`, plus `workflows.apps.sync` to announce its
  workflows.
- The client never prints, logs or marshals its API key.

### On an EZGH compute instance

Leave `APIKey` empty on an EZGH compute instance that has a Bot attached, and the client, the
queues client and the worker use the Bot's short-lived key from the instance's metadata service.
They fetch it when needed, refresh it 5 minutes before it expires, and fetch once more and
retry once when the API answers 401. The Bot needs the actions above, like any key. An explicit
`APIKey` always wins; off an instance (or with no Bot attached, `ErrNoBot`) an empty key is still
an error. Anything on the instance can fetch the Bot's key, containers included.

## Determinism rules for Go

Code outside steps runs again on every invocation, so keep it deterministic and free of side
effects.

- **Call steps only from the workflow's goroutine, or from a `Parallel`/`Race` branch with the
  branch's `Context`.** Calling steps from goroutines you start yourself fails the run, and
  calling them from inside a step's function fails that step.
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
| A step timeout | 1 ms to 24 hours (push: at most `MaxInvocationSeconds`) |
| Retries | 1–100 attempts, at most 24 hours apart, multiplier 1–10 |
| An invocation body (push) | 8 MiB |
| An invocation's result | 16 MiB (push), 8 MiB (pull); more fails the run |
| Steps planned together (`Parallel`) | 1,000; more fails the run (fan out through children) |
| Steps run inline per invocation | 50 (`MaxStepsPerInvocation`) |
| Concurrent step invocations per run | 32 |
| Child workflow depth | 10 |
| Error messages and stacks | cut to 8 KiB; data over 64 KiB is dropped; 8 levels of causes |

## The protocol package

`github.com/ezgamehost/workflows-sdk-go/protocol` holds what an app may need on its own: the
protocol versions and the `EZGH-Signature` on push invocations (`DecodeSecret`, `Sign`,
`ParseSignature`, `Verify`), for apps that verify signatures themselves; `Handler` already does.
The rest of the wire model is internal. It's checked against workflows-stack's protocol vectors
(`testdata/vectors`), and the SDK validates every message it sends against the engine's limits.

The SDK supports the two latest Go releases (go.mod declares Go 1.26); CI builds and tests both.

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
