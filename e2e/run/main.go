// The end-to-end driver: each check through the public API (workflows.Client) against a local
// workflows-stack; it exits 1 at the first failure, with the API's status, code and message.
// E2E_PHASE1_ONLY=1 runs the phase 1 checks only.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	workflows "github.com/ezgamehost/workflows-sdk-go"
)

var (
	ctx    = context.Background()
	client *workflows.Client
	app    = envOr("E2E_APP", "http://127.0.0.1:3200")
)

func check(ok bool, what string, detail any) {
	if !ok {
		b, _ := json.MarshalIndent(detail, "", "  ")
		fmt.Fprintf(os.Stderr, "FAILED: %s\n%s\n", what, b)
		os.Exit(1)
	}
}

func fail(err error) {
	var ae *workflows.APIError
	if errors.As(err, &ae) {
		fmt.Fprintf(os.Stderr, "FAILED: the API answered %d %s: %s\n", ae.Status, ae.Code, ae.Message)
	} else {
		fmt.Fprintf(os.Stderr, "FAILED: %v\n", err)
	}
	os.Exit(1)
}

func must[T any](v T, err error) T {
	if err != nil {
		fail(err)
	}
	return v
}

func until(id string, done func(*workflows.WorkflowRun) bool, d time.Duration) *workflows.WorkflowRun {
	end := time.Now().Add(d)
	r := must(client.Get(ctx, id))
	for !done(r) && time.Now().Before(end) {
		time.Sleep(500 * time.Millisecond)
		r = must(client.Get(ctx, id))
	}
	check(done(r), fmt.Sprintf("run %s reached the expected state (it's %s)", r.Workflow, r.Status), r)
	return r
}

func terminal(r *workflows.WorkflowRun) bool { return r.Terminal() }

func output[T any](id string) T {
	var v T
	if err := client.Output(ctx, id, &v); err != nil {
		fail(err)
	}
	return v
}

func stepNamed(r *workflows.WorkflowRun, name string) *workflows.RunStep {
	for i := range r.Steps {
		if r.Steps[i].Name == name {
			return &r.Steps[i]
		}
	}
	return &workflows.RunStep{}
}

type scheduled struct {
	RunID         string    `json:"runId"`
	ScheduleID    string    `json:"scheduleId"`
	ScheduledTime time.Time `json:"scheduledTime"`
}

type state struct {
	Fixed       bool        `json:"fixed"`
	Prepare     int         `json:"prepare"`
	Cleanup     int         `json:"cleanup"`
	CallbackURL string      `json:"callbackUrl"`
	Scheduled   []scheduled `json:"scheduled"`
}

func readState() state {
	res, err := http.Get(app + "/__e2e/state")
	if err != nil {
		fail(err)
	}
	defer res.Body.Close()
	var s state
	if err := json.NewDecoder(res.Body).Decode(&s); err != nil {
		fail(err)
	}
	return s
}

func waitState(ok func(state) bool, d time.Duration) state {
	end := time.Now().Add(d)
	s := readState()
	for !ok(s) && time.Now().Before(end) {
		time.Sleep(time.Second)
		s = readState()
	}
	return s
}

// start starts a run, waiting out a pull app whose workflows a worker hasn't announced yet.
func start(appID, workflow string, input any, o workflows.StartOptions) *workflows.WorkflowRun {
	end := time.Now().Add(90 * time.Second)
	for {
		r, err := client.Start(ctx, appID, workflow, input, o)
		var ae *workflows.APIError
		if err == nil || !errors.As(err, &ae) || ae.Status != 409 || (ae.Code != "workflow_not_synced" && ae.Code != "app_not_synced") || time.Now().After(end) {
			return must(r, err)
		}
		time.Sleep(time.Second)
	}
}

func main() {
	client = must(workflows.NewClient(workflows.ClientOptions{APIKey: os.Getenv("E2E_API_KEY"), OrganizationID: os.Getenv("E2E_ORG"),
		ProjectID: os.Getenv("E2E_PROJECT"), BaseURL: envOr("E2E_API", "http://127.0.0.1:6100"), Retries: 90, MaxRetryDelay: 2 * time.Second}))
	push, pull := os.Getenv("E2E_APP_ID"), os.Getenv("E2E_PULL_APP_ID")
	phase2 := os.Getenv("E2E_PHASE1_ONLY") != "1"

	synced := must(client.Sync(ctx, push, workflows.SyncOptions{}))
	check(synced.SyncStatus == "synced" && len(synced.Workflows) == 12 && synced.Deployment != nil && *synced.Deployment == "e2e" &&
		synced.SDK != nil && synced.SDK.Name == workflows.SDKName, "sync records the app's workflows", synced)
	if phase2 {
		check(synced.ProtocolVersion != nil && *synced.ProtocolVersion == "1.1", "sync negotiates protocol 1.1", synced)
	}
	fmt.Println("ok   sync")

	r := until(start(push, "e2e-complete", nil, workflows.StartOptions{}).ID, terminal, time.Minute)
	check(r.Status == "completed" && output[map[string]int](r.ID)["sum"] == 3, "e2e-complete completes", r)
	fmt.Println("ok   completion")

	key := fmt.Sprintf("key-%d", time.Now().UnixNano())
	k1 := start(push, "e2e-complete", map[string]int{"a": 1}, workflows.StartOptions{RunKey: key})
	k2 := start(push, "e2e-complete", map[string]int{"a": 1}, workflows.StartOptions{RunKey: key})
	check(k1.ID == k2.ID, "the same runKey and input return the same run", []any{k1, k2})
	_, cerr := client.Start(ctx, push, "e2e-complete", map[string]int{"a": 2}, workflows.StartOptions{RunKey: key})
	var ae *workflows.APIError
	check(errors.As(cerr, &ae) && ae.Status == 409 && ae.Code == "run_key_conflict", "the same runKey with different input is 409 run_key_conflict", fmt.Sprint(cerr))
	fmt.Println("ok   idempotent start")

	slept := until(start(push, "e2e-sleep", nil, workflows.StartOptions{}).ID, terminal, time.Minute)
	check(slept.Status == "completed" && output[int](slept.ID) == 100_000 && stepNamed(slept, "nap").Status == "completed",
		"e2e-sleep resumes after its timer, with the large payload replayed", slept)
	fmt.Println("ok   sleep and payload replay")

	retried := until(start(push, "e2e-retry", nil, workflows.StartOptions{}).ID, terminal, time.Minute)
	check(retried.Status == "completed" && output[int](retried.ID) == 3 && stepNamed(retried, "flaky").Attempt == 3, "e2e-retry succeeds on attempt 3", retried)
	fmt.Println("ok   step retries")

	crashed := until(start(push, "e2e-crash", nil, workflows.StartOptions{}).ID, terminal, 90*time.Second)
	check(crashed.Status == "completed" && output[string](crashed.ID) == "survived" && stepNamed(crashed, "crashy").Attempt == 2,
		"a crash mid-step counts as an attempt of that step", crashed)
	fmt.Println("ok   crash mid-step is a step attempt")

	cancelling := start(push, "e2e-cancel", nil, workflows.StartOptions{})
	until(cancelling.ID, func(r *workflows.WorkflowRun) bool { return stepNamed(r, "wait").Status == "sleeping" }, time.Minute)
	must(client.Cancel(ctx, cancelling.ID, workflows.CancelOptions{}))
	cancelled := until(cancelling.ID, terminal, time.Minute)
	s1 := readState()
	check(cancelled.Status == "cancelled" && stepNamed(cancelled, "cleanup").Status == "completed" && s1.Cleanup == 1,
		"cancel runs the cleanup step and ends cancelled", []any{cancelled, s1})
	fmt.Println("ok   cancel with cleanup")

	fixable := until(start(push, "e2e-fixable", nil, workflows.StartOptions{}).ID, terminal, time.Minute)
	check(fixable.Status == "failed", "e2e-fixable fails before the fix", fixable)
	res, err := http.Get(app + "/__e2e/fix")
	if err != nil {
		fail(err)
	}
	res.Body.Close()
	must(client.Retry(ctx, fixable.ID))
	fixed := until(fixable.ID, func(r *workflows.WorkflowRun) bool {
		return r.Status == "completed" || (r.Status == "failed" && r.Attempt > fixable.Attempt)
	}, time.Minute)
	s2 := readState()
	check(fixed.Status == "completed" && output[string](fixed.ID) == "fixed" && s2.Prepare == 1,
		"retry resumes from the failed step without re-running completed ones", []any{fixed, s2})
	fmt.Println("ok   retry from failure")

	if os.Getenv("E2E_EXACT_JSON") == "1" {
		exact := until(start(push, "e2e-exact", nil, workflows.StartOptions{}).ID, terminal, time.Minute)
		text := string(output[json.RawMessage](exact.ID))
		check(exact.Status == "completed" && text == `{"b":1,"a":"x\u0000y","c":{"z":[2,1],"y":null}}`, "recorded JSON replays with its key order and NUL characters", []any{exact, text})
		fmt.Println("ok   exact JSON replay")
	} else {
		fmt.Println("skip exact JSON replay (this workflows-stack has no json-column migration 0005 yet)")
	}

	if !phase2 {
		fmt.Println("PASSED (phase 1 only)")
		return
	}

	if only("parallel") {
		par := until(start(push, "e2e-parallel", nil, workflows.StartOptions{}).ID, terminal, time.Minute)
		check(par.Status == "completed" && output[int](par.ID) == 6 && stepNamed(par, "p0").Status == "completed" &&
			stepNamed(par, "p1").Status == "completed" && stepNamed(par, "p2").Status == "completed", "parallel steps run as step invocations and join", par)
		fmt.Println("ok   parallel")
	}

	if only("callback") {
		cb := start(push, "e2e-callback", nil, workflows.StartOptions{})
		url := waitState(func(s state) bool { return s.CallbackURL != "" }, time.Minute).CallbackURL
		// The URL is on the public API base (an https origin); the local stack serves the same path.
		i := strings.Index(url, "/public/v1/callbacks/")
		check(i > 0 && strings.HasPrefix(url, "https://"), "the callback's URL is on the public API base", url)
		url = envOr("E2E_API", "http://127.0.0.1:6100") + url[i:]
		until(cb.ID, func(r *workflows.WorkflowRun) bool { return stepNamed(r, "approval").Status == "waiting" }, time.Minute)
		if err := workflows.CompleteCallbackURL(ctx, http.DefaultClient, url, "approved"); err != nil {
			fail(err)
		}
		again := workflows.CompleteCallbackURL(ctx, http.DefaultClient, url, "approved")
		differ := workflows.CompleteCallbackURL(ctx, http.DefaultClient, url, "rejected")
		cbr := until(cb.ID, terminal, time.Minute)
		check(cbr.Status == "completed" && output[string](cb.ID) == "approved" && again == nil && errors.As(differ, &ae) && ae.Status == 409,
			"a callback completes once over HTTP; a repeat is idempotent, a different body 409", []any{cbr, fmt.Sprint(again), fmt.Sprint(differ)})
		fmt.Println("ok   callback")
	}

	if only("child") {
		parent := until(start(push, "e2e-parent", nil, workflows.StartOptions{}).ID, terminal, 90*time.Second)
		check(parent.Status == "completed" && output[int](parent.ID) == 41, "a child workflow's result reaches its parent", parent)
		fmt.Println("ok   child workflow")
	}

	if only("schedule") {
		sched := must(client.CreateSchedule(ctx, workflows.ScheduleSpec{AppID: push, Workflow: "e2e-scheduled", Name: "e2e", Cron: "* * * * *", TimeZone: "UTC"}))
		trig := until(must(client.TriggerSchedule(ctx, sched.ID, fmt.Sprintf("t-%d", time.Now().UnixNano()))).ID, terminal, time.Minute)
		check(trig.Status == "completed", "a triggered schedule run completes", trig)
		fired := waitState(func(s state) bool {
			for _, e := range s.Scheduled {
				if e.RunID != trig.ID && e.ScheduleID == sched.ID {
					return true
				}
			}
			return false
		}, 150*time.Second)
		must(client.PauseSchedule(ctx, sched.ID))
		var firing *scheduled
		for i, e := range fired.Scheduled {
			if e.RunID != trig.ID && e.ScheduleID == sched.ID {
				firing = &fired.Scheduled[i]
			}
		}
		check(firing != nil && firing.ScheduledTime.Second() == 0 && firing.ScheduledTime.Nanosecond() == 0,
			"the schedule fired on its own, and the run saw its schedule and the minute it was scheduled for", fired)
		fmt.Println("ok   schedule")
	}

	if only("pull") {
		pr := until(start(pull, "e2e-pull", map[string]int{"x": 4}, workflows.StartOptions{}).ID, terminal, 90*time.Second)
		check(pr.Status == "completed" && output[int](pr.ID) == 10, "a pull worker runs the workflow", pr)
		fmt.Println("ok   pull worker")
	}
	if os.Getenv("E2E_ONLY") != "" {
		fmt.Println("PASSED (only " + os.Getenv("E2E_ONLY") + ")")
		return
	}

	fmt.Println("PASSED")
}

// only reports whether a phase 2 check runs: all of them, unless E2E_ONLY lists some (local
// debugging).
func only(name string) bool {
	list := os.Getenv("E2E_ONLY")
	if list == "" {
		return true
	}
	for _, n := range strings.Split(list, ",") {
		if n == name {
			return true
		}
	}
	return false
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
