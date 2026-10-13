package workflows

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/jsonx"
	"github.com/ezgamehost/workflows-sdk-go/internal/live"
	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// How the runtime works (the design decision everything here rests on):
//
//   - Workflow code runs on goroutines, one per branch: the root, and one per Parallel or Race
//     branch. A step call parks its branch and waits for the executor's reply.
//   - The executor decides only at quiescence, when every branch is parked at a step call or has
//     returned. It orders pending calls by (epoch, branch path, the branch's call number): the
//     epoch counts the executor's replies, and the path is the index chain of nested
//     Parallel/Race branches. That's the TypeScript SDK's call order wherever TypeScript's is
//     defined, and it's deterministic where goroutine scheduling isn't.
//   - It replies to one call at a time (recorded outcomes lowest seq first, as every SDK does),
//     then waits for quiescence again.
//   - When the invocation ends, every parked call is answered with "exit": the step function
//     calls runtime.Goexit, so the branch's goroutine ends (running its deferred functions)
//     without the call ever returning. Goexit can't be recovered, so no user code runs past a
//     suspension, and no goroutine leaks.

type callKind int

const (
	kindRun callKind = iota
	kindSleep
	kindValue
	kindCallbackCreate
	kindCallbackWait
	kindInvoke
	kindGroup // Parallel or Race: answered by the executor at once, never journaled
)

type call struct {
	kind     callKind
	name     string
	explicit string
	stepID   string
	epoch    int
	path     []int
	seq      int
	b        *branch
	reply    chan reply
	settled  bool
	assigned bool

	fn        func(StepContext) (json.RawMessage, error)
	retry     protocol.RetryPolicy
	timeout   time.Duration
	until     time.Time
	produce   func(*executor) (json.RawMessage, error)
	timeoutMs int64
	workflow  string
	input     json.RawMessage
	branches  []func(Context) error
	race      bool
	failFast  bool // All: the first error decides
}

type reply struct {
	out    json.RawMessage
	err    error
	winner int
	exit   bool
}

type event struct {
	park   *call
	done   *branch
	err    error
	exited bool // the goroutine ended by Goexit, not by returning
}

// branch is one goroutine running workflow code: the root, or a Parallel/Race branch.
type branch struct {
	context.Context
	x       *executor
	path    []int
	next    int         // the branch's call counter (touched only by its own goroutine)
	busy    atomic.Bool // a call is parked: a second one at once is misuse
	exiting atomic.Bool
	group   *group // nil for the root
	index   int

	// Executor-side state.
	active bool // running workflow code (not parked, not done)
	epoch  int  // when it finished
}

type group struct {
	parent   *call
	pending  int
	errs     []error
	race     bool
	failFast bool
	decided  bool
}

func (b *branch) Run() RunInfo { return b.x.info }

func branchOf(ctx Context) (*branch, error) {
	b, ok := ctx.(*branch)
	if !ok || b == nil {
		return nil, argumentError("steps take the workflows.Context their workflow (or Parallel or Race branch) received")
	}
	return b, nil
}

// call parks the branch until the executor answers; an exit answer ends the goroutine.
func (b *branch) call(c *call) reply {
	if b.exiting.Load() || b.x.ended.Load() {
		runtime.Goexit()
	}
	if b.x.inStep.Load() {
		// A step's function called a step: the workflow is parked at that step, so the call can't
		// be answered. The inner call fails (and with it, normally, the step).
		return reply{err: NonRetriable(argumentError("steps can't be called inside a step's function; call them from the workflow"))}
	}
	if !b.busy.CompareAndSwap(false, true) {
		b.x.misuse.Store(true)
		runtime.Goexit()
	}
	c.b, c.path, c.seq, c.reply = b, b.path, b.next, make(chan reply, 1)
	b.next++
	b.x.events <- event{park: c}
	r := <-c.reply
	b.busy.Store(false)
	if r.exit {
		b.exiting.Store(true)
		runtime.Goexit()
	}
	return r
}

type execOptions struct {
	Now       func() time.Time
	Deadline  time.Time       // the invocation's deadline on this machine's clock
	MaxSteps  int             // steps run inline per invocation (default 50)
	Cancelled <-chan struct{} // closed when a pull heartbeat reports the run is cancelling
	Rand      io.Reader
	APIBase   string // callback URLs when the invocation carries no endpoints.api
	// MaxResultBytes bounds the invocation's result (the transport's body cap): a result past it
	// fails the run instead of being rebuilt on every redispatch. 0: 8 MiB.
	MaxResultBytes int
	Logf           func(format string, args ...any)
}

// DefaultAPIBase is the public API, on which callback URLs are built when an invocation doesn't
// say otherwise.
const DefaultAPIBase = "https://workflows-api.ezghcloud.com"

type executor struct {
	inv      *protocol.Invocation
	wf       *definition
	io       live.IO
	o        execOptions
	info     RunInfo
	now      func() time.Time
	rand     io.Reader
	parallel bool // protocol 1.1 or later: the frontier rule; 1.0: phase 1's sequential steps
	apiBase  string

	events chan event
	ended  atomic.Bool
	misuse atomic.Bool
	inStep atomic.Bool // a step's function is running
	// hbCancel: a pull heartbeat reported the run is cancelling; no new inline step starts.
	hbCancel atomic.Bool

	journal      map[string]protocol.JournalEntry
	pending      []*call   // parked calls, settled or not, kept for the exit replies
	groups       []*group  // every Parallel/Race, for the exit replies
	completions  []*branch // group branches that returned, not yet applied
	branchErrs   map[*branch]error
	running      int // active branches
	live         int // branch goroutines not yet ended
	epoch        int
	order        int
	counters     map[string]int
	explicit     map[string]bool
	outbox       []protocol.Command
	cpSeq        int
	executed     int
	started      time.Time
	fatal        *protocol.WireError
	cancelThrown bool
	finished     bool
	result       json.RawMessage
	lastOutput   json.RawMessage
	resultErr    error
	errStep      map[*Error]string // recorded failures → their step IDs (causeStepId)
	cancelCtx    context.Context
	cancelFn     context.CancelFunc
}

func supportsParallel(version string) bool {
	major, minor, _ := strings.Cut(version, ".")
	if major != "1" {
		return false
	}
	n := 0
	for _, r := range minor {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
		if n > 1000 {
			break
		}
	}
	return n >= 1
}

// execute runs one orchestrate or step invocation of wf and returns its InvocationResult's
// commands. Errors: live.ErrSuperseded, or *live.CheckpointFailed, when workflows-live didn't
// take a checkpoint (no step ran past it).
func execute(ctx context.Context, inv *protocol.Invocation, wf *definition, io live.IO, o execOptions) ([]protocol.Command, error) {
	x := &executor{
		inv: inv, wf: wf, io: io, o: o, now: o.Now, rand: o.Rand,
		parallel: supportsParallel(inv.ProtocolVersion),
		events:   make(chan event, 64),
		journal:  map[string]protocol.JournalEntry{}, counters: map[string]int{}, explicit: map[string]bool{},
		errStep: map[*Error]string{}, branchErrs: map[*branch]error{},
	}
	if x.now == nil {
		x.now = time.Now
	}
	if x.rand == nil {
		x.rand = rand.Reader
	}
	if x.o.MaxSteps <= 0 {
		x.o.MaxSteps = 50
	}
	if x.o.Deadline.IsZero() {
		x.o.Deadline = inv.Deadline
	}
	x.apiBase = inv.Endpoints.API
	if x.apiBase == "" {
		x.apiBase = o.APIBase
	}
	if x.apiBase == "" {
		x.apiBase = DefaultAPIBase
	}
	x.apiBase = strings.TrimRight(x.apiBase, "/")
	x.started = x.now()
	x.info = RunInfo{ID: inv.Run.ID, RunKey: inv.Run.RunKey, Workflow: inv.Run.Workflow, Attempt: inv.Run.Attempt, Traceparent: inv.Traceparent}
	if s := inv.Run.Schedule; s != nil {
		x.info.Schedule = &ScheduleInfo{ID: s.ID, ScheduledTime: s.ScheduledTime}
	}
	x.cancelCtx, x.cancelFn = context.WithCancel(context.WithoutCancel(ctx))
	defer x.cancelFn()
	if inv.Cancel != nil {
		x.cancelFn() // ctx.Done() is closed for the whole of a cancelling invocation
	}
	if o.Cancelled != nil {
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-o.Cancelled:
				x.hbCancel.Store(true)
				x.cancelFn()
			case <-stop:
			}
		}()
	}
	if x.o.MaxResultBytes <= 0 {
		x.o.MaxResultBytes = 8 << 20
	}
	if err := x.load(ctx); err != nil {
		return nil, err
	}
	return x.loop(ctx)
}

func (x *executor) logf(format string, args ...any) {
	if x.o.Logf != nil {
		x.o.Logf(format, args...)
	}
}

func (x *executor) defaultRetry() protocol.RetryPolicy {
	if x.wf.retry != nil {
		return *x.wf.retry
	}
	return protocol.DefaultRetry
}

// load pages the rest of an incomplete journal and fetches offloaded outputs (eight at a time)
// and the input, so workflow code never waits on I/O during replay.
func (x *executor) load(ctx context.Context) error {
	var entries []protocol.JournalEntry
	complete := true
	if x.inv.Journal != nil {
		entries = slices.Clone(x.inv.Journal.Entries)
		complete = x.inv.Journal.Complete
	}
	for !complete {
		var last int64
		for _, e := range entries {
			last = max(last, e.Seq)
		}
		page, err := x.io.JournalPage(ctx, last)
		if err != nil {
			return err
		}
		entries = append(entries, page.Entries...)
		complete = page.Complete || len(page.Entries) == 0
	}
	entries = dedupeJournal(entries)
	var refs []int
	for i := range entries {
		if entries[i].OutputRef != "" {
			refs = append(refs, i)
		}
	}
	for start := 0; start < len(refs); start += 8 {
		batch := refs[start:min(start+8, len(refs))]
		errs := make([]error, len(batch))
		var wg sync.WaitGroup
		for j, i := range batch {
			wg.Add(1)
			go func() {
				defer wg.Done()
				out, err := x.io.Payload(ctx, entries[i].OutputRef)
				errs[j] = err
				if err == nil {
					entries[i].Output, entries[i].OutputRef = out, ""
				}
			}()
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			for _, e := range errs {
				if e != nil {
					return e
				}
			}
		}
	}
	if ref := x.inv.Run.InputRef; ref != "" {
		in, err := x.io.Payload(ctx, ref)
		if err != nil {
			return err
		}
		x.inv.Run.Input = in
	}
	for _, e := range entries {
		x.journal[e.StepID] = e
	}
	return nil
}

// dedupeJournal keeps one entry per step, the highest-seq one: pages are read against the
// current state, so a step that changed between page fetches comes back on a later page at its
// new seq. A callback's createdSeq is kept from whichever copy has it.
func dedupeJournal(entries []protocol.JournalEntry) []protocol.JournalEntry {
	byStep := make(map[string]int, len(entries))
	out := make([]protocol.JournalEntry, 0, len(entries))
	for _, e := range entries {
		i, seen := byStep[e.StepID]
		if !seen {
			byStep[e.StepID] = len(out)
			out = append(out, e)
			continue
		}
		created := max(out[i].CreatedSeq, e.CreatedSeq)
		if e.Seq > out[i].Seq {
			out[i] = e
		}
		if out[i].CreatedSeq == 0 {
			out[i].CreatedSeq = created
		}
	}
	return out
}

func (x *executor) spawn(path []int, g *group, index int, fn func(Context) error) {
	b := &branch{Context: x.cancelCtx, x: x, path: path, group: g, index: index, active: true}
	x.running++
	x.live++
	go func() {
		var err error
		returned := false
		defer func() {
			if r := recover(); r != nil {
				err = &Error{Code: "user", Name: "panic", Message: fmt.Sprint(r)}
				returned = true
			}
			x.events <- event{done: b, err: err, exited: !returned}
		}()
		err = fn(b)
		returned = true
	}()
}

// quiesce handles events until no branch runs workflow code. It gives up at the deadline (a
// branch blocked outside a step) and reports false.
func (x *executor) quiesce(ctx context.Context) bool {
	if x.running == 0 {
		return true
	}
	timer := time.NewTimer(max(time.Millisecond, x.o.Deadline.Sub(x.now())))
	defer timer.Stop()
	for x.running > 0 {
		select {
		case ev := <-x.events:
			x.handle(ev)
		case <-timer.C:
			x.logf("workflows: the workflow's code didn't reach a step or return before the deadline")
			return false
		case <-ctx.Done():
			return false
		}
	}
	return true
}

func (x *executor) handle(ev event) {
	switch {
	case ev.park != nil:
		c := ev.park
		b := c.b
		if b.active {
			b.active = false
			x.running--
		}
		c.epoch = x.epoch
		if c.kind == kindGroup {
			g := &group{parent: c, pending: len(c.branches), errs: make([]error, len(c.branches)), race: c.race, failFast: c.failFast}
			x.groups = append(x.groups, g)
			if len(c.branches) == 0 {
				x.resume(c, reply{})
				return
			}
			for i, fn := range c.branches {
				x.spawn(append(slices.Clone(c.path), i), g, i, fn)
			}
			return
		}
		c.assigned = false
		x.pending = append(x.pending, c)
	case ev.done != nil:
		b := ev.done
		x.live--
		if b.active {
			b.active = false
			x.running--
		}
		if ev.exited {
			return
		}
		b.epoch = x.epoch
		if b.group == nil {
			x.finished = true
			if x.resultErr == nil {
				x.resultErr = ev.err
			}
			return
		}
		x.branchErrs[b] = ev.err
		x.completions = append(x.completions, b)
	}
}

func (x *executor) resume(c *call, r reply) {
	c.settled = true
	x.epoch++
	if b := c.b; b != nil && !b.active {
		b.active = true
		x.running++
	}
	c.reply <- r
}

func pathLess(ae int, ap []int, be int, bp []int) int {
	if ae != be {
		if ae < be {
			return -1
		}
		return 1
	}
	return slices.Compare(ap, bp)
}

func before(a, b *call) bool {
	if c := pathLess(a.epoch, a.path, b.epoch, b.path); c != 0 {
		return c < 0
	}
	return a.seq < b.seq
}

// applyCompletion applies the first returned group branch (by epoch, then path); it reports
// whether there was one.
func (x *executor) applyCompletion() bool {
	if len(x.completions) == 0 {
		return false
	}
	slices.SortStableFunc(x.completions, func(a, b *branch) int { return pathLess(a.epoch, a.path, b.epoch, b.path) })
	b := x.completions[0]
	x.completions = x.completions[1:]
	g := b.group
	err := x.branchErrs[b]
	delete(x.branchErrs, b)
	g.errs[b.index] = err
	g.pending--
	switch {
	case g.decided:
	case g.race:
		g.decided = true
		x.resume(g.parent, reply{winner: b.index, err: err})
	case g.failFast && err != nil:
		g.decided = true
		x.resume(g.parent, reply{winner: b.index, err: err})
	case g.pending == 0:
		g.decided = true
		x.resume(g.parent, reply{err: joinBranchErrors(g.errs)})
	}
	return true
}

// joinBranchErrors: nil, the one error, or every error joined in branch order.
func joinBranchErrors(errs []error) error {
	var non []error
	for _, e := range errs {
		if e != nil {
			non = append(non, e)
		}
	}
	switch len(non) {
	case 0:
		return nil
	case 1:
		return non[0]
	}
	return errors.Join(non...)
}

// unsettled: the parked calls not yet answered, in call order. Step IDs are assigned to newly
// parked calls in that order, so names count up the same way on every replay.
func (x *executor) unsettled() []*call {
	var out []*call
	keep := x.pending[:0]
	for _, c := range x.pending {
		if !c.settled {
			out = append(out, c)
			keep = append(keep, c)
		}
	}
	x.pending = keep
	slices.SortStableFunc(out, func(a, b *call) int {
		if before(a, b) {
			return -1
		}
		if before(b, a) {
			return 1
		}
		return 0
	})
	return out
}

func (x *executor) setFatal(err error) {
	if x.fatal == nil {
		x.fatal = encodeError(err)
	}
}

// assign registers newly parked calls in call order: step IDs, duplicate names and IDs, and the
// protocol version's primitives.
func (x *executor) assign(pending []*call) {
	for _, c := range pending {
		if c.assigned {
			continue
		}
		c.assigned = true
		x.order++
		if c.kind == kindCallbackWait {
			continue // the callback's own step ID
		}
		if (c.kind == kindCallbackCreate || c.kind == kindInvoke) && !x.parallel {
			what := "CreateCallback"
			if c.kind == kindInvoke {
				what = "Invoke"
			}
			x.setFatal(NonRetriable(fmt.Errorf("workflows.%s needs protocol 1.1; this invocation is %s", what, x.inv.ProtocolVersion)))
		}
		if c.explicit != "" {
			if x.explicit[c.explicit] {
				x.setFatal(newError("workflows.duplicate_step_name", "DuplicateStepName", "the step id %q is used twice; give each step its own id", c.explicit))
			}
			x.explicit[c.explicit] = true
			c.stepID = c.explicit
			continue
		}
		n := x.counters[c.name]
		x.counters[c.name] = n + 1
		c.stepID = protocol.StepID(c.name, n)
		for _, o := range pending {
			if o != c && o.assigned && !o.settled && o.explicit == "" && o.kind != kindCallbackWait && o.name == c.name && o.stepID != "" && o.stepID != c.stepID {
				x.setFatal(newError("workflows.duplicate_step_name", "DuplicateStepName", "two steps named %q are pending at once; give each a distinct name or an ID", c.name))
			}
		}
	}
}

var settledStatus = map[string]bool{protocol.StatusCompleted: true, protocol.StatusFailed: true, protocol.StatusCancelled: true}

// recorded: the journal entry that settles c, and the seq it settles at. A callback's creation
// is recorded once its token is journaled, whatever the callback's status, and settles at the
// seq its creation committed (createdSeq; seq from an engine that doesn't send it); every other
// call, wait() included, once its step has an outcome, at its seq.
func (x *executor) recorded(c *call) (protocol.JournalEntry, int64, bool) {
	e, ok := x.journal[c.stepID]
	if !ok {
		return e, 0, false
	}
	if c.kind == kindCallbackCreate {
		seq := e.Seq
		if e.CreatedSeq > 0 {
			seq = e.CreatedSeq
		}
		return e, seq, e.Token != ""
	}
	return e, e.Seq, settledStatus[e.Status]
}

func cancelledError(msg string) *Error {
	return &Error{Code: "workflows.cancelled", Name: "CancelledError", Message: msg}
}

func (x *executor) settleRecorded(c *call, e protocol.JournalEntry) {
	switch {
	case c.kind == kindCallbackCreate:
		x.resume(c, reply{out: x.callbackHandle(e.Token)})
	case e.Status == protocol.StatusCompleted && c.kind == kindSleep:
		x.resume(c, reply{})
	case e.Status == protocol.StatusCompleted:
		out := e.Output
		if len(out) == 0 {
			out = json.RawMessage("null")
		}
		x.resume(c, reply{out: out})
	case e.Status == protocol.StatusCancelled:
		x.cancelThrown = true
		x.resume(c, reply{err: cancelledError("the run was cancelled")})
	default:
		err := decodeError(e.Error)
		x.errStep[err] = c.stepID
		x.resume(c, reply{err: err})
	}
}

func (x *executor) loop(ctx context.Context) ([]protocol.Command, error) {
	x.spawn(nil, nil, 0, func(ctx Context) error {
		out, err := x.wf.run(ctx, x.inv.Run.Input)
		x.result, x.resultErr = out, err
		return err
	})
	for {
		if !x.quiesce(ctx) {
			return x.end(nil), nil
		}
		if x.misuse.Load() {
			x.setFatal(argumentError("a workflow called steps from two goroutines at once; use workflows.Parallel or workflows.Race for concurrent steps, each branch with its own Context"))
		}
		if (x.fatal != nil || x.finished) && x.inv.Kind == protocol.InvocationStep {
			// A step invocation never ends the run: it runs its target or nothing.
			return x.end(nil), nil
		}
		if x.fatal != nil {
			return x.end([]protocol.Command{{Type: protocol.CommandRunFailed, Error: x.fatal}}), nil
		}
		if x.finished {
			return x.finish(x.terminal()), nil
		}
		if x.applyCompletion() {
			continue
		}
		pending := x.unsettled()
		x.assign(pending)
		if x.fatal != nil {
			continue
		}
		var best *call
		var bestEntry protocol.JournalEntry
		var bestSeq int64
		for _, c := range pending { // in call order: equal seqs go to the older call
			if e, seq, ok := x.recorded(c); ok && (best == nil || seq < bestSeq) {
				best, bestEntry, bestSeq = c, e, seq
			}
		}
		if best != nil {
			x.settleRecorded(best, bestEntry)
			continue
		}
		if len(pending) == 0 {
			x.setFatal(argumentError("the workflow's goroutine ended without returning (runtime.Goexit?)"))
			continue
		}
		if x.inv.Kind == protocol.InvocationStep {
			return x.stepInvocation(ctx, pending)
		}
		c := pending[0]
		if x.inv.Cancel != nil && !x.cancelThrown {
			x.cancelThrown = true
			x.resume(c, reply{err: cancelledError("the run was cancelled")})
			continue
		}
		if x.cancelThrown {
			// Only Run cleanup steps run while cancelling: the first other call, wherever it is,
			// is refused at once.
			if refused := firstOf(pending, func(p *call) bool { return p.kind != kindRun }); refused != nil {
				x.resume(refused, reply{err: cancelledError("the run is cancelling: only workflows.Run cleanup steps can run")})
				continue
			}
			// A cleanup step doesn't wait behind steps the engine already owns (an in-flight
			// parallel sibling): the next call is the oldest one the engine doesn't own.
			c = firstOf(pending, func(p *call) bool {
				e, ok := x.journal[p.stepID]
				return !ok || e.Status == protocol.StatusRetrying
			})
			if c == nil {
				return x.end(nil), nil
			}
		}
		// The deciding call: at 1.1 the oldest call with no journal entry (D2-19; calls the
		// engine owns don't hold values back); at 1.0, and while cancelling, the oldest call.
		head := c
		if x.parallel && !x.cancelThrown {
			head = nil
			for _, p := range pending {
				if _, ok := x.journal[p.stepID]; !ok {
					head = p
					break
				}
			}
		}
		if head != nil {
			if _, journaled := x.journal[head.stepID]; !journaled {
				switch head.kind {
				case kindValue:
					v, err := head.produce(x)
					if err != nil {
						return nil, err
					}
					x.outbox = append(x.outbox, protocol.Command{Type: protocol.CommandValueRecorded, StepID: head.stepID, Name: head.name, Value: v})
					x.resume(head, reply{out: v})
					continue
				case kindCallbackCreate:
					if err := x.createCallback(ctx, head); err != nil {
						return nil, err
					}
					continue
				}
			}
		}
		var inline *call
		attempt := 1
		var cmds []protocol.Command
		if x.parallel && !x.cancelThrown {
			inline, attempt, cmds = x.plan(pending)
		} else {
			inline, attempt, cmds = x.sequential(c)
		}
		if inline == nil {
			return x.finish(cmds), nil
		}
		if x.executed >= x.o.MaxSteps || !x.timeLeftOK() || x.hbCancel.Load() {
			// The budget, the deadline, or a heartbeat that reported a cancel: the next invocation
			// carries on (with the cancel).
			return x.end(nil), nil
		}
		failed, err := x.runStep(ctx, inline, attempt)
		if err != nil {
			return nil, err
		}
		if failed != nil {
			return x.end([]protocol.Command{*failed}), nil
		}
		x.resume(inline, reply{out: x.lastOutput})
	}
}

func (x *executor) entryAttempt(c *call) int {
	if e, ok := x.journal[c.stepID]; ok && e.Status == protocol.StatusRetrying {
		return e.Attempt + 1
	}
	return 1
}

func firstOf(cs []*call, pred func(*call) bool) *call {
	for _, c := range cs {
		if pred(c) {
			return c
		}
	}
	return nil
}

// plan is the frontier rule (protocol 1.1, D2-19, identical in every SDK): one actionable run
// with nothing else pending runs inline; otherwise every new run is planned together and every
// suspension sent with it, in call order. A retrying run alongside other work waits (planParallel
// skips existing steps); a running entry (a planned step at attempt 0 too) is awaited, never
// re-run.
func (x *executor) plan(pending []*call) (*call, int, []protocol.Command) {
	var runs, retrying []*call
	busy := 0
	var suspends []protocol.Command
	waited := map[string]bool{}
	for _, p := range pending {
		e, ok := x.journal[p.stepID]
		switch p.kind {
		case kindRun:
			switch {
			case !ok:
				runs = append(runs, p)
			case e.Status == protocol.StatusRetrying:
				retrying = append(retrying, p)
			default:
				busy++
			}
		case kindSleep:
			if ok {
				busy++
			} else {
				u := p.until
				suspends = append(suspends, protocol.Command{Type: protocol.CommandSleep, StepID: p.stepID, Name: p.name, Until: &u})
			}
		case kindInvoke:
			if ok {
				busy++
			} else {
				suspends = append(suspends, protocol.Command{Type: protocol.CommandInvoke, StepID: p.stepID, Name: p.name, Workflow: p.workflow, Input: p.input})
			}
		case kindCallbackWait:
			// Sent for every pending wait, even one already waiting: a no-op for the engine.
			if !waited[p.stepID] {
				waited[p.stepID] = true
				suspends = append(suspends, protocol.Command{Type: protocol.CommandWaitForCallback, StepID: p.stepID})
			}
			// value and callback creation are handled when they're the oldest new call.
		}
	}
	if busy == 0 && len(suspends) == 0 && len(runs)+len(retrying) == 1 {
		c := append(runs, retrying...)[0]
		return c, x.entryAttempt(c), nil
	}
	var cmds []protocol.Command
	if len(runs) > 0 {
		steps := make([]protocol.PlannedStep, 0, len(runs))
		for _, r := range runs {
			retry := r.retry
			ps := protocol.PlannedStep{StepID: r.stepID, Name: r.name, Retry: &retry}
			if r.timeout > 0 {
				ps.TimeoutMs = r.timeout.Milliseconds()
			}
			steps = append(steps, ps)
		}
		cmds = append(cmds, protocol.Command{Type: protocol.CommandPlanParallel, Steps: steps})
	}
	return nil, 0, append(cmds, suspends...)
}

// sequential is protocol 1.0's frontier (and every version's while cancelling): phase 1,
// exactly. Any recorded status but retrying (sleeping, running, waiting, or one a later version
// adds) means the engine is still on it: the invocation ends.
func (x *executor) sequential(c *call) (*call, int, []protocol.Command) {
	if e, ok := x.journal[c.stepID]; ok && e.Status != protocol.StatusRetrying {
		return nil, 0, nil
	}
	if c.kind == kindSleep {
		u := c.until
		return nil, 0, []protocol.Command{{Type: protocol.CommandSleep, StepID: c.stepID, Name: c.name, Until: &u}}
	}
	if c.kind != kindRun {
		return nil, 0, nil
	}
	return c, x.entryAttempt(c), nil
}

// stepInvocation runs exactly the target step, at its journal attempt + 1, and ends. It never
// records values, plans or suspends.
func (x *executor) stepInvocation(ctx context.Context, pending []*call) ([]protocol.Command, error) {
	var target *call
	for _, c := range pending {
		if c.kind == kindRun && c.stepID == x.inv.TargetStepID {
			target = c
			break
		}
	}
	if target == nil || !x.timeLeftOK() {
		return x.end(nil), nil
	}
	attempt := 1
	if e, ok := x.journal[target.stepID]; ok {
		attempt = e.Attempt + 1
	}
	failed, err := x.runStep(ctx, target, attempt)
	if err != nil {
		return nil, err
	}
	if failed != nil {
		return x.end([]protocol.Command{*failed}), nil
	}
	return x.end(nil), nil
}

func (x *executor) timeLeftOK() bool {
	total := x.o.Deadline.Sub(x.started)
	return x.o.Deadline.Sub(x.now()) > max(5*time.Second, total/10)
}

func (x *executor) checkpoint(ctx context.Context, cmds []protocol.Command) error {
	x.cpSeq++
	if cmds == nil {
		cmds = []protocol.Command{}
	}
	return x.io.Checkpoint(ctx, protocol.Checkpoint{InvocationID: x.inv.InvocationID, Seq: x.cpSeq, Commands: cmds})
}

// runStep sends stepStarted (with the outbox) and waits for its acknowledgement, then runs the
// step's function. Success records stepCompleted in the outbox (x.lastOutput is the result) and
// returns nil; failure returns the stepFailed command.
func (x *executor) runStep(ctx context.Context, c *call, attempt int) (*protocol.Command, error) {
	retry := c.retry
	start := protocol.Command{Type: protocol.CommandStepStarted, StepID: c.stepID, Name: c.name, Attempt: attempt, Retry: &retry}
	if c.timeout > 0 {
		start.TimeoutMs = c.timeout.Milliseconds()
	}
	if err := x.checkpoint(ctx, append(x.takeOutbox(), start)); err != nil {
		return nil, err // superseded, or the checkpoint failed: the step must not run
	}
	out, err := x.runFn(ctx, c, attempt)
	if err == nil {
		x.executed++
		x.lastOutput = out
		x.outbox = append(x.outbox, protocol.Command{Type: protocol.CommandStepCompleted, StepID: c.stepID, Name: c.name, Output: out})
		return nil, nil
	}
	w := encodeError(err)
	retryable := !errors.Is(err, ErrNonRetriable) && !errors.Is(err, ErrOutputTooLarge) && !isNonRetriable(w)
	cmd := protocol.Command{Type: protocol.CommandStepFailed, StepID: c.stepID, Name: c.name, Attempt: attempt, Error: w, Retryable: retryable}
	var ra *retryAfter
	if errors.As(err, &ra) {
		ms := ra.delay.Milliseconds()
		cmd.RetryAfterMs = min(int64(protocol.MaxRetryMs), max(0, ms))
	}
	return &cmd, nil
}

type stepCtx struct {
	context.Context
	attempt int
	key     string
}

func (s *stepCtx) Attempt() int           { return s.attempt }
func (s *stepCtx) IdempotencyKey() string { return s.key }

// runFn runs the step's function with its StepContext: cancelled at the step's timeout (the
// step then fails with workflows.step_timeout at once), at the invocation's deadline, and when a
// pull heartbeat reports the run is cancelling.
func (x *executor) runFn(ctx context.Context, c *call, attempt int) (json.RawMessage, error) {
	withCause, cancelCause := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelCause(nil)
	base, cancel := context.WithDeadline(withCause, x.o.Deadline)
	defer cancel()
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-x.o.Cancelled:
			// The workflow's Done closes first, so code that sees the step cancelled sees it too.
			x.hbCancel.Store(true)
			x.cancelFn()
			cancelCause(ErrCancelled) // context.Cause(sc) is ErrCancelled
		case <-ctx.Done():
			cancelCause(context.Cause(ctx))
		case <-stopWatch:
		}
	}()
	sctx := base
	timedOut := func() bool { return false }
	if c.timeout > 0 {
		var tcancel context.CancelFunc
		sctx, tcancel = context.WithTimeoutCause(base, c.timeout, ErrStepTimeout)
		defer tcancel()
		timedOut = func() bool { return context.Cause(sctx) == ErrStepTimeout }
	}
	sc := &stepCtx{Context: sctx, attempt: attempt, key: fmt.Sprintf("%s:%s:%d", x.info.ID, c.stepID, attempt)}
	type result struct {
		out json.RawMessage
		err error
	}
	done := make(chan result, 1)
	x.inStep.Store(true)
	defer x.inStep.Store(false)
	go func() {
		returned := false
		defer func() {
			switch r := recover(); {
			case r != nil:
				done <- result{err: &Error{Code: "user", Name: "panic", Message: fmt.Sprint(r)}}
			case !returned:
				// runtime.Goexit in the step's function (t.FailNow, say): the step fails.
				done <- result{err: NonRetriable(argumentError("step %q's function ended its goroutine (runtime.Goexit) without returning", c.name))}
			}
		}()
		out, err := c.fn(sc)
		returned = true
		done <- result{out, err}
	}()
	stepTimeout := func() error {
		return newError("workflows.step_timeout", "StepTimeout", "step %q ran past %v", c.name, c.timeout)
	}
	var timeoutC <-chan struct{}
	if c.timeout > 0 {
		timeoutC = sctx.Done()
	}
	for {
		select {
		case r := <-done:
			if timedOut() {
				return nil, stepTimeout()
			}
			return r.out, r.err
		case <-timeoutC:
			if timedOut() {
				return nil, stepTimeout() // the function has its cancelled context; it isn't waited for
			}
			timeoutC = nil // the deadline or a cancel: wait for the function, which has its context
		}
	}
}

func (x *executor) createCallback(ctx context.Context, c *call) error {
	shard, ok := protocol.ShardOf(x.info.ID)
	if !ok {
		x.resume(c, reply{err: argumentError("the run ID %q carries no shard; callbacks need a v7 or v8 run ID", x.info.ID)})
		return nil
	}
	tok, err := protocol.NewCallbackToken(shard, x.rand)
	if err != nil {
		return err
	}
	token := tok.String()
	cmd := protocol.Command{Type: protocol.CommandCallbackCreated, StepID: c.stepID, Name: c.name, Token: token, TimeoutMs: c.timeoutMs}
	if err := x.checkpoint(ctx, append(x.takeOutbox(), cmd)); err != nil {
		return err
	}
	// A wait later in this invocation finds the callback waiting.
	x.journal[c.stepID] = protocol.JournalEntry{Seq: math.MaxInt64, StepID: c.stepID, Name: c.name, Kind: protocol.KindCallback, Status: protocol.StatusWaiting, Attempt: 1, Token: token}
	x.resume(c, reply{out: x.callbackHandle(token)})
	return nil
}

// callbackHandle is the {URL, Token} CreateCallback returns: the URL is endpoints.api +
// "/public/v1/callbacks/" + token, the same in every SDK.
func (x *executor) callbackHandle(token string) json.RawMessage {
	out, _ := json.Marshal(struct{ URL, Token string }{x.apiBase + "/public/v1/callbacks/" + token, token})
	return out
}

func (x *executor) takeOutbox() []protocol.Command {
	out := x.outbox
	x.outbox = nil
	return out
}

func (x *executor) terminal() []protocol.Command {
	if x.resultErr == nil {
		out := x.result
		if len(out) == 0 {
			out = json.RawMessage("null")
		}
		return []protocol.Command{{Type: protocol.CommandRunCompleted, Output: out}}
	}
	cmd := protocol.Command{Type: protocol.CommandRunFailed, Error: encodeError(x.resultErr)}
	walkErrors(x.resultErr, func(e error) bool {
		if we, ok := e.(*Error); ok {
			if id, ok := x.errStep[we]; ok {
				cmd.CauseStepID = id
				return true
			}
		}
		return false
	})
	return []protocol.Command{cmd}
}

// exitWait is how long end waits for branch goroutines to end (their deferred functions run).
const exitWait = 2 * time.Second

// end answers every parked call with exit and waits (2 s at most) for the branches to end.
func (x *executor) end(cmds []protocol.Command) []protocol.Command {
	x.ended.Store(true)
	for _, c := range x.pending {
		if !c.settled {
			c.settled = true
			c.reply <- reply{exit: true}
		}
	}
	for _, g := range x.groups {
		if !g.decided && !g.parent.settled {
			g.parent.settled = true
			g.parent.reply <- reply{exit: true}
		}
	}
	out := append(x.takeOutbox(), cmds...)
	timer := time.NewTimer(exitWait)
	defer timer.Stop()
	for x.live > 0 {
		select {
		case ev := <-x.events:
			if ev.park != nil {
				if !ev.park.settled {
					ev.park.settled = true
					ev.park.reply <- reply{exit: true}
				}
				continue
			}
			if ev.done != nil {
				x.live--
			}
		case <-timer.C:
			x.logf("workflows: %d workflow goroutines didn't end within %v of the invocation's end (a deferred function blocks?)", x.live, exitWait)
			go drain(x.events, x.live)
			return out
		}
	}
	return out
}

// drain answers whatever the abandoned branches still send, so they can end, and returns once
// the last of them has.
func drain(events chan event, live int) {
	for live > 0 {
		ev := <-events
		switch {
		case ev.park != nil && !ev.park.settled:
			ev.park.settled = true
			ev.park.reply <- reply{exit: true}
		case ev.done != nil:
			live--
		}
	}
}

// finish is end for an orchestration's last commands, checked against what the engine and the
// transport accept: a plan of more than 1,000 steps, or a result past the body cap, fails the run
// non-retriably instead of being rebuilt on every redispatch.
func (x *executor) finish(cmds []protocol.Command) []protocol.Command {
	for _, c := range cmds {
		if c.Type == protocol.CommandPlanParallel && len(c.Steps) > protocol.MaxPlannedSteps {
			return x.end([]protocol.Command{{Type: protocol.CommandRunFailed, Error: encodeError(NonRetriable(fmt.Errorf(
				"the workflow started %d steps at once; at most %d can be planned together (fan out through child workflows)", len(c.Steps), protocol.MaxPlannedSteps)))}})
		}
	}
	size := 0
	for _, c := range append(append([]protocol.Command(nil), x.outbox...), cmds...) {
		if b, err := jsonx.Marshal(c); err == nil {
			size += len(b) + 1
		}
	}
	if size+1024 > x.o.MaxResultBytes {
		return x.end([]protocol.Command{{Type: protocol.CommandRunFailed, Error: encodeError(NonRetriable(fmt.Errorf(
			"the invocation's commands are %d bytes of JSON, more than the %d the transport takes (child inputs and outputs are at most 1 MiB each; fewer at once fit)", size, x.o.MaxResultBytes)))}})
	}
	return x.end(cmds)
}
