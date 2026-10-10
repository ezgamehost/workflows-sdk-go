package workflows

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/protocol"
)

type fakeIO struct {
	mu          sync.Mutex
	checkpoints [][]protocol.Command
	answer      func(seq int) error // nil: accepted
	payloads    map[string]json.RawMessage
	pages       []protocol.Journal
}

func (f *fakeIO) Checkpoint(_ context.Context, cp protocol.Checkpoint) error {
	if err := cp.Validate(); err != nil {
		panic("invalid checkpoint: " + err.Error())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checkpoints = append(f.checkpoints, cp.Commands)
	if f.answer != nil {
		return f.answer(cp.Seq)
	}
	return nil
}

func (f *fakeIO) JournalPage(context.Context, int64) (protocol.Journal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pages) == 0 {
		return protocol.Journal{Complete: true}, nil
	}
	p := f.pages[0]
	f.pages = f.pages[1:]
	return p, nil
}

func (f *fakeIO) Payload(_ context.Context, id string) (json.RawMessage, error) {
	return f.payloads[id], nil
}

const testRunID = "01a12088-d200-73ff-8899-aabbccddeeff"

func invocation(workflow string, input any, entries ...protocol.JournalEntry) *protocol.Invocation {
	var in json.RawMessage
	if input != nil {
		in, _ = json.Marshal(input)
	}
	return &protocol.Invocation{
		ProtocolVersion: "1.1", Kind: protocol.InvocationOrchestrate, InvocationID: "inv_aaaqeayeaudaocajbifqydiob4",
		Token:     "3ff." + testRunID + ".inv_aaaqeayeaudaocajbifqydiob4.AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA",
		Endpoints: protocol.Endpoints{Live: "http://live", API: "https://api.test"},
		Run:       &protocol.RunInfo{ID: testRunID, Workflow: workflow, Input: in, Attempt: 1},
		Journal:   &protocol.Journal{Complete: true, Entries: entries},
		Deadline:  time.Now().Add(5 * time.Minute),
	}
}

func run(t *testing.T, wf Workflow, inv *protocol.Invocation, io *fakeIO, opts ...func(*execOptions)) []protocol.Command {
	t.Helper()
	o := execOptions{Deadline: inv.Deadline}
	for _, f := range opts {
		f(&o)
	}
	cmds, err := execute(context.Background(), inv, wf.definition(), io, o)
	if err != nil {
		t.Fatal(err)
	}
	res := protocol.InvocationResult{Commands: cmds}
	if err := res.Validate(); err != nil {
		t.Fatalf("the engine would refuse %s: %v", show(cmds), err)
	}
	return cmds
}

func show(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func types(cmds []protocol.Command) []string {
	out := []string{}
	for _, c := range cmds {
		out = append(out, c.Type)
	}
	return out
}

func entry(seq int64, name, kind, status string, attempt int, output string) protocol.JournalEntry {
	e := protocol.JournalEntry{Seq: seq, StepID: protocol.StepID(name, 0), Name: name, Kind: kind, Status: status, Attempt: attempt}
	if output != "" {
		e.Output = json.RawMessage(output)
	}
	return e
}

func step[T any](ctx Context, name string, v T, opts ...StepOption) (T, error) {
	return Run(ctx, name, func(StepContext) (T, error) { return v, nil }, opts...)
}

type none = json.RawMessage
