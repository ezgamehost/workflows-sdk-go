package workflows

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

const testQueueID = "0199a3d0-1111-7000-8000-00000000aaaa"

func v12(workflow string, entries ...protocol.JournalEntry) *protocol.Invocation {
	inv := invocation(workflow, nil, entries...)
	inv.ProtocolVersion = "1.2"
	return inv
}

func messageEntry(status string) protocol.JournalEntry {
	return protocol.JournalEntry{Seq: 2, CreatedSeq: 1, StepID: protocol.StepID("paid", 0), Name: "paid", Kind: protocol.KindMessage,
		Status: status, Attempt: 1, QueueID: testQueueID, Key: "order:o-1"}
}

const testMessage = `{"messageId":"0199a3d0-2222-7000-8000-000000000001","body":"paid","attributes":{"orderId":"order:o-1"},"sentAt":"2026-10-13T09:00:00.123Z","queueId":"0199a3d0-1111-7000-8000-00000000aaaa"}`

func waitMessageWF(caught *error) Workflow {
	return New("waitmessage", func(ctx Context, _ none) (map[string]any, error) {
		m, ok, err := WaitForMessage(ctx, "paid", WaitForMessageOptions{QueueID: testQueueID, Key: "order:o-1", Timeout: time.Hour})
		if err != nil {
			if caught != nil {
				*caught = err
			}
			return nil, err
		}
		if !ok {
			return map[string]any{"received": false}, nil
		}
		return map[string]any{"received": true, "body": m.Body, "messageId": m.MessageID, "sentAt": m.SentAt.UTC().Format(time.RFC3339Nano)}, nil
	})
}

func TestWaitForMessageCreatesThenSuspends(t *testing.T) {
	io := &fakeIO{}
	cmds := run(t, waitMessageWF(nil), v12("waitmessage"), io)
	if len(io.checkpoints) != 1 || len(io.checkpoints[0]) != 1 {
		t.Fatalf("checkpoints %s", show(io.checkpoints))
	}
	c := io.checkpoints[0][0]
	if c.Type != "messageWaitCreated" || c.StepID != protocol.StepID("paid", 0) || c.Name != "paid" || c.QueueID != testQueueID || c.Key != "order:o-1" || c.TimeoutMs != 3_600_000 {
		t.Fatalf("created %s", show(c))
	}
	if len(cmds) != 1 || cmds[0].Type != "waitForMessage" || cmds[0].StepID != protocol.StepID("paid", 0) {
		t.Fatalf("commands %s", show(cmds))
	}
}

func TestWaitForMessageReplays(t *testing.T) {
	// waiting: suspends again, without a second messageWaitCreated.
	io := &fakeIO{}
	waiting := messageEntry("waiting")
	waiting.Seq = 1
	cmds := run(t, waitMessageWF(nil), v12("waitmessage", waiting), io)
	if len(io.checkpoints) != 0 || len(cmds) != 1 || cmds[0].Type != "waitForMessage" {
		t.Fatalf("waiting: %s %s", show(io.checkpoints), show(cmds))
	}
	// completed: the message.
	done := messageEntry("completed")
	done.Output = json.RawMessage(testMessage)
	cmds = run(t, waitMessageWF(nil), v12("waitmessage", done), &fakeIO{})
	if got := string(cmds[0].Output); len(cmds) != 1 || got != `{"body":"paid","messageId":"0199a3d0-2222-7000-8000-000000000001","received":true,"sentAt":"2026-10-13T09:00:00.123Z"}` {
		t.Fatalf("completed: %s", show(cmds))
	}
	// timedOut: no message, no error.
	timedOut := messageEntry("completed")
	timedOut.TimedOut = true
	cmds = run(t, waitMessageWF(nil), v12("waitmessage", timedOut), &fakeIO{})
	if len(cmds) != 1 || string(cmds[0].Output) != `{"received":false}` {
		t.Fatalf("timed out: %s", show(cmds))
	}
	// An offloaded message is fetched from the payload store.
	ref := messageEntry("completed")
	ref.OutputRef = "pay_aaaaaaaaaaaaaaaaaaaaaaaaaa"
	cmds = run(t, waitMessageWF(nil), v12("waitmessage", ref), &fakeIO{payloads: map[string]json.RawMessage{ref.OutputRef: json.RawMessage(testMessage)}})
	if len(cmds) != 1 || !strings.Contains(string(cmds[0].Output), `"received":true`) {
		t.Fatalf("outputRef: %s", show(cmds))
	}
}

func TestWaitForMessageCancelled(t *testing.T) {
	var caught error
	e := messageEntry("cancelled")
	e.Error = &protocol.WireError{Code: "workflows.cancelled", Name: "CancelledError", Message: "The run was cancelled"}
	inv := v12("waitmessage", e)
	inv.Cancel = &protocol.Cancel{RequestedAt: time.Now(), GraceUntil: time.Now().Add(time.Hour)}
	cmds := run(t, waitMessageWF(&caught), inv, &fakeIO{})
	if !errors.Is(caught, ErrCancelled) || len(cmds) != 1 || cmds[0].Type != "runFailed" || cmds[0].Error.Code != "workflows.cancelled" {
		t.Fatalf("%v %s", caught, show(cmds))
	}
}

func TestWaitForMessageBesideARun(t *testing.T) {
	wf := New("both", func(ctx Context, _ none) (int, error) {
		err := Parallel(ctx,
			func(c Context) error { _, err := step(c, "work", 1); return err },
			func(c Context) error {
				_, _, err := WaitForMessage(c, "paid", WaitForMessageOptions{QueueID: testQueueID, Key: "k", Timeout: time.Second})
				return err
			},
		)
		return 0, err
	})
	io := &fakeIO{}
	cmds := run(t, wf, v12("both"), io)
	var types []string
	for _, cp := range io.checkpoints {
		for _, c := range cp {
			types = append(types, c.Type)
		}
	}
	if strings.Join(types, ",") != "stepStarted,stepCompleted,messageWaitCreated" || len(cmds) != 1 || cmds[0].Type != "waitForMessage" {
		t.Fatalf("checkpoints %v, commands %s", types, show(cmds))
	}
}

func TestWaitForMessageNeeds12AndValidOptions(t *testing.T) {
	inv := v12("waitmessage")
	inv.ProtocolVersion = "1.1"
	cmds := run(t, waitMessageWF(nil), inv, &fakeIO{})
	if len(cmds) != 1 || cmds[0].Type != "runFailed" || cmds[0].Error.Code != "workflows.non_retriable" || !strings.Contains(cmds[0].Error.Message, "needs protocol 1.2") {
		t.Fatalf("1.1: %s", show(cmds))
	}
	for _, o := range []WaitForMessageOptions{
		{QueueID: "que_abc", Key: "k", Timeout: time.Minute},
		{QueueID: testQueueID, Key: "has space", Timeout: time.Minute},
		{QueueID: testQueueID, Key: "k", Timeout: 500 * time.Millisecond},
		{QueueID: testQueueID, Key: "k", Timeout: MaxMessageWait + time.Second},
	} {
		wf := New("bad", func(ctx Context, _ none) (any, error) {
			_, _, err := WaitForMessage(ctx, "w", o)
			return nil, err
		})
		cmds := run(t, wf, v12("bad"), &fakeIO{})
		if len(cmds) != 1 || cmds[0].Type != "runFailed" || cmds[0].Error.Name != "TypeError" {
			t.Fatalf("%+v: %s", o, show(cmds))
		}
	}
}
