package workflows

import (
	"testing"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

func TestReplayOrderVector(t *testing.T) {
	var f struct {
		Workflow       string
		Journal        protocol.Journal
		ExpectNextStep string
	}
	loadVector(t, "replay-order.json", &f)
	if f.Workflow != "race" {
		t.Fatalf("workflow %q", f.Workflow)
	}
	for i := 0; i < 20; i++ {
		inv := invocation("race", nil, f.Journal.Entries...)
		io := &fakeIO{}
		run(t, raceWF(), inv, io)
		if len(io.checkpoints) == 0 || io.checkpoints[0][0].StepID != protocol.StepID(f.ExpectNextStep, 0) {
			t.Fatalf("next step %s, want %s", show(io.checkpoints), f.ExpectNextStep)
		}
	}
}
