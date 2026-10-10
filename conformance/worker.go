package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	workflows "github.com/ezgamehost/workflows-sdk-go"
)

// runWorker serves the same workflows as a pull worker for the harness's pull scenarios, with
// protocol/conformance/workflows.md's settings.
func runWorker() {
	w, err := workflows.NewWorker(workflows.WorkerOptions{
		APIKey:            envOr("CONFORMANCE_API_KEY", "conformance-key"),
		OrganizationID:    "0199a3c2-5b1e-7d40-9f3a-2c8e6b1d4f70",
		ProjectID:         "0199a3c2-6c2f-7e51-8a4b-3d9f7c2e5a81",
		App:               envOr("CONFORMANCE_PULL_APP", "0199a3c2-7d30-7f62-9b5c-4eaf8d3f6b92"),
		Queue:             envOr("CONFORMANCE_PULL_QUEUE", "conformance"),
		Workflows:         all(),
		MaxTasks:          4,
		MaxConcurrent:     4,
		LiveURL:           envOr("CONFORMANCE_LIVE", "http://127.0.0.1:3300"),
		Deployment:        "conformance",
		HeartbeatInterval: 300 * time.Millisecond,
		MaxPollBackoff:    time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := w.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
