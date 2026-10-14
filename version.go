// Package workflows is the Go SDK for EZGH Workflows: durable workflows that run in your own
// app, invoked by EZGH over HTTP (Handler) or pulled by a worker (NewWorker).
package workflows

// SDKName and SDKVersion go in every sync reply, invocation result and poll.
const (
	SDKName    = "github.com/ezgamehost/workflows-sdk-go"
	SDKVersion = "0.3.0"
)
