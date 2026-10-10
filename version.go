// Package workflows is the Go SDK for EZGH Workflows: durable workflows that run in your own
// app, invoked by EZGH over HTTP (Handler) or pulled by a worker (NewWorker).
package workflows

import "github.com/ezgamehost/workflows-sdk-go/protocol"

// SDKName and SDKVersion go in every sync reply, invocation result and poll.
const (
	SDKName    = protocol.SDKName
	SDKVersion = "0.1.0"
)
