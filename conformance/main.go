// The conformance app: protocol/conformance/workflows.md in Go, served for workflows-conformance.
// CONFORMANCE_MODE=pull runs the same workflows as a pull worker against the harness's live
// endpoint (CONFORMANCE_LIVE).
package main

import (
	"log"
	"net/http"
	"os"

	workflows "github.com/ezgamehost/workflows-sdk-go"
)

func main() {
	if os.Getenv("CONFORMANCE_MODE") == "pull" {
		runWorker()
		return
	}
	h, err := workflows.Handler(workflows.HandlerOptions{Workflows: all(), SigningSecrets: []string{os.Getenv("CONFORMANCE_SECRET")}, MaxInvocationSeconds: 300, Deployment: "conformance"})
	if err != nil {
		log.Fatal(err)
	}
	addr := "127.0.0.1:" + envOr("PORT", "3100")
	log.Fatal(http.ListenAndServe(addr, h))
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
