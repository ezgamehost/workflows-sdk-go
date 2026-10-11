// Creates the e2e push and pull apps through the Client (which retries the cold engine's 503s
// with the same Idempotency-Key) and prints "<pushAppId> <signingSecret> <pullAppId>".
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	workflows "github.com/ezgamehost/workflows-sdk-go"
)

func main() {
	ctx := context.Background()
	c, err := workflows.NewClient(workflows.ClientOptions{APIKey: os.Getenv("E2E_API_KEY"), OrganizationID: os.Getenv("E2E_ORG"), ProjectID: os.Getenv("E2E_PROJECT"),
		BaseURL: envOr("E2E_API", "http://127.0.0.1:6100"), Retries: new(90), MaxRetryDelay: 2 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	stamp := time.Now().UnixNano()
	push, err := c.CreateApp(ctx, fmt.Sprintf("e2e-go-%d", stamp), workflows.CreateAppOptions{Endpoint: envOr("E2E_APP", "http://127.0.0.1:3200/")})
	if err != nil {
		log.Fatal(err)
	}
	if push.SigningSecret == nil {
		log.Fatal("CreateApp returned no signing secret")
	}
	pullID := "-"
	if os.Getenv("E2E_PHASE1_ONLY") != "1" {
		pull, err := c.CreateApp(ctx, fmt.Sprintf("e2e-go-pull-%d", stamp), workflows.CreateAppOptions{Kind: workflows.AppKindPull, Queue: "e2e"})
		if err != nil {
			log.Fatal(err)
		}
		pullID = pull.App.ID
	}
	fmt.Println(push.App.ID, *push.SigningSecret, pullID)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
