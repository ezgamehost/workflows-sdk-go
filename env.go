package workflows

import (
	"os"

	"github.com/ezgamehost/workflows-sdk-go/internal/protocol"
)

// detectDeployment is the platform's deployment ID, recorded with each invocation (design,
// "Runs continue on whatever code is deployed"): Vercel, AWS Lambda (function:version, but not
// $LATEST), Cloud Run, Fly.io and Render.
func detectDeployment() string {
	if v := os.Getenv("VERCEL_DEPLOYMENT_ID"); v != "" {
		return protocol.CleanDeployment(v)
	}
	if fn, v := os.Getenv("AWS_LAMBDA_FUNCTION_NAME"), os.Getenv("AWS_LAMBDA_FUNCTION_VERSION"); fn != "" && v != "" && v != "$LATEST" {
		return protocol.CleanDeployment(fn + ":" + v)
	}
	for _, k := range []string{"K_REVISION", "FLY_MACHINE_VERSION", "RENDER_GIT_COMMIT"} {
		if v := os.Getenv(k); v != "" {
			return protocol.CleanDeployment(v)
		}
	}
	return ""
}
