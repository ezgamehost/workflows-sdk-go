package workflows

import (
	"strings"
	"testing"
)

func TestDetectDeployment(t *testing.T) {
	for _, k := range []string{"VERCEL_DEPLOYMENT_ID", "AWS_LAMBDA_FUNCTION_NAME", "AWS_LAMBDA_FUNCTION_VERSION", "K_REVISION", "FLY_MACHINE_VERSION", "RENDER_GIT_COMMIT"} {
		t.Setenv(k, "")
	}
	if got := detectDeployment(); got != "" {
		t.Fatalf("none: %q", got)
	}
	t.Setenv("AWS_LAMBDA_FUNCTION_NAME", "fn")
	t.Setenv("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST")
	if got := detectDeployment(); got != "" {
		t.Fatalf("$LATEST: %q", got)
	}
	t.Setenv("AWS_LAMBDA_FUNCTION_VERSION", "7")
	if got := detectDeployment(); got != "fn:7" {
		t.Fatalf("lambda: %q", got)
	}
	t.Setenv("VERCEL_DEPLOYMENT_ID", "dpl_"+strings.Repeat("x", 300))
	if got := detectDeployment(); len(got) != 256 || !strings.HasPrefix(got, "dpl_") {
		t.Fatalf("vercel: %d", len(got))
	}
}
