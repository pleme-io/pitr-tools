// Command canary-create creates the drill canary secret on the SOURCE
// akeyless deployment.
//
// Per Decision 19 (Job runtime — KCL emits Composition graph as before, but
// Job command bodies are Go binaries) + Conventions Section 6 (k8s auth
// method): authenticates via the akeyless k8s auth method using the Job's
// projected SA token, then calls CreateSecret on the canary path. The
// canary value is the correlation_id itself — deterministic, non-secret-
// shaped, and re-derivable for verify.
//
//	/binary --correlation-id drill-mte-staging-3a7f9c2b \
//	        --source-akeyless-url https://saas-uam-service-ingress.default.svc.cluster.local:443 \
//	        --akeyless-access-id p-abc123 \
//	        --k8s-auth-config mte-staging-pitr
//
// The canary secret survives the RDS PITR restore and the verify Job later
// reads it from the RESTORED akeyless deployment to confirm the restore
// captured both the secret store AND the binlog up to the requested
// restoreTime.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/pleme-io/pitr-tools/internal/akeyless"
	"github.com/pleme-io/pitr-tools/internal/correlation"
	"github.com/pleme-io/pitr-tools/internal/log"
)

const operationTimeout = 60 * time.Second

func main() {
	var (
		correlationID = flag.String("correlation-id", "", "drill correlation_id (e.g. drill-mte-staging-3a7f9c2b)")
		sourceURL     = flag.String("source-akeyless-url", "", "source akeyless gateway URL")
		accessID      = flag.String("akeyless-access-id", "", "akeyless k8s auth method access ID (per-tenant)")
		configName    = flag.String("k8s-auth-config", "", "akeyless k8s auth config name (per-tenant)")
	)
	flag.Parse()

	// Auth mode dispatch (mirrors verify's pattern):
	//   AKEYLESS_ACCESS_KEY env set → api-key auth (smoke / pre-coordination)
	//   --k8s-auth-config flag set → k8s auth method (production / ASM-18083)
	// Either is valid; both unset is an error. The cluster-side Job is expected
	// to inject AKEYLESS_ACCESS_KEY via a Secret-mounted env var (NEVER argv)
	// when running in api-key mode.
	apiKey := os.Getenv("AKEYLESS_ACCESS_KEY")
	if apiKey == "" && *configName == "" {
		fmt.Fprintln(os.Stderr, "canary-create: must set either --k8s-auth-config (k8s mode) OR AKEYLESS_ACCESS_KEY env (api-key mode)")
		os.Exit(2)
	}
	if err := requireFlagsRelaxed(*correlationID, *sourceURL, *accessID); err != nil {
		fmt.Fprintln(os.Stderr, "canary-create:", err.Error())
		os.Exit(2)
	}

	logger := log.New(*correlationID).With("job", "canary-create")

	hash := correlation.HashFromID(*correlationID)
	if hash == "" {
		logger.Error("invalid correlation-id format", "correlation_id", *correlationID)
		os.Exit(2)
	}
	canaryPath := correlation.CanaryPath(hash)

	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()

	client, err := akeyless.NewClient(ctx, akeyless.AuthConfig{
		GatewayURL: *sourceURL,
		AccessID:   *accessID,
		ConfigName: *configName,
		AccessKey:  apiKey,
	})
	if err != nil {
		logger.Error("akeyless k8s auth failed", "error", err.Error())
		os.Exit(1)
	}

	// Canary value = correlation_id. Deterministic + verify can re-derive it
	// from the same correlation_id without out-of-band coordination.
	canaryValue := *correlationID

	if err := client.CreateSecret(ctx, canaryPath, canaryValue); err != nil {
		logger.Error("create-secret failed", "path", canaryPath, "error", err.Error())
		os.Exit(1)
	}
	logger.Info("canary created", "path", canaryPath)
}

// requireFlags returns a single error listing any missing required flags.
// Extracted as a pure function so main_test.go can exercise the input
// contract without an akeyless server.
//
// Strict (k8s mode) — kept for backwards-compatible test coverage of the
// k8s-auth code path. main() now calls requireFlagsRelaxed and dispatches
// the auth-mode check separately so api-key mode is also accepted.
func requireFlags(correlationID, sourceURL, accessID, configName string) error {
	missing := []string{}
	if correlationID == "" {
		missing = append(missing, "--correlation-id")
	}
	if sourceURL == "" {
		missing = append(missing, "--source-akeyless-url")
	}
	if accessID == "" {
		missing = append(missing, "--akeyless-access-id")
	}
	if configName == "" {
		missing = append(missing, "--k8s-auth-config")
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("required args missing: %v", missing)
}

// requireFlagsRelaxed validates the always-required args. The auth mode
// (k8s vs api-key) is dispatched separately in main().
func requireFlagsRelaxed(correlationID, sourceURL, accessID string) error {
	missing := []string{}
	if correlationID == "" {
		missing = append(missing, "--correlation-id")
	}
	if sourceURL == "" {
		missing = append(missing, "--source-akeyless-url")
	}
	if accessID == "" {
		missing = append(missing, "--akeyless-access-id")
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("required args missing: %v", missing)
}
