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

	if err := requireFlags(*correlationID, *sourceURL, *accessID, *configName); err != nil {
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
