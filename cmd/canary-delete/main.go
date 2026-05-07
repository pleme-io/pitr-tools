// Command canary-delete deletes the drill canary secret from the SOURCE
// akeyless deployment.
//
// Per Decision 19 + Conventions Section 6. Idempotent: 404 on delete is a
// success path (canary already gone — internal/akeyless.Client.DeleteItem
// handles this). Same arg shape as canary-create.
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
		correlationID = flag.String("correlation-id", "", "drill correlation_id")
		sourceURL     = flag.String("source-akeyless-url", "", "source akeyless gateway URL")
		accessID      = flag.String("akeyless-access-id", "", "akeyless k8s auth method access ID")
		configName    = flag.String("k8s-auth-config", "", "akeyless k8s auth config name")
	)
	flag.Parse()

	if err := requireFlags(*correlationID, *sourceURL, *accessID, *configName); err != nil {
		fmt.Fprintln(os.Stderr, "canary-delete:", err.Error())
		os.Exit(2)
	}

	logger := log.New(*correlationID).With("job", "canary-delete")

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

	if err := client.DeleteItem(ctx, canaryPath); err != nil {
		logger.Error("delete-item failed", "path", canaryPath, "error", err.Error())
		os.Exit(1)
	}
	logger.Info("canary deleted (idempotent — 404 treated as success)", "path", canaryPath)
}

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
