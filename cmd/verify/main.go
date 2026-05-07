// Command verify confirms canary (or operator-supplied) secrets exist in
// the RESTORED akeyless deployment after the RDS PITR restore + saas-pitr
// workload deployment have completed.
//
// This is the critical verification step in the drill: it proves the
// restored env actually carries the data committed up to the requested
// restoreTime. Per Decision 19 (Job runtime KCL+Go) + Decision 5 ("real
// PITR every iteration; favor highest certainty").
//
//	/binary --correlation-id drill-mte-staging-3a7f9c2b \
//	        --restored-akeyless-url https://saas-pitr-uam-service-ingress.restore-3a7f9c2b.svc.cluster.local:443 \
//	        --akeyless-access-id p-abc \
//	        --k8s-auth-config mte-staging-pitr \
//	        --secret-paths ""        # empty = use auto-generated canary path
//
// Behavior:
//
//   - Polls the restored akeyless URL until it accepts auth (saas-pitr Helm
//     release + RDS PITR restore can take 15-25 min — wait that long).
//   - For each secret path: DescribeItem; non-existent → exit non-zero.
//   - Exit 0 only when every requested path is found in the restored env.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pleme-io/pitr-tools/internal/akeyless"
	"github.com/pleme-io/pitr-tools/internal/correlation"
	"github.com/pleme-io/pitr-tools/internal/log"
)

// Default poll loop bounds. Tuned for the saas-pitr Helm release coming up
// after RDS PITR restore: typical ready-time 10-20 min.
const (
	defaultMaxWait      = 25 * time.Minute
	defaultPollInterval = 30 * time.Second
)

func main() {
	var (
		correlationID = flag.String("correlation-id", "", "drill correlation_id")
		restoredURL   = flag.String("restored-akeyless-url", "", "restored akeyless gateway URL (in the restore-* namespace)")
		accessID      = flag.String("akeyless-access-id", "", "akeyless k8s auth method access ID (same as source — restored env is a snapshot)")
		configName    = flag.String("k8s-auth-config", "", "akeyless k8s auth config name (same as source)")
		secretPaths   = flag.String("secret-paths", "", "comma-separated akeyless paths to verify; empty = auto-canary path /drill-canary/<hash>")
		maxWait       = flag.Duration("max-wait", defaultMaxWait, "max time to wait for the restored akeyless to come up")
		pollInterval  = flag.Duration("poll-interval", defaultPollInterval, "poll interval while waiting for the restored akeyless to come up")
	)
	flag.Parse()

	if err := requireFlags(*correlationID, *restoredURL, *accessID, *configName); err != nil {
		fmt.Fprintln(os.Stderr, "verify:", err.Error())
		os.Exit(2)
	}

	logger := log.New(*correlationID).With("job", "verify")

	paths, err := resolveSecretPaths(*correlationID, *secretPaths)
	if err != nil {
		logger.Error("resolve secret paths", "error", err.Error())
		os.Exit(2)
	}
	logger.Info("verify scope", "paths", paths)

	ctx, cancel := context.WithTimeout(context.Background(), *maxWait+5*time.Minute)
	defer cancel()

	client, err := pollUntilAuthSucceeds(ctx, logger, *restoredURL, *accessID, *configName, *maxWait, *pollInterval)
	if err != nil {
		logger.Error("restored akeyless never authenticated", "max_wait", maxWait.String(), "error", err.Error())
		os.Exit(1)
	}

	failed := []string{}
	for _, p := range paths {
		if err := client.DescribeItem(ctx, p); err != nil {
			logger.Error("secret missing in restored env", "path", p, "error", err.Error())
			failed = append(failed, p)
			continue
		}
		logger.Info("secret found", "path", p)
	}

	if len(failed) > 0 {
		logger.Error("verify failed; secrets missing from restored env", "missing", failed)
		os.Exit(1)
	}
	logger.Info("verify succeeded; all requested secrets present in restored env", "count", len(paths))
}

// pollUntilAuthSucceeds attempts NewClient repeatedly until it succeeds or
// the maxWait elapses. The restored akeyless deployment isn't reachable
// until saas-pitr's Helm release reconciles + the restored RDS endpoint
// accepts connections — empirically 10-20 min for first-iteration drills.
func pollUntilAuthSucceeds(
	ctx context.Context,
	logger interface{ Info(string, ...any) },
	url, accessID, configName string,
	maxWait, interval time.Duration,
) (*akeyless.Client, error) {
	deadline := time.Now().Add(maxWait)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := akeyless.NewClient(ctx, akeyless.AuthConfig{
			GatewayURL: url,
			AccessID:   accessID,
			ConfigName: configName,
		})
		if err == nil {
			return client, nil
		}
		lastErr = err
		logger.Info("restored akeyless not yet ready; retrying", "interval", interval.String())
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context canceled while waiting: %w", ctx.Err())
		case <-time.After(interval):
		}
	}
	if lastErr == nil {
		lastErr = errors.New("max wait elapsed")
	}
	return nil, fmt.Errorf("after %s: %w", maxWait, lastErr)
}

// resolveSecretPaths returns the akeyless paths to verify. Empty input means
// "use the auto-generated canary path derived from the correlation_id"; a
// non-empty input is a comma-separated explicit list.
func resolveSecretPaths(correlationID, raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		hash := correlation.HashFromID(correlationID)
		if hash == "" {
			return nil, fmt.Errorf("invalid correlation-id format: %q", correlationID)
		}
		return []string{correlation.CanaryPath(hash)}, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("secret-paths is non-empty but contains only empty entries")
	}
	return out, nil
}

func requireFlags(correlationID, restoredURL, accessID, configName string) error {
	missing := []string{}
	if correlationID == "" {
		missing = append(missing, "--correlation-id")
	}
	if restoredURL == "" {
		missing = append(missing, "--restored-akeyless-url")
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
