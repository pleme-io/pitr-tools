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
	"github.com/pleme-io/pitr-tools/internal/result"
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
		restoredURL   = flag.String("restored-akeyless-url", "", "restored akeyless gateway URL — used for /auth (typically the auth microservice service ingress in restore-* ns)")
		uamURL        = flag.String("restored-uam-url", "", "restored akeyless UAM service URL — used for /describe-item (typically the uam microservice service ingress in restore-* ns); empty = use restored-akeyless-url for both")
		accessID      = flag.String("akeyless-access-id", "", "akeyless k8s auth method access ID (same as source — restored env is a snapshot)")
		configName    = flag.String("k8s-auth-config", "", "akeyless k8s auth config name (same as source)")
		secretPaths   = flag.String("secret-paths", "", "comma-separated akeyless paths to verify; empty = auto-canary path /drill-canary/<hash>")
		maxWait       = flag.Duration("max-wait", defaultMaxWait, "max time to wait for the restored akeyless to come up")
		pollInterval  = flag.Duration("poll-interval", defaultPollInterval, "poll interval while waiting for the restored akeyless to come up")
	)
	flag.Parse()

	// Auth mode dispatch:
	//   AKEYLESS_ACCESS_KEY env set → api-key auth (smoke / pre-coordination)
	//   --k8s-auth-config flag set → k8s auth method (production / ASM-18083)
	// Either is valid; both unset is an error. The cluster-side Job is
	// expected to inject AKEYLESS_ACCESS_KEY via a Secret-mounted env var
	// (NEVER argv) when running in api-key mode.
	apiKey := os.Getenv("AKEYLESS_ACCESS_KEY")
	if apiKey == "" && *configName == "" {
		fmt.Fprintln(os.Stderr, "verify: must set either --k8s-auth-config (k8s mode) OR AKEYLESS_ACCESS_KEY env (api-key mode)")
		os.Exit(2)
	}
	if err := requireFlagsRelaxed(*correlationID, *restoredURL, *accessID); err != nil {
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

	authClient, err := pollUntilAuthSucceeds(ctx, logger, *restoredURL, *accessID, *configName, apiKey, *maxWait, *pollInterval)
	if err != nil {
		logger.Error("restored akeyless never authenticated", "max_wait", maxWait.String(), "error", err.Error())
		os.Exit(1)
	}

	// Akeyless production routes API paths via an Ingress (e.g. /auth →
	// auth microservice, /describe-item → uam microservice). Per-microservice
	// saas-pitr Pods don't have that router. So auth happened against the
	// auth-svc URL above; item lookups now run against the uam-svc URL with
	// the same session token. When --restored-uam-url is empty, fall back
	// to the auth URL (legacy behavior + tests against environments with a
	// unified gateway).
	itemClient := authClient
	if *uamURL != "" && *uamURL != *restoredURL {
		itemClient = authClient.WithGatewayURL(*uamURL)
		logger.Info("split-URL routing: auth via auth-svc, items via uam-svc", "auth_url", *restoredURL, "uam_url", *uamURL)
	}

	retrieved := []string{}
	missing := []string{}
	for _, p := range paths {
		if err := itemClient.DescribeItem(ctx, p); err != nil {
			logger.Error("secret missing in restored env", "path", p, "error", err.Error())
			missing = append(missing, p)
			continue
		}
		retrieved = append(retrieved, p)
		logger.Info("secret found", "path", p)
	}

	// Persist outcome for the chart's status.retrievedSecrets[] composition
	// ref to consume, regardless of pass/fail (partial-failure case still
	// surfaces what was retrieved). ConfigMap write failures do NOT change
	// the Job's exit code — verify already succeeded or failed on its own
	// merits; the result-write is best-effort observability that the chart
	// can poll-then-give-up if it never lands.
	phase := result.PhaseSucceeded
	if len(missing) > 0 {
		phase = result.PhaseFailed
	}
	cmName, err := result.WriteConfigMap(ctx, os.Getenv("POD_NAMESPACE"), result.Outcome{
		CorrelationID:    *correlationID,
		RetrievedSecrets: retrieved,
		MissingSecrets:   missing,
		Phase:            phase,
	})
	if err != nil {
		logger.Error("write result configmap (verify Job's stdout still authoritative)", "error", err.Error())
	} else {
		logger.Info("result configmap written", "configmap", cmName, "retrieved_count", len(retrieved), "missing_count", len(missing))
	}

	if len(missing) > 0 {
		logger.Error("verify failed; secrets missing from restored env", "missing", missing)
		os.Exit(1)
	}
	logger.Info("verify succeeded; all requested secrets present in restored env", "count", len(paths))
}

// pollUntilAuthSucceeds attempts NewClient repeatedly until it succeeds or
// the maxWait elapses. The restored akeyless deployment isn't reachable
// until saas-pitr's Helm release reconciles + the restored RDS endpoint
// accepts connections — empirically 10-20 min for first-iteration drills.
//
// Auth mode chosen by which credential is non-empty:
//   - apiKey set     → access_key auth (admin / smoke)
//   - configName set → k8s auth method (production / Decision 13)
func pollUntilAuthSucceeds(
	ctx context.Context,
	logger interface{ Info(string, ...any) },
	url, accessID, configName, apiKey string,
	maxWait, interval time.Duration,
) (*akeyless.Client, error) {
	deadline := time.Now().Add(maxWait)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := akeyless.NewClient(ctx, akeyless.AuthConfig{
			GatewayURL: url,
			AccessID:   accessID,
			ConfigName: configName,
			AccessKey:  apiKey,
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

// requireFlagsRelaxed validates the always-required args. The auth mode
// dispatch (k8s vs api-key) is checked separately by the caller.
func requireFlagsRelaxed(correlationID, restoredURL, accessID string) error {
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
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("required args missing: %v", missing)
}
