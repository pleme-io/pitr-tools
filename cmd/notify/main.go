// Command notify posts a PITR drill phase notification to Slack.
//
// Used by pitr-akeyless's Crossplane Composition: the KCL function emits a
// kubernetes.crossplane.io/v1alpha1.Object wrapping a Job that runs this
// binary. The Composition reconciliation graph treats the Object as Ready
// only when the Job completes successfully — any Slack post failure (5xx,
// network unreachable, missing webhook secret) cascades to drill phase=
// Failed. This is the 1:1 reconciliation property of Decision 15.
//
// SLACK_WEBHOOK_URL is read from env (mounted by the KCL Job spec from a K8s
// Secret synced via ESO from akeyless per Decision 11). All other inputs
// flow as flag args so the KCL emission is structured and unit-testable:
//
//	/binary --correlation-id drill-mte-staging-3a7f9c2b \
//	        --phase Succeeded \
//	        --tenant mte-staging \
//	        --bundle-url https://...        # optional; only on Failed
//
// Phase values match the XRD (apiVersion pitr.akeyless.io/v1) status.phase
// enum (Conventions Section 5): Started, Succeeded, Failed, Canceled.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/pleme-io/pitr-tools/internal/log"
	"github.com/pleme-io/pitr-tools/internal/slack"
)

func main() {
	var (
		correlationID = flag.String("correlation-id", "", "drill correlation_id (e.g. drill-mte-staging-3a7f9c2b)")
		phase         = flag.String("phase", "", "drill phase: Started|Succeeded|Failed|Canceled")
		tenant        = flag.String("tenant", "", "tenant identifier (e.g. mte-staging)")
		bundleURL     = flag.String("bundle-url", "", "S3 URL of diagnostic bundle (only on phase=Failed)")
	)
	flag.Parse()

	if *correlationID == "" || *phase == "" || *tenant == "" {
		fmt.Fprintln(os.Stderr, "notify: --correlation-id, --phase, --tenant are required")
		os.Exit(2)
	}

	logger := log.New(*correlationID).With("phase", *phase, "tenant", *tenant)

	webhookURL := os.Getenv("SLACK_WEBHOOK_URL")
	if webhookURL == "" {
		// Per Decision 11 test-era flag: when chart-level notification.enabled=false,
		// the KCL emission omits the env var mount AND skips the notify Job emission
		// entirely. Reaching this point with an empty env var is a chart wiring bug
		// (notify Job emitted with notifications disabled). Surface as error so the
		// drill phase=Failed flushes the bug to the operator.
		logger.Error("SLACK_WEBHOOK_URL not set; chart wiring bug — notify Job emitted with notifications disabled?")
		os.Exit(1)
	}

	msg := slack.Message{Text: composeMessage(*phase, *correlationID, *tenant, *bundleURL)}
	if err := slack.New(webhookURL).Post(msg); err != nil {
		logger.Error("slack post failed", "error", err.Error())
		os.Exit(1)
	}
	logger.Info("slack post succeeded")
}

// composeMessage builds the Slack text for a drill phase notification. Kept
// simple for v1; richer block formatting (per-phase color, links to Grafana
// dashboards, etc.) is a future enhancement once drill iteration validates
// what the on-call eye actually wants.
func composeMessage(phase, correlationID, tenant, bundleURL string) string {
	emoji := phaseEmoji(phase)
	base := fmt.Sprintf("%s PITR drill *%s* — tenant `%s`, correlation `%s`", emoji, phase, tenant, correlationID)
	if phase == "Failed" && bundleURL != "" {
		base += fmt.Sprintf("\n📦 diagnostic bundle: %s", bundleURL)
	}
	return base
}

func phaseEmoji(phase string) string {
	switch phase {
	case "Started":
		return ":hourglass_flowing_sand:"
	case "Succeeded":
		return ":white_check_mark:"
	case "Failed":
		return ":x:"
	case "Canceled":
		return ":warning:"
	default:
		return ":information_source:"
	}
}
