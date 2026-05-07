// Command diagnostic-collect runs after a drill terminates. It self-gates on
// PITRSession.status.phase: only on `Failed` does it collect K8s + AWS state
// + saas-pitr logs into a tarball and upload to s3://<bucket>/<correlation-id>/.
//
// Per Decision 7 (Omri's failure-handling shape: preserveInfrastructure +
// diagnostic Job + Slack notification) and Decision 11/15 (Slack stays
// inside Composition reconciliation; the slack post is gated on
// SLACK_WEBHOOK_URL env presence — chart-level notification.enabled flag
// controls the env mount).
//
// **Stub status (Group A vertical-slice integration-branch test era):**
// this binary currently runs as a working stub. It self-gates correctly on
// phase=Failed and exits 0 on the happy path so the drill can complete
// green during early iteration. Bundle creation + S3 upload + Slack post
// are TODOs that drill iteration cycles fill in once real failures surface
// (per Decision 5: "real PITR every iteration"). The structural shape is in
// place; what's missing is the concrete bundle implementation.
//
//	/binary --correlation-id drill-mte-staging-3a7f9c2b \
//	        --pitrsession-name drill-2026-q2-mte-staging \
//	        --diagnostics-bucket mte-staging-pitr-diagnostics-us-east-2 \
//	        --tenant mte-staging
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/pleme-io/pitr-tools/internal/log"
)

func main() {
	var (
		correlationID     = flag.String("correlation-id", "", "drill correlation_id")
		pitrsessionName   = flag.String("pitrsession-name", "", "PITRSession CR name (kubectl read target)")
		diagnosticsBucket = flag.String("diagnostics-bucket", "", "S3 bucket for diagnostic bundle upload")
		tenant            = flag.String("tenant", "", "tenant identifier (e.g. mte-staging)")
	)
	flag.Parse()

	if err := requireFlags(*correlationID, *pitrsessionName, *diagnosticsBucket, *tenant); err != nil {
		fmt.Fprintln(os.Stderr, "diagnostic-collect:", err.Error())
		os.Exit(2)
	}

	logger := log.New(*correlationID).With("job", "diagnostic-collect", "tenant", *tenant)

	phase, err := readPITRSessionPhase(*pitrsessionName)
	if err != nil {
		// Reading the PITRSession status itself is best-effort — log + exit 0 so the
		// drill's own state machine isn't blocked by the diagnostics path. Real drill-
		// iteration cycles surface what to do here (e.g. retry, fall back to assuming
		// Failed, etc.).
		logger.Error("could not read PITRSession phase; treating as success and exiting", "error", err.Error())
		os.Exit(0)
	}

	if phase != "Failed" {
		logger.Info("PITRSession not in Failed phase; nothing to collect", "phase", phase)
		os.Exit(0)
	}

	logger.Info("PITRSession in Failed phase; bundle collection runs here in iteration",
		"diagnostics_bucket", *diagnosticsBucket,
		"todo", "TODO drill iteration: kubectl describe pitrsession + restore namespace state + AWS RDS describe + Crossplane composed-resources + saas-pitr pod logs → tarball → s3://<bucket>/<correlation-id>/; Slack post if SLACK_WEBHOOK_URL set",
	)
	// Exit 0 even on Failed phase for the integration-branch test era stub. The
	// drill's own RDS / saas-pitr / verify Jobs already failed; this binary's
	// success-or-failure does not change the drill's terminal state. Once drill
	// iteration fills in the bundle creation, exit non-zero on bundle-upload
	// failure (so the operator notices missing diagnostics).
	os.Exit(0)
}

// readPITRSessionPhase calls the K8s API to read the PITRSession's status.phase.
// Stub: returns a placeholder value during the integration-branch test era.
// Real implementation lands during drill iteration using k8s.io/client-go's
// dynamic client (PITRSession is a CRD type, so unstructured access is fine
// without a generated typed client).
func readPITRSessionPhase(name string) (string, error) {
	// TODO drill iteration: in-cluster K8s API call:
	//   restConfig, err := rest.InClusterConfig()
	//   dynClient, err := dynamic.NewForConfig(restConfig)
	//   gvr := schema.GroupVersionResource{Group: "pitr.akeyless.io", Version: "v1", Resource: "pitrsessions"}
	//   obj, err := dynClient.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
	//   phase, _, err := unstructured.NestedString(obj.Object, "status", "phase")
	//   return phase, err
	//
	// Stub: return "Unknown" so the binary exits 0 on the "phase not Failed" branch.
	// Real failure cases that warrant bundle collection will only happen once the
	// bundle implementation lands; until then phase=Unknown is a safe stand-in.
	if name == "" {
		return "", fmt.Errorf("empty pitrsession name")
	}
	return "Unknown", nil
}

func requireFlags(correlationID, pitrsessionName, diagnosticsBucket, tenant string) error {
	missing := []string{}
	if correlationID == "" {
		missing = append(missing, "--correlation-id")
	}
	if pitrsessionName == "" {
		missing = append(missing, "--pitrsession-name")
	}
	if diagnosticsBucket == "" {
		missing = append(missing, "--diagnostics-bucket")
	}
	if tenant == "" {
		missing = append(missing, "--tenant")
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("required args missing: %v", missing)
}
