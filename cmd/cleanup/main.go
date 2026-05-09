// Command cleanup is the substrate-side enforcer of the PITRSession's
// `failureHandling` interface. It runs as the last step in every drill
// (success OR failure), reads the verify Job's terminal outcome, reads
// the operator's spec.failureHandling.preserve choice, and acts:
//
//	verify phase == Succeeded                       → sleep retentionAfterSuccess, then teardown
//	verify phase == Failed AND preserve == false   → teardown immediately (after diagnostic-collect)
//	verify phase == Failed AND preserve == true    → leave infra up, mark cleanup=Skipped, exit
//
// "Teardown" here means the per-drill DESTRUCTIVE resources (the ones
// that cost money or pin AWS state):
//   - rds.aws.upbound.io Instance MRs labeled with this drill correlation_id
//   - the per-drill restore-<short_hash> namespace (cascade-deletes saas
//     Pods, Deployments, Services, ConfigMaps, RBAC inside)
//   - akeyless drill identity MRs (auth method, role, etc.) when those
//     land via crossplane-akeyless
//
// Composition-level housekeeping (the chart's own Object MRs in
// pitr-akeyless namespace — verify Job, cleanup Job itself, etc.)
// stays around until the operator deletes the PITRSession; those are
// cheap K8s objects with no AWS cost.
//
// Per the deterministic-flow principle:
//   - never hangs (poll loop has a hard ceiling)
//   - structured outcome reported back to drill-result-<correlation> ConfigMap
//   - exit code reflects cleanup outcome (0 = clean, 1 = partial, 2 = bad input)
//
// Pillar 12 / additive only: new GVRs to clean up (akeyless drill
// identities, Route53 records, ACM certs) extend the gvrsToCleanup
// slice without touching the orchestration logic.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/pleme-io/pitr-tools/internal/log"
)

// pitrSessionGVR identifies the chart's own XR. Cluster-scoped per the
// chart's XRD (cluster-scoped composite).
var pitrSessionGVR = schema.GroupVersionResource{
	Group:    "pitr.akeyless.io",
	Version:  "v1",
	Resource: "pitrsessions",
}

// rdsInstanceGVR — the heavy AWS resource each drill provisions four of.
// Same constant as in cmd/verify/presence.go; duplicated here to keep
// each binary's import surface minimal.
var rdsInstanceGVR = schema.GroupVersionResource{
	Group:    "rds.aws.upbound.io",
	Version:  "v1beta1",
	Resource: "instances",
}

// gvrsToCleanup is the canonical set of GVRs the cleanup binary deletes
// by label. Add new GVRs here when new MR classes are emitted by the
// chart's Composition (akeyless drill identities, Route53 records, etc).
//
// Order matters for AWS-cost-first: heaviest/costliest resource first.
// Even if a later delete fails, the earlier ones stop accruing cost.
var gvrsToCleanup = []schema.GroupVersionResource{
	rdsInstanceGVR,
	// Future:
	// {Group: "akeyless.crossplane.io", Version: "v1alpha1", Resource: "authmethodapikeys"},
	// {Group: "route53.aws.upbound.io",  Version: "v1beta1", Resource: "records"},
}

const (
	defaultPollInterval = 30 * time.Second
	defaultMaxWait      = 90 * time.Minute // upper bound; drill verify itself caps at 60m
)

func main() {
	var (
		correlationID  = flag.String("correlation-id", "", "drill correlation_id (e.g. drill-smoke-abc12345)")
		podNamespace   = flag.String("pod-namespace", "", "namespace where drill-result-* ConfigMap lives (chart's release ns)")
		restoreNs      = flag.String("restore-namespace", "", "per-drill restore-<short_hash> namespace to delete")
		pitrSessionRef = flag.String("pitrsession", "smoke", "PITRSession XR name to read failureHandling spec from")
		maxWait        = flag.Duration("max-wait", defaultMaxWait, "how long to wait for verify Job's drill-result ConfigMap to appear with phase set")
		pollInterval   = flag.Duration("poll-interval", defaultPollInterval, "how often to poll the drill-result ConfigMap")
		dryRun         = flag.Bool("dry-run", false, "report decisions without performing deletes")
	)
	flag.Parse()

	if *correlationID == "" || *podNamespace == "" || *restoreNs == "" {
		fmt.Fprintln(os.Stderr, "cleanup: --correlation-id, --pod-namespace, --restore-namespace are required")
		os.Exit(2)
	}

	logger := log.New(*correlationID).With("job", "cleanup")
	logger.Info("cleanup binary starting",
		"pod_namespace", *podNamespace,
		"restore_namespace", *restoreNs,
		"pitrsession", *pitrSessionRef,
		"max_wait", maxWait.String(),
		"dry_run", *dryRun,
	)

	cfg, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("in-cluster config", "error", err.Error())
		os.Exit(1)
	}
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error("kubernetes client", "error", err.Error())
		os.Exit(1)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		logger.Error("dynamic client", "error", err.Error())
		os.Exit(1)
	}

	// === Step 1: read failureHandling from the PITRSession spec ===
	// We read this BEFORE polling for the verify outcome so that a
	// preserve=true operator override is honored even if the verify
	// Job never wrote its result (e.g. crashed early). preserve=true
	// means "no matter what, leave infra up" — the safest default for
	// post-mortem investigation.
	ctx, cancel := context.WithTimeout(context.Background(), *maxWait+5*time.Minute)
	defer cancel()

	preserve, retentionAfterSuccess, err := readFailureHandling(ctx, dyn, *pitrSessionRef)
	if err != nil {
		// If we can't read the spec, assume preserve=true (safe default)
		// and surface the error in the drill-result ConfigMap. Operator
		// inspects manually. Prefer leaving infra up vs deleting it on
		// a misread spec.
		logger.Error("read PITRSession spec — defaulting preserve=true (safe)", "error", err.Error())
		preserve = true
		retentionAfterSuccess = 5 * time.Minute
	}
	logger.Info("failureHandling spec",
		"preserve", preserve,
		"retentionAfterSuccess", retentionAfterSuccess.String(),
	)

	// === Step 2: poll the drill-result-<correlation> ConfigMap ===
	// Verify Job's terminal write sets data.phase to either "Succeeded"
	// or "Failed". This Pod runs concurrently with verify; we don't
	// know if verify is still running, has completed, or has failed
	// to write the ConfigMap at all.
	//
	// Per the deterministic-flow principle: hard ceiling on the poll
	// loop. If maxWait elapses without phase being set, treat as
	// "cleanup outcome ambiguous" and act on preserve flag alone.
	cmName := "drill-result-" + *correlationID
	phase, cmErr := pollResultPhase(ctx, k8s, *podNamespace, cmName, *pollInterval, *maxWait, logger)
	if cmErr != nil {
		logger.Error("verify outcome unavailable", "error", cmErr.Error(), "configmap", cmName)
		// If we can't read the verify outcome, ASSUME failure (safer
		// than assuming success) — that way the preserve flag still
		// governs whether we clean up or leave infra.
		phase = "Failed"
	}
	logger.Info("verify phase observed", "phase", phase)

	// === Step 3: decide cleanup action ===
	type Decision string
	const (
		DecisionTeardown   Decision = "Teardown"
		DecisionPreserved  Decision = "Preserved"
		DecisionRetained   Decision = "RetainedAfterSuccess"
	)
	var decision Decision
	switch phase {
	case "Succeeded":
		// Honor retentionAfterSuccess BEFORE teardown so the operator
		// has a window to inspect the restored env if they want.
		logger.Info("drill succeeded — sleeping retentionAfterSuccess before teardown",
			"retentionAfterSuccess", retentionAfterSuccess.String(),
		)
		select {
		case <-ctx.Done():
			logger.Error("context canceled during retention sleep — proceeding with teardown immediately", "error", ctx.Err().Error())
		case <-time.After(retentionAfterSuccess):
		}
		decision = DecisionTeardown
	case "Failed":
		if preserve {
			decision = DecisionPreserved
		} else {
			decision = DecisionTeardown
		}
	default:
		// Unknown phase. Honor preserve flag.
		logger.Error("verify phase is empty/unknown — falling back to preserve flag", "phase", phase)
		if preserve {
			decision = DecisionPreserved
		} else {
			decision = DecisionTeardown
		}
	}
	logger.Info("cleanup decision",
		"decision", string(decision),
		"phase", phase,
		"preserve", preserve,
	)

	// === Step 4: execute decision ===
	cleanupStatus := "Skipped"
	cleanupReason := ""
	if decision == DecisionTeardown {
		if *dryRun {
			cleanupStatus = "DryRun"
			cleanupReason = "dry-run flag set; no deletes performed"
			logger.Info("dry-run — would delete", "correlation_id", *correlationID, "restore_namespace", *restoreNs)
		} else {
			deleted, errs := executeTeardown(ctx, dyn, k8s, *correlationID, *restoreNs, logger)
			if len(errs) == 0 {
				cleanupStatus = "Completed"
				cleanupReason = fmt.Sprintf("deleted %d resources", deleted)
			} else {
				cleanupStatus = "Partial"
				cleanupReason = fmt.Sprintf("deleted %d resources; %d errors: %s", deleted, len(errs), errors.Join(errs...).Error())
			}
		}
	} else if decision == DecisionPreserved {
		cleanupStatus = "Skipped"
		cleanupReason = "spec.failureHandling.preserve=true — infrastructure left up for investigation"
		logger.Info("preserving infrastructure per operator override")
	}

	// === Step 5: write outcome back to drill-result ConfigMap ===
	// The chart's observe Object lifts cleanup_status into the XR's
	// status; downstream observers (kubectl get pitrsession,
	// alerting, dashboards) read the deterministic outcome.
	if err := writeCleanupOutcome(ctx, k8s, *podNamespace, cmName, cleanupStatus, cleanupReason, string(decision)); err != nil {
		logger.Error("write cleanup outcome to ConfigMap", "error", err.Error())
		// Don't change exit code on this — the ACTUAL cleanup status
		// is the authoritative signal. Logging suffices.
	}
	logger.Info("cleanup outcome",
		"status", cleanupStatus,
		"reason", cleanupReason,
		"decision", string(decision),
	)

	// Exit code reflects cleanup outcome:
	//   Completed | Skipped | DryRun  → 0
	//   Partial                       → 1
	//   anything else                 → 1
	if cleanupStatus == "Completed" || cleanupStatus == "Skipped" || cleanupStatus == "DryRun" {
		os.Exit(0)
	}
	os.Exit(1)
}

// readFailureHandling reads the named PITRSession's spec.failureHandling
// + spec.retentionAfterSuccess. Returns sensible defaults on any field
// being absent. The dynamic client is used so this binary doesn't import
// the chart's typed Go schema.
func readFailureHandling(ctx context.Context, dyn dynamic.Interface, name string) (preserve bool, retention time.Duration, err error) {
	xr, err := dyn.Resource(pitrSessionGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return true, 5 * time.Minute, fmt.Errorf("get pitrsession/%s: %w", name, err)
	}
	spec, _, _ := unstructuredNestedMap(xr.Object, "spec")
	fh, _, _ := unstructuredNestedMap(spec, "failureHandling")
	if v, ok := fh["preserve"].(bool); ok {
		preserve = v
	}
	retention = 5 * time.Minute
	if rs, ok := spec["retentionAfterSuccess"].(string); ok && rs != "" {
		if d, perr := time.ParseDuration(rs); perr == nil {
			retention = d
		}
	}
	return preserve, retention, nil
}

// pollResultPhase polls the drill-result-<correlation> ConfigMap until
// data.phase is set, ctx is canceled, or maxWait elapses. Returns the
// phase string ("Succeeded" / "Failed" / "" if not set).
func pollResultPhase(ctx context.Context, k8s kubernetes.Interface, ns, name string, interval, maxWait time.Duration, logger interface {
	Info(string, ...any)
	Error(string, ...any)
}) (string, error) {
	deadline := time.Now().Add(maxWait)
	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		cm, err := k8s.CoreV1().ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			if phase := cm.Data["phase"]; phase != "" {
				logger.Info("drill-result ConfigMap phase observed", "phase", phase, "attempts", attempts)
				return phase, nil
			}
		} else if !apierrors.IsNotFound(err) {
			logger.Error("get drill-result ConfigMap (transient)", "attempt", attempts, "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context canceled: %w", ctx.Err())
		case <-time.After(interval):
		}
	}
	return "", fmt.Errorf("max-wait elapsed (attempts=%d)", attempts)
}

// executeTeardown deletes resources by label-selector across the canonical
// gvrsToCleanup set, then deletes the per-drill restore namespace.
// Returns (count_deleted, errors). Continues past partial failures.
func executeTeardown(ctx context.Context, dyn dynamic.Interface, k8s kubernetes.Interface, correlationID, restoreNs string, logger interface {
	Info(string, ...any)
	Error(string, ...any)
}) (int, []error) {
	deleted := 0
	var errs []error

	// 1. Delete heavy MRs by label across the canonical GVR set.
	for _, gvr := range gvrsToCleanup {
		list, err := dyn.Resource(gvr).List(ctx, metav1.ListOptions{
			LabelSelector: "pitr-correlation-id=" + correlationID,
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("list %s: %w", gvr.String(), err))
			continue
		}
		for _, item := range list.Items {
			name := item.GetName()
			if err := dyn.Resource(gvr).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
				if apierrors.IsNotFound(err) {
					continue // already gone — fine
				}
				errs = append(errs, fmt.Errorf("delete %s/%s: %w", gvr.Resource, name, err))
				continue
			}
			deleted++
			logger.Info("deleted resource", "gvr", gvr.String(), "name", name)
		}
	}

	// 2. Delete the per-drill restore namespace. Cascade-deletes all
	//    in-namespace resources (saas Pods, Deployments, Services,
	//    ConfigMap clones, in-ns RBAC).
	if restoreNs != "" {
		if err := k8s.CoreV1().Namespaces().Delete(ctx, restoreNs, metav1.DeleteOptions{}); err != nil {
			if !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("delete namespace %s: %w", restoreNs, err))
			}
		} else {
			deleted++
			logger.Info("deleted namespace", "namespace", restoreNs)
		}
	}

	return deleted, errs
}

// writeCleanupOutcome appends cleanup_status + cleanup_reason +
// cleanup_decision to the existing drill-result-<correlation> ConfigMap.
// Idempotent: if the ConfigMap doesn't exist yet, creates it (rare —
// usually verify writes it first). Existing keys are preserved.
func writeCleanupOutcome(ctx context.Context, k8s kubernetes.Interface, ns, cmName, status, reason, decision string) error {
	api := k8s.CoreV1().ConfigMaps(ns)
	cm, err := api.Get(ctx, cmName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get %s: %w", cmName, err)
		}
		// Create-with-cleanup-fields-only path. Should be rare; verify
		// usually writes the CM first.
		newCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: ns,
				Labels: map[string]string{
					"pitr-component": "verify-result",
				},
			},
			Data: map[string]string{
				"cleanup_status":   status,
				"cleanup_reason":   reason,
				"cleanup_decision": decision,
				"cleanup_at":       time.Now().UTC().Format(time.RFC3339),
			},
		}
		_, err = api.Create(ctx, newCM, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s: %w", cmName, err)
		}
		return nil
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["cleanup_status"] = status
	cm.Data["cleanup_reason"] = reason
	cm.Data["cleanup_decision"] = decision
	cm.Data["cleanup_at"] = time.Now().UTC().Format(time.RFC3339)
	if _, err := api.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update %s: %w", cmName, err)
	}
	return nil
}

// unstructuredNestedMap navigates an unstructured-style map by string
// keys; returns (innerMap, found, error). Used to read PITRSession
// spec without importing typed schema.
func unstructuredNestedMap(obj map[string]interface{}, fields ...string) (map[string]interface{}, bool, error) {
	cur := interface{}(obj)
	for _, f := range fields {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return nil, false, fmt.Errorf("path %q is not a map", strings.Join(fields, "."))
		}
		v, ok := m[f]
		if !ok {
			return nil, false, nil
		}
		cur = v
	}
	final, ok := cur.(map[string]interface{})
	if !ok {
		return nil, true, fmt.Errorf("path is not a map")
	}
	return final, true, nil
}

// avoid unused-import errors when corev1/json are referenced through the
// k8s.io types only.
var _ = json.RawMessage{}
var _ = corev1.Namespace{}
