// Package result persists drill verify outcomes as Kubernetes ConfigMaps so
// the orchestrating Crossplane Composition can lift them into the
// PITRSession XR's status.retrievedSecrets[] (the function signature's
// output side).
//
// The verify Job runs in the chart's stable namespace (pitr-akeyless),
// authenticated as the pitr-akeyless-job-runner ServiceAccount. After the
// Job iterates the requested secret paths, it calls WriteConfigMap to
// persist the result list. The chart then emits a Crossplane
// kubernetes.crossplane.io/Object in managementPolicies=[Observe] mode
// pointing at this ConfigMap; the function-kcl pipeline reads it via
// observed-composed-resources (ocds) and patches PITRSession.status.
//
// Why a ConfigMap (not a Secret, not a CRD): the result list is non-
// sensitive (just secret paths, never values) and ConfigMaps are the
// lowest-friction K8s primitive for the Crossplane Object/Observe
// pattern — no schema registration, no operator-side controller, just a
// pure pull.
package result

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corev1typed "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

// Phase enumerates the verify Job's terminal outcomes that the chart's
// status update path knows how to interpret. Mirrors PITRSession.status.
// phase semantics defined in the XRD.
type Phase string

const (
	PhaseSucceeded Phase = "Succeeded"
	PhaseFailed    Phase = "Failed"
)

// CheckSummary is one verify Check's outcome plus its gathered diagnostics.
// Mirrors cmd/verify/presence.go:CheckResult but lives here so it can be
// embedded in Outcome without creating an import cycle. Diagnostics is a
// flat string→string map (kubectl-describe-friendly + JSON-friendly + can
// be lifted into Composition status by function-extra-resources without
// nested-shape gymnastics).
type CheckSummary struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Passed      bool              `json:"passed"`
	Message     string            `json:"message,omitempty"`
	Diagnostics map[string]string `json:"diagnostics,omitempty"`
	DurationMs  int64             `json:"duration_ms,omitempty"`
}

// Outcome is the structured shape the verify Job persists. Marshaled to
// the ConfigMap's `retrievedSecrets` data key as a JSON array of strings;
// counts are written as separate keys for human-readable kubectl-describe
// output without re-parsing JSON.
//
// Checks (added 2026-05-09 with --mode=presence) carries per-Check
// pass/fail + diagnostics so the operator's report aggregates every
// failure mode the run encountered (no fail-fast). When Checks is
// empty, callers (api-mode) get the legacy retrievedSecrets-only shape.
type Outcome struct {
	CorrelationID    string         `json:"correlation_id"`
	RetrievedSecrets []string       `json:"retrieved_secrets"`
	MissingSecrets   []string       `json:"missing_secrets"`
	Phase            Phase          `json:"phase"`
	Checks           []CheckSummary `json:"checks,omitempty"`
}

// WriteConfigMap upserts a ConfigMap named drill-result-<correlationID>
// in the namespace of the running Pod (read from POD_NAMESPACE env, set
// via the chart's downward API). Idempotent: existing ConfigMaps with
// the same name are updated rather than create-rejected.
//
// Returns the ConfigMap name on success so callers can log it.
func WriteConfigMap(ctx context.Context, podNamespace string, outcome Outcome) (string, error) {
	if podNamespace == "" {
		return "", fmt.Errorf("pod namespace is empty (POD_NAMESPACE env unset?)")
	}
	if outcome.CorrelationID == "" {
		return "", fmt.Errorf("outcome has empty correlation id")
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return "", fmt.Errorf("in-cluster config: %w", err)
	}

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("kubernetes client: %w", err)
	}

	api := cs.CoreV1().ConfigMaps(podNamespace)
	if err := writeWithClient(ctx, api, podNamespace, outcome); err != nil {
		return "", err
	}
	return "drill-result-" + outcome.CorrelationID, nil
}

// writeWithClient is the testable inner: it takes an already-resolved
// ConfigMap typed-client (real or fake) and performs the upsert. Pulled
// out so tests can run against client-go's fake.NewSimpleClientset
// without an in-cluster service account.
func writeWithClient(ctx context.Context, api corev1typed.ConfigMapInterface, namespace string, outcome Outcome) error {
	cmName := "drill-result-" + outcome.CorrelationID

	retrievedJSON, err := json.Marshal(outcome.RetrievedSecrets)
	if err != nil {
		return fmt.Errorf("marshal retrieved: %w", err)
	}
	missingJSON, err := json.Marshal(outcome.MissingSecrets)
	if err != nil {
		return fmt.Errorf("marshal missing: %w", err)
	}

	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: namespace,
			Labels: map[string]string{
				"pitr-correlation-id":          outcome.CorrelationID,
				"pitr-component":               "verify-result",
				"app.kubernetes.io/managed-by": "pitr-akeyless",
			},
		},
		Data: map[string]string{
			"retrievedSecrets": string(retrievedJSON),
			"missingSecrets":   string(missingJSON),
			"totalRetrieved":   strconv.Itoa(len(outcome.RetrievedSecrets)),
			"totalMissing":     strconv.Itoa(len(outcome.MissingSecrets)),
			"phase":            string(outcome.Phase),
			"correlationId":    outcome.CorrelationID,
		},
	}

	if len(outcome.Checks) > 0 {
		checksJSON, err := json.Marshal(outcome.Checks)
		if err != nil {
			return fmt.Errorf("marshal checks: %w", err)
		}
		passed, failed := 0, 0
		for _, c := range outcome.Checks {
			if c.Passed {
				passed++
			} else {
				failed++
			}
		}
		desired.Data["checks"] = string(checksJSON)
		desired.Data["totalChecks"] = strconv.Itoa(len(outcome.Checks))
		desired.Data["checksPassed"] = strconv.Itoa(passed)
		desired.Data["checksFailed"] = strconv.Itoa(failed)
	}

	_, err = api.Create(ctx, desired, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create configmap: %w", err)
	}
	existing, err := api.Get(ctx, cmName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get configmap for update: %w", err)
	}
	existing.Labels = desired.Labels
	existing.Data = desired.Data
	if _, err := api.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update configmap: %w", err)
	}
	return nil
}
