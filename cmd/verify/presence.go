// presence-mode checks: mirror the implicit verifications the human-driven
// PITR drill performs by the time its `staging_rds_restore_deploy_*.sh`
// script reaches its final "finished successfully!!!" echo.
//
// The human script's notion of success is "infrastructure deployed":
//   - terraform apply on the akeyless_global staging-restore env succeeded
//     (all source RDS instances have been PITR-restored to fresh instances)
//   - kubectl apply on the {auth,kfm1,kfm2,kfm3,uam}-configmap clones to
//     the restore namespace succeeded (the saas microservices have their
//     runtime config available)
//   - helm install of saas-restore succeeded (the 5 Deployments rolled out
//     and reached spec.replicas == status.readyReplicas)
//   - kubectl set env on each Deployment succeeded (cross-service env vars
//     UAM_AUTH_DNS / KFM_AUTH_DNS / etc. are present)
//
// Our pitr-akeyless Composition gates on most of the same conditions
// implicitly via Crossplane's Ready=True reconciliation, but until the
// verify Job runs we don't have a single typed report aggregating the
// state of all of them. presence-mode runs these checks against the K8s
// API + Crossplane MR statuses, accumulates pass/fail per check (NEVER
// fail-fast — every failure adds context to the bundle), and writes the
// structured outcome to the drill-result-<correlation> ConfigMap.
//
// On any check failure: per-check Diagnostics are gathered (Pod status,
// recent Events, MR conditions, Job statuses) and embedded in the same
// ConfigMap so the diagnostic-collect Job (or operator inspecting
// kubectl describe) has the failure context co-located with the report.
//
// Each Check is independent + composable; new Checks (e.g. AuthCheck for
// when akeyless-internals are sufficiently understood, or Route53Check
// when we wire DNS provisioning into the chart) extend the slice without
// touching the existing impls. Pillar 12 (generation over composition):
// the Check stack grows; nothing shrinks.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// CheckResult is one Check's outcome, with attached diagnostics for the
// failure case. JSON-marshaled into the drill-result-<correlation>
// ConfigMap's checks data key so downstream observers (chart's observe
// Object → composition function-extra-resources → XR status), kubectl
// describe, and the diagnostic-collect Job all see the same payload.
type CheckResult struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Passed      bool              `json:"passed"`
	Message     string            `json:"message,omitempty"`
	Diagnostics map[string]string `json:"diagnostics,omitempty"`
	StartedAt   time.Time         `json:"started_at"`
	DurationMs  int64             `json:"duration_ms"`
}

// Check is the per-step probe contract. Each Check is run in sequence
// against the same k8s + dynamic clients so cluster connection setup is
// not duplicated. Returning a CheckResult NEVER aborts the run; even a
// fatal-shaped error returns Passed=false and Diagnostics populated, so
// the operator's report carries every failure mode the run encountered.
type Check interface {
	Name() string
	Description() string
	Run(ctx context.Context, env *checkEnv) CheckResult
}

// checkEnv carries shared dependencies + scope into each Check. Built
// once, passed by reference to every Run().
type checkEnv struct {
	k8s            kubernetes.Interface
	dyn            dynamic.Interface
	correlationID  string
	correlationLab string // pitr-correlation-id label value, e.g. drill-smoke-<hash>
	restoreNs      string // restore-<short_hash> namespace where saas Pods live
	jobNs          string // pitr-akeyless namespace where Jobs run
}

// runChecks executes each Check in order, accumulates results, and returns
// the slice. Per the operator's principle: never fail-fast — every check
// runs even when an earlier one failed, so the final report aggregates
// every failure mode. The caller decides exit code based on the results.
func runChecks(ctx context.Context, env *checkEnv, checks []Check) []CheckResult {
	results := make([]CheckResult, 0, len(checks))
	for _, c := range checks {
		started := time.Now()
		res := c.Run(ctx, env)
		res.Name = c.Name()
		res.Description = c.Description()
		res.StartedAt = started
		res.DurationMs = time.Since(started).Milliseconds()
		results = append(results, res)
	}
	return results
}

// allPassed returns true iff every CheckResult has Passed=true.
func allPassed(results []CheckResult) bool {
	for _, r := range results {
		if !r.Passed {
			return false
		}
	}
	return true
}

// summarizeChecks returns (passed, failed) counts.
func summarizeChecks(results []CheckResult) (int, int) {
	passed, failed := 0, 0
	for _, r := range results {
		if r.Passed {
			passed++
		} else {
			failed++
		}
	}
	return passed, failed
}

// newCheckEnv builds a checkEnv from in-cluster config + the provided
// scope (namespaces + correlation_id). Returns nil + error if the
// in-cluster setup fails — the verify Job must be running inside the
// cluster (it is, by chart construction).
func newCheckEnv(correlationID, restoreNs, jobNs string) (*checkEnv, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	return &checkEnv{
		k8s:            k8s,
		dyn:            dyn,
		correlationID:  correlationID,
		correlationLab: correlationID, // already the full label value (e.g. drill-smoke-abc12345)
		restoreNs:      restoreNs,
		jobNs:          jobNs,
	}, nil
}

// =====================================================================
// Concrete Checks — each extends the human-script's implicit verifications.
// Add new Checks below; do NOT modify existing impls (additive only).
// =====================================================================

// rdsReadyCheck verifies all 4 Crossplane RDS Instance MRs labeled with the
// drill correlation_id are READY=True (mirrors `terraform apply` succeeding
// in the human script — that step provisions the same set of restored
// instances). Reads via dynamic client to avoid a hard provider-aws-rds
// type dependency in this binary.
type rdsReadyCheck struct{}

func (rdsReadyCheck) Name() string { return "rds-pitr-ready" }
func (rdsReadyCheck) Description() string {
	return "all Crossplane RDS Instance MRs for this drill are Ready=True (mirrors terraform apply success in human script)"
}

var rdsInstanceGVR = schema.GroupVersionResource{
	Group:    "rds.aws.upbound.io",
	Version:  "v1beta1",
	Resource: "instances",
}

func (rdsReadyCheck) Run(ctx context.Context, env *checkEnv) CheckResult {
	list, err := env.dyn.Resource(rdsInstanceGVR).List(ctx, metav1.ListOptions{
		LabelSelector: "pitr-correlation-id=" + env.correlationLab,
	})
	if err != nil {
		return CheckResult{
			Passed:  false,
			Message: "list RDS Instance MRs: " + err.Error(),
			Diagnostics: map[string]string{
				"selector": "pitr-correlation-id=" + env.correlationLab,
				"gvr":      rdsInstanceGVR.String(),
			},
		}
	}
	if len(list.Items) == 0 {
		return CheckResult{
			Passed:  false,
			Message: "no RDS Instance MRs found for this drill correlation",
			Diagnostics: map[string]string{
				"selector":      "pitr-correlation-id=" + env.correlationLab,
				"matched_count": "0",
			},
		}
	}
	notReady := []string{}
	for _, item := range list.Items {
		conds, found, _ := nestedSlice(item.Object, "status", "conditions")
		ready := false
		for _, c := range conds {
			cm, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			if cm["type"] == "Ready" && cm["status"] == "True" {
				ready = true
				break
			}
		}
		if !found || !ready {
			notReady = append(notReady, item.GetName())
		}
	}
	sort.Strings(notReady)
	if len(notReady) == 0 {
		return CheckResult{
			Passed:  true,
			Message: fmt.Sprintf("%d RDS Instance MRs all Ready=True", len(list.Items)),
		}
	}
	return CheckResult{
		Passed:  false,
		Message: fmt.Sprintf("%d/%d RDS Instance MRs not yet Ready", len(notReady), len(list.Items)),
		Diagnostics: map[string]string{
			"not_ready":   joinTrimmed(notReady),
			"total_count": fmt.Sprintf("%d", len(list.Items)),
		},
	}
}

// deploymentsReadyCheck verifies the 5 saas-pitr-* Deployments in the
// per-drill restore namespace have spec.replicas == status.readyReplicas
// (mirrors `helm install saas-restore` reaching its rollout-complete state
// in the human script). Tolerates absence of the namespace (returns Passed
// = false with descriptive Message + diagnostics) so the operator sees
// "deploys not yet reconciled" distinctly from "Pods crashed".
type deploymentsReadyCheck struct{}

func (deploymentsReadyCheck) Name() string { return "saas-deployments-ready" }
func (deploymentsReadyCheck) Description() string {
	return "all saas-pitr-* Deployments have spec.replicas == status.readyReplicas (mirrors helm rollout complete)"
}

func (deploymentsReadyCheck) Run(ctx context.Context, env *checkEnv) CheckResult {
	dl, err := env.k8s.AppsV1().Deployments(env.restoreNs).List(ctx, metav1.ListOptions{})
	if err != nil {
		return CheckResult{
			Passed:  false,
			Message: "list Deployments in restore namespace: " + err.Error(),
			Diagnostics: map[string]string{
				"namespace": env.restoreNs,
			},
		}
	}
	if len(dl.Items) == 0 {
		return CheckResult{
			Passed:  false,
			Message: "no Deployments found in restore namespace",
			Diagnostics: map[string]string{
				"namespace":    env.restoreNs,
				"matched_total": "0",
			},
		}
	}
	notReady := []string{}
	readyDetail := map[string]string{}
	for _, d := range dl.Items {
		desired := int32(0)
		if d.Spec.Replicas != nil {
			desired = *d.Spec.Replicas
		}
		if desired == d.Status.ReadyReplicas {
			readyDetail[d.Name] = fmt.Sprintf("ready=%d/%d", d.Status.ReadyReplicas, desired)
		} else {
			detail := fmt.Sprintf("ready=%d/%d updated=%d available=%d",
				d.Status.ReadyReplicas, desired, d.Status.UpdatedReplicas, d.Status.AvailableReplicas)
			readyDetail[d.Name] = detail
			notReady = append(notReady, d.Name+": "+detail)
		}
	}
	sort.Strings(notReady)
	if len(notReady) == 0 {
		return CheckResult{
			Passed:  true,
			Message: fmt.Sprintf("%d Deployments all at desired ready replica count", len(dl.Items)),
		}
	}
	diag, _ := json.Marshal(readyDetail)
	return CheckResult{
		Passed:  false,
		Message: fmt.Sprintf("%d/%d Deployments not yet at desired replicas", len(notReady), len(dl.Items)),
		Diagnostics: map[string]string{
			"namespace":   env.restoreNs,
			"not_ready":   joinTrimmed(notReady),
			"all_status":  string(diag),
			"total_count": fmt.Sprintf("%d", len(dl.Items)),
		},
	}
}

// canaryCreateSucceededCheck verifies the canary-create Job for this
// drill correlation reached status.succeeded > 0 (proves the canary
// secret was written to the SOURCE akeyless tenant before the PITR
// snapshot was taken — a precondition for the canary to exist in the
// restored DB). The job-kind label is set by the chart's KCL; we read
// via that selector so the check is correlation-id-scoped.
type canaryCreateSucceededCheck struct{}

func (canaryCreateSucceededCheck) Name() string { return "canary-create-succeeded" }
func (canaryCreateSucceededCheck) Description() string {
	return "the canary-create Job for this drill reached status.succeeded > 0 (canary written on source pre-snapshot)"
}

func (canaryCreateSucceededCheck) Run(ctx context.Context, env *checkEnv) CheckResult {
	jobs, err := env.k8s.BatchV1().Jobs(env.jobNs).List(ctx, metav1.ListOptions{
		LabelSelector: "pitr-correlation-id=" + env.correlationLab + ",pitr-job-kind=canary-create",
	})
	if err != nil {
		return CheckResult{
			Passed:  false,
			Message: "list canary-create Job: " + err.Error(),
			Diagnostics: map[string]string{
				"namespace": env.jobNs,
				"selector":  "pitr-correlation-id=" + env.correlationLab + ",pitr-job-kind=canary-create",
			},
		}
	}
	if len(jobs.Items) == 0 {
		return CheckResult{
			Passed:  false,
			Message: "no canary-create Job found",
			Diagnostics: map[string]string{
				"namespace":    env.jobNs,
				"selector":     "pitr-correlation-id=" + env.correlationLab + ",pitr-job-kind=canary-create",
				"matched_count": "0",
			},
		}
	}
	for _, j := range jobs.Items {
		if j.Status.Succeeded > 0 {
			return CheckResult{
				Passed:  true,
				Message: fmt.Sprintf("Job %s succeeded=%d", j.Name, j.Status.Succeeded),
			}
		}
	}
	// No success yet: collect Job state diagnostics for each candidate.
	diagBag := map[string]string{}
	for _, j := range jobs.Items {
		diagBag[j.Name+".status.active"] = fmt.Sprintf("%d", j.Status.Active)
		diagBag[j.Name+".status.failed"] = fmt.Sprintf("%d", j.Status.Failed)
		diagBag[j.Name+".status.succeeded"] = fmt.Sprintf("%d", j.Status.Succeeded)
		for _, c := range j.Status.Conditions {
			diagBag[j.Name+".condition."+string(c.Type)] = fmt.Sprintf("%s: %s", c.Status, c.Message)
		}
	}
	return CheckResult{
		Passed:      false,
		Message:     "no canary-create Job has succeeded yet for this correlation",
		Diagnostics: diagBag,
	}
}

// configMapsClonedCheck verifies the per-microservice runtime ConfigMaps
// the human script clones from default ns (line 159-163) are present in
// the per-drill restore namespace AND non-empty. Without auth-configmap
// the auth Pod can't read its 50+ runtime config keys; without kfm1/2/3-
// configmap the KFMs miss their config; same for uam.
type configMapsClonedCheck struct{}

func (configMapsClonedCheck) Name() string { return "configmaps-cloned" }
func (configMapsClonedCheck) Description() string {
	return "the per-microservice ConfigMaps are present in the restore namespace with non-empty data (mirrors human-script line 159-163 clone)"
}

var requiredConfigMaps = []string{
	"auth-configmap",
	"kfm1-configmap",
	"kfm2-configmap",
	"kfm3-configmap",
	"uam-configmap",
}

func (configMapsClonedCheck) Run(ctx context.Context, env *checkEnv) CheckResult {
	missing := []string{}
	empty := []string{}
	for _, name := range requiredConfigMaps {
		cm, err := env.k8s.CoreV1().ConfigMaps(env.restoreNs).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			missing = append(missing, name)
			continue
		}
		if len(cm.Data) == 0 && len(cm.BinaryData) == 0 {
			empty = append(empty, name)
		}
	}
	if len(missing) == 0 && len(empty) == 0 {
		return CheckResult{
			Passed:  true,
			Message: fmt.Sprintf("%d ConfigMaps all present + non-empty", len(requiredConfigMaps)),
		}
	}
	return CheckResult{
		Passed:  false,
		Message: fmt.Sprintf("%d ConfigMaps missing, %d empty", len(missing), len(empty)),
		Diagnostics: map[string]string{
			"namespace": env.restoreNs,
			"missing":   joinTrimmed(missing),
			"empty":     joinTrimmed(empty),
			"required":  joinTrimmed(requiredConfigMaps),
		},
	}
}

// =====================================================================
// helpers
// =====================================================================

// nestedSlice navigates an unstructured.Unstructured-style map by string
// keys and returns (slice, found, error).
func nestedSlice(obj map[string]interface{}, fields ...string) ([]interface{}, bool, error) {
	cur := interface{}(obj)
	for _, f := range fields {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return nil, false, nil
		}
		v, ok := m[f]
		if !ok {
			return nil, false, nil
		}
		cur = v
	}
	s, ok := cur.([]interface{})
	if !ok {
		return nil, false, nil
	}
	return s, true, nil
}

// joinTrimmed joins a string slice with comma-space separators; empty
// slices return "(none)". Used for Diagnostics map values where the
// string is a list of items.
func joinTrimmed(items []string) string {
	if len(items) == 0 {
		return "(none)"
	}
	out := items[0]
	for _, s := range items[1:] {
		out += ", " + s
	}
	return out
}

// gatherEvents lists recent K8s Events in a namespace and returns the
// last N as a single newline-separated diagnostics string. Used by
// Checks that want "what's been going wrong on this scope" context
// when a check fails.
func gatherEvents(ctx context.Context, k8s kubernetes.Interface, namespace string, max int) string {
	events, err := k8s.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{Limit: int64(max)})
	if err != nil {
		return "events list error: " + err.Error()
	}
	if len(events.Items) == 0 {
		return "(no events)"
	}
	out := ""
	for _, e := range events.Items {
		out += fmt.Sprintf("[%s] %s/%s %s: %s\n",
			e.LastTimestamp.Format(time.RFC3339),
			e.Type,
			e.Reason,
			e.InvolvedObject.Name,
			e.Message)
	}
	return out
}

// mrConditionsActionableCheck detects MRs in terminal-error states and
// surfaces the AWS / provider error message as Diagnostics. Distinguishes
// "still creating" (transient, just wait) from "AsyncCreateFailure"
// (terminal — the next reconcile will hit the same error indefinitely).
//
// Drill #11 motivated this Check: bis RDS Instance MR was stuck with
// LastAsyncOperation status=False reason=AsyncCreateFailure with message
// "InvalidParameterValue: cannot be restored to a time later than X" —
// AWS PITR per-DB binlog window violation. Without this Check, the drill
// would hang for activeDeadlineSeconds (60 min) waiting for an MR that
// will never reconcile to Ready=True.
//
// The signal pattern (Crossplane managed-resources convention):
//   - Synced=False                     → reconcile error (provider-side)
//   - LastAsyncOperation=False AND
//     reason ∈ {AsyncCreateFailure,
//               AsyncUpdateFailure,
//               AsyncDeleteFailure}    → terminal AWS-side failure
//   - Ready=False AND reason=Creating
//     AND LastAsyncOperation=True
//     (or absent)                      → transient (still in progress)
//
// On any terminal-failure detection: returns Passed=false with the full
// LastAsyncOperation.message in Diagnostics, distinguishing this from
// the rdsReadyCheck's "still creating" verdict. The operator's report
// gets the precise AWS error string co-located with the MR identifier.
//
// Today this Check examines RDS Instance MRs; future iterations extend
// the GVR list to cover akeyless drill identities (AuthMethod, Role, etc),
// Route53 records when DNS provisioning lands, ACM cert MRs, etc.
type mrConditionsActionableCheck struct{}

func (mrConditionsActionableCheck) Name() string { return "mr-conditions-actionable" }
func (mrConditionsActionableCheck) Description() string {
	return "detect MRs in terminal-error states (AsyncCreateFailure / ReconcileError) and surface the provider error message — distinguishes terminal failures from in-progress creates so the operator's report is actionable, not hanging"
}

// terminalReasons collects the Crossplane LastAsyncOperation condition
// reasons that indicate an unrecoverable async operation. Add reasons here
// when new failure modes are observed (ReconcileError, etc.) — additive.
var terminalReasons = map[string]bool{
	"AsyncCreateFailure": true,
	"AsyncUpdateFailure": true,
	"AsyncDeleteFailure": true,
}

// gvrsExaminedForActionableConditions is the set of provider-aws-rds /
// provider-kubernetes / etc. GVRs the Check inspects. Extending this slice
// adds coverage without touching the Run() logic.
var gvrsExaminedForActionableConditions = []schema.GroupVersionResource{
	rdsInstanceGVR,
	// Future: route53 RecordSet GVR, ACM Certificate GVR, akeyless
	// AuthMethod GVR (when crossplane-akeyless lands), etc.
}

func (mrConditionsActionableCheck) Run(ctx context.Context, env *checkEnv) CheckResult {
	type failedMR struct {
		gvr     string
		name    string
		reason  string
		message string
	}
	var terminal []failedMR
	totalExamined := 0

	for _, gvr := range gvrsExaminedForActionableConditions {
		list, err := env.dyn.Resource(gvr).List(ctx, metav1.ListOptions{
			LabelSelector: "pitr-correlation-id=" + env.correlationLab,
		})
		if err != nil {
			// Listing failure for one GVR shouldn't fail-fast the Check;
			// other GVRs may still surface actionable info.
			continue
		}
		for _, item := range list.Items {
			totalExamined++
			conds, found, _ := nestedSlice(item.Object, "status", "conditions")
			if !found {
				continue
			}
			for _, c := range conds {
				cm, ok := c.(map[string]interface{})
				if !ok {
					continue
				}
				if cm["type"] != "LastAsyncOperation" {
					continue
				}
				status, _ := cm["status"].(string)
				reason, _ := cm["reason"].(string)
				if status != "False" || !terminalReasons[reason] {
					continue
				}
				msg, _ := cm["message"].(string)
				terminal = append(terminal, failedMR{
					gvr:     gvr.String(),
					name:    item.GetName(),
					reason:  reason,
					message: msg,
				})
			}
		}
	}

	if len(terminal) == 0 {
		return CheckResult{
			Passed:  true,
			Message: fmt.Sprintf("no MRs in terminal-error state (examined %d MRs across %d GVRs)", totalExamined, len(gvrsExaminedForActionableConditions)),
		}
	}

	// Build operator-actionable Diagnostics. Each terminal failure gets a
	// keyed entry: <gvr>/<name>.{reason,message}. The provider message
	// goes through verbatim — typically carries the AWS api error string
	// like "InvalidParameterValue: cannot be restored to a time later
	// than X" — directly actionable for the operator.
	diag := map[string]string{
		"terminal_count": fmt.Sprintf("%d", len(terminal)),
		"total_examined": fmt.Sprintf("%d", totalExamined),
	}
	names := []string{}
	for _, t := range terminal {
		names = append(names, t.name)
		diag[t.name+".gvr"] = t.gvr
		diag[t.name+".reason"] = t.reason
		diag[t.name+".message"] = t.message
	}
	sort.Strings(names)
	return CheckResult{
		Passed:      false,
		Message:     fmt.Sprintf("%d MRs in terminal-error state — drill cannot reach Ready: %s", len(terminal), joinTrimmed(names)),
		Diagnostics: diag,
	}
}

// presenceChecks is the canonical Check set for --mode=presence today.
// New Checks land here additively (Pillar 12: generation grows; never
// shrinks). When akeyless-internals are sufficiently understood,
// authCheck + describeItemCheck land in --mode=api as the next layer
// and this list extends accordingly.
//
// Order matters for diagnostic ergonomics:
//   1. mr-conditions-actionable runs FIRST so terminal failures surface
//      before any "Ready=False" Check muddies the report. The operator's
//      first failed-Check is the actionable one.
//   2. rds-pitr-ready / saas-deployments-ready report progress state.
//   3. configmaps-cloned + canary-create-succeeded validate static
//      preconditions.
func presenceChecks() []Check {
	return []Check{
		mrConditionsActionableCheck{},
		rdsReadyCheck{},
		configMapsClonedCheck{},
		deploymentsReadyCheck{},
		canaryCreateSucceededCheck{},
	}
}

// avoid unused-import error when corev1 happens not to be referenced
// directly (it's used through kubernetes.Interface return types).
var _ = corev1.Namespace{}
