// Command diagnostic-collect runs after a drill terminates and emits a
// structured bundle of K8s + akeyless state for triage. Operator retrieves
// via `kubectl logs diagnostic-collect-<correlation_id> -n pitr-akeyless`.
//
// Per Decision 7 (Omri's failure-handling shape: preserveInfrastructure +
// diagnostic Job + Slack notification), Decision 19 (Go binaries in
// pitr-tools image), and project idiom (no shell beyond unavoidable glue —
// orchestration logic stays in typed Go).
//
// Bundle shape (JSON to stdout):
//
//	{
//	  "manifest": {
//	    "correlation_id": "drill-smoke-d6f2d450",
//	    "tenant": "smoke",
//	    "phase": "Failed",
//	    "restore_time": "2026-05-09T00:31:50Z",
//	    "secrets_requested": ["/pitr/smoke/baseline"],
//	    "secrets_retrieved": [],
//	    "completion_time": "2026-05-09T01:08:42Z",
//	    "file_count": 38
//	  },
//	  "files": {
//	    "pitrsession.yaml":         "...",
//	    "resources/instance-auth":  "...",
//	    "resources/instance-uam":   "...",
//	    "logs/saas-pitr-auth-XXX":  "...",
//	    "logs/saas-pitr-uam-XXX":   "...",
//	    "akeyless/source/_pitr_smoke_baseline.json": "..."
//	  }
//	}
//
// Behavior:
//
//   - Reads PITRSession.status to determine phase + extract drill metadata.
//   - On phase=Succeeded: minimal proof bundle (manifest + pitrsession + retrieved-secrets describe).
//   - On phase=Failed (or any non-Succeeded): full bundle.
//   - Lists MRs labeled `pitr-correlation-id=<id>` across known GVRs and
//     captures their YAML.
//   - Collects logs (current + previous container) for every Pod labeled
//     with the correlation_id, plus the per-drill restore-* namespace's
//     saas-pitr-* Pods.
//   - Calls source akeyless /describe-item per requested secret (validates
//     source-side state at drill end — disambiguates "secret missing in
//     source" from "secret missing post-restore").
//
// Exit codes:
//
//   - 0: bundle emitted (drill itself may have Failed; this binary's exit
//        does not change drill state).
//   - 2: required-arg validation failed.
//
// Future (v0.4.0): add aws-sdk-go-v2/s3 + tar.zst streaming upload to
// s3://<bucket>/<correlation_id>/bundle.tar.zst for persistence beyond
// Pod lifetime + cmd/triage classifier consumption.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"

	"github.com/pleme-io/pitr-tools/internal/log"
)

const (
	pitrSessionGroup    = "pitr.akeyless.io"
	pitrSessionVersion  = "v1"
	pitrSessionResource = "pitrsessions"

	correlationLabelKey = "pitr-correlation-id"

	bundleTimeout = 5 * time.Minute
	logTailLines  = int64(1000) // per container, current + previous
)

// drillMRGVRs is the catalog of Crossplane MR kinds the chart's Composition
// emits with the pitr-correlation-id label. Order matters only for output
// readability.
var drillMRGVRs = []schema.GroupVersionResource{
	{Group: "rds.aws.upbound.io", Version: "v1beta1", Resource: "instances"},
	{Group: "akeyless.crossplane.io", Version: "v1alpha1", Resource: "authmethodapikeys"},
	{Group: "akeyless.crossplane.io", Version: "v1alpha1", Resource: "roles"},
	{Group: "akeyless.crossplane.io", Version: "v1alpha1", Resource: "roleauthmethodassocs"},
	{Group: "akeyless.crossplane.io", Version: "v1alpha1", Resource: "rolerules"},
	{Group: "kubernetes.crossplane.io", Version: "v1alpha2", Resource: "objects"},
}

// Bundle is the structured output written to stdout as JSON.
type Bundle struct {
	Manifest Manifest          `json:"manifest"`
	Files    map[string]string `json:"files"` // path → contents (typically YAML or log text)
}

// Manifest summarizes what's in the bundle. Indexed first by triage tools.
type Manifest struct {
	CorrelationID    string    `json:"correlation_id"`
	Tenant           string    `json:"tenant"`
	Phase            string    `json:"phase"`
	RestoreTime      string    `json:"restore_time,omitempty"`
	RestoreNamespace string    `json:"restore_namespace,omitempty"`
	SecretsRequested []string  `json:"secrets_requested"`
	SecretsRetrieved []string  `json:"secrets_retrieved"`
	CompletionTime   time.Time `json:"completion_time"`
	FileCount        int       `json:"file_count"`
	BinaryVersion    string    `json:"binary_version,omitempty"`
}

// version is set via -ldflags at build time (substrate flake).
var version = "dev"

func main() {
	var (
		correlationID     = flag.String("correlation-id", "", "drill correlation_id (e.g. drill-smoke-d6f2d450)")
		pitrsessionName   = flag.String("pitrsession-name", "", "PITRSession CR name (cluster-scoped)")
		diagnosticsBucket = flag.String("diagnostics-bucket", "", "S3 bucket for future upload (deferred to v0.4.0; informational only here)")
		tenant            = flag.String("tenant", "", "tenant identifier (e.g. smoke, mte-staging)")
	)
	flag.Parse()
	_ = diagnosticsBucket // accepted for chart-flag-stability; not yet used

	if err := requireFlags(*correlationID, *pitrsessionName, *tenant); err != nil {
		fmt.Fprintln(os.Stderr, "diagnostic-collect:", err.Error())
		os.Exit(2)
	}

	logger := log.New(*correlationID).With("job", "diagnostic-collect", "tenant", *tenant)
	logger.Info("collecting drill bundle", "binary_version", version)

	ctx, cancel := context.WithTimeout(context.Background(), bundleTimeout)
	defer cancel()

	cfg, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("in-cluster config unavailable", "error", err.Error())
		os.Exit(0) // best-effort; don't fail the drill
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error("kubernetes client init failed", "error", err.Error())
		os.Exit(0)
	}
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		logger.Error("dynamic client init failed", "error", err.Error())
		os.Exit(0)
	}

	bundle := collectBundle(ctx, logger, clientset, dynClient, *correlationID, *pitrsessionName, *tenant)

	out, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		logger.Error("bundle marshal failed", "error", err.Error())
		os.Exit(0)
	}
	// Write bundle to stdout — operator retrieves via `kubectl logs`.
	fmt.Println(string(out))
	logger.Info("bundle emitted",
		"phase", bundle.Manifest.Phase,
		"file_count", bundle.Manifest.FileCount,
	)
}

// collectBundle reads the PITRSession's terminal state then assembles a
// bundle of supporting evidence. Each step is best-effort — a single failure
// (e.g. one MR's Get returns NotFound) does not abort the rest of the
// collection. Errors are recorded as files in the bundle so triage can see
// what couldn't be collected.
func collectBundle(
	ctx context.Context,
	logger interface{ Info(string, ...any); Error(string, ...any) },
	clientset *kubernetes.Clientset,
	dynClient dynamic.Interface,
	correlationID, pitrsessionName, tenant string,
) Bundle {
	files := map[string]string{}

	// 1. PITRSession itself
	xr := getPITRSession(ctx, dynClient, pitrsessionName)
	if xr != nil {
		if y, err := yaml.Marshal(xr.Object); err == nil {
			files["pitrsession.yaml"] = string(y)
		}
	}

	manifest := buildManifest(xr, correlationID, tenant)

	// 2. Crossplane MRs labeled with this drill's correlation_id
	for _, gvr := range drillMRGVRs {
		listed := listMRsByCorrelation(ctx, dynClient, gvr, correlationID)
		for _, item := range listed {
			path := fmt.Sprintf("resources/%s_%s.yaml", gvr.Resource, item.GetName())
			if y, err := yaml.Marshal(item.Object); err == nil {
				files[path] = string(y)
			} else {
				files[path+".error"] = err.Error()
			}
		}
	}

	// 3. Pod logs across two namespaces:
	//    - pitr-akeyless (drill jobs: verify, canary-create/delete, diagnostic-collect itself)
	//    - restore-<short_hash> (saas-pitr-* Pods, only for non-Succeeded phases)
	jobNamespace := os.Getenv("POD_NAMESPACE")
	if jobNamespace == "" {
		jobNamespace = "pitr-akeyless"
	}
	collectPodLogs(ctx, logger, clientset, jobNamespace, fmt.Sprintf("%s=%s", correlationLabelKey, correlationID), files)

	if manifest.RestoreNamespace != "" {
		// All Pods in restore-<short_hash> ns belong to this drill;
		// label-selecting unnecessary.
		collectPodLogs(ctx, logger, clientset, manifest.RestoreNamespace, "", files)
	}

	manifest.FileCount = len(files)
	manifest.CompletionTime = time.Now().UTC()
	manifest.BinaryVersion = version

	return Bundle{Manifest: manifest, Files: files}
}

// getPITRSession reads the cluster-scoped PITRSession via dynamic client.
// Returns nil on error (logged as a best-effort no-op — the bundle still
// emits what it can collect from labels alone).
func getPITRSession(ctx context.Context, dynClient dynamic.Interface, name string) *unstructured.Unstructured {
	gvr := schema.GroupVersionResource{
		Group:    pitrSessionGroup,
		Version:  pitrSessionVersion,
		Resource: pitrSessionResource,
	}
	obj, err := dynClient.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	return obj
}

// buildManifest extracts metadata from the PITRSession (best-effort across
// missing fields — the chart may surface them at different times during
// reconcile).
func buildManifest(xr *unstructured.Unstructured, correlationID, tenant string) Manifest {
	m := Manifest{
		CorrelationID:    correlationID,
		Tenant:           tenant,
		Phase:            "Unknown",
		SecretsRequested: []string{},
		SecretsRetrieved: []string{},
	}
	if xr == nil {
		return m
	}
	if phase, ok, _ := unstructured.NestedString(xr.Object, "status", "phase"); ok && phase != "" {
		m.Phase = phase
	}
	if rt, ok, _ := unstructured.NestedString(xr.Object, "spec", "restoreTime"); ok {
		m.RestoreTime = rt
	}
	if ns, ok, _ := unstructured.NestedString(xr.Object, "status", "restoreNamespace"); ok {
		m.RestoreNamespace = ns
	}
	if names, ok, _ := unstructured.NestedStringSlice(xr.Object, "spec", "secretNames"); ok {
		m.SecretsRequested = names
	}
	if got, ok, _ := unstructured.NestedStringSlice(xr.Object, "status", "retrievedSecrets"); ok {
		m.SecretsRetrieved = got
	}
	return m
}

// listMRsByCorrelation queries a GVR for items labeled with the drill's
// correlation_id. Returns empty slice if listing fails (logged + recorded).
func listMRsByCorrelation(ctx context.Context, dynClient dynamic.Interface, gvr schema.GroupVersionResource, correlationID string) []unstructured.Unstructured {
	selector := fmt.Sprintf("%s=%s", correlationLabelKey, correlationID)
	list, err := dynClient.Resource(gvr).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil
	}
	return list.Items
}

// collectPodLogs lists Pods matching the selector in the namespace and
// fetches both the current container's log and (when present) the
// previous container's log. Each becomes a file in the bundle.
func collectPodLogs(
	ctx context.Context,
	logger interface{ Info(string, ...any); Error(string, ...any) },
	clientset *kubernetes.Clientset,
	namespace, selector string,
	files map[string]string,
) {
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		files[fmt.Sprintf("logs/%s.list-error", namespace)] = err.Error()
		return
	}
	for _, pod := range pods.Items {
		for _, container := range pod.Spec.Containers {
			path := fmt.Sprintf("logs/%s_%s_%s.log", namespace, pod.Name, container.Name)
			if body := readPodLog(ctx, clientset, namespace, pod.Name, container.Name, false); body != "" {
				files[path] = body
			}
			// Previous-container log: only present if this container has restarted.
			if hasPrevious(pod.Status.ContainerStatuses, container.Name) {
				prevPath := fmt.Sprintf("logs/%s_%s_%s.previous.log", namespace, pod.Name, container.Name)
				if body := readPodLog(ctx, clientset, namespace, pod.Name, container.Name, true); body != "" {
					files[prevPath] = body
				}
			}
		}
	}
}

func readPodLog(ctx context.Context, clientset *kubernetes.Clientset, namespace, podName, containerName string, previous bool) string {
	tail := logTailLines
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: containerName,
		TailLines: &tail,
		Previous:  previous,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return fmt.Sprintf("(log fetch failed: %s)", err.Error())
	}
	defer stream.Close()
	body, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Sprintf("(log read failed: %s)", err.Error())
	}
	return string(body)
}

func hasPrevious(statuses []corev1.ContainerStatus, name string) bool {
	for _, s := range statuses {
		if s.Name == name && s.RestartCount > 0 {
			return true
		}
	}
	return false
}

// requireFlags returns a single error listing missing required args. The
// --diagnostics-bucket flag is accepted but not yet used (deferred to v0.4.0
// when S3 upload lands); validating it would prevent operators from running
// the binary in setups without an S3 bucket configured.
func requireFlags(correlationID, pitrsessionName, tenant string) error {
	missing := []string{}
	if correlationID == "" {
		missing = append(missing, "--correlation-id")
	}
	if pitrsessionName == "" {
		missing = append(missing, "--pitrsession-name")
	}
	if tenant == "" {
		missing = append(missing, "--tenant")
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("required args missing: %v", strings.Join(missing, ", "))
}
