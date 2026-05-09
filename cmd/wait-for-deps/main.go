// Command wait-for-deps blocks until every supplied HTTP /health endpoint
// returns 200 OK or until --max-wait elapses, OR exits early non-zero when
// any --watch-pod target enters an unrecoverable state (ImagePullBackOff,
// CrashLoop with restartCount over threshold, etc.). Designed for Pod
// initContainer use: replaces shell `until curl ... do sleep ...` loops with
// a typed Go binary using the project's standard internal/log structured
// logging.
//
// Per Decision 19 (Job/initContainer runtime — Go binaries via the
// pitr-tools multi-binary image). Mirrors the shape of canary-create /
// canary-delete / verify / diagnostic-collect / notify so deployers
// pull a single image and select via container.command.
//
//	/wait-for-deps --correlation-id drill-mte-staging-3a7f9c2b \
//	               --service http://saas-pitr-auth-service-ingress.restore-3a7f9c2b.svc.cluster.local/health \
//	               --service http://saas-pitr-uam-service-ingress.restore-3a7f9c2b.svc.cluster.local/health \
//	               --watch-pod restore-3a7f9c2b/app=saas-pitr-auth \
//	               --watch-pod restore-3a7f9c2b/app=saas-pitr-uam \
//	               --max-wait 25m
//
// Behavior:
//
//   - Spawns one goroutine per --service URL that polls every --poll-interval
//     and returns success when the URL returns 200 at least once.
//   - Spawns one goroutine per --watch-pod target that watches Pods via the
//     K8s API and returns an unrecoverable error when any matching Pod
//     enters ImagePullBackOff / CrashLoopBackOff (restartCount over
//     --max-restarts) / CreateContainerError. This converts "wait for the
//     timeout" into "fail fast with structured cause."
//   - Exits 0 when every --service URL has returned 200 (Pod watchers run
//     concurrently as fail-fast guards but don't gate success).
//   - Exits 1 when (a) --max-wait elapses with services still not ready,
//     or (b) any Pod watcher detects an unrecoverable state.
//   - Exits 2 on flag parse error.
//
// Used by: chart 0.9.x KCL Composition (initContainers on KFM/UAM/GATOR
// saas Pods + verify Job). Eliminates the cascade-race we mis-diagnosed
// in chart 0.8.6 by replacing in-process retry budgets with deterministic
// dependency gates. The Pod-watch addition (v0.3.1) eliminates the
// "wait 25 min hoping things resolve" anti-pattern when the upstream
// is observably broken.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/pleme-io/pitr-tools/internal/log"
)

const (
	defaultMaxWait      = 10 * time.Minute
	defaultPollInterval = 5 * time.Second
	defaultMaxRestarts  = 5
	requestTimeout      = 2 * time.Second
)

// unrecoverableImageWaitingReasons enumerates K8s container-waiting reasons
// that indicate an image cannot be obtained — no amount of waiting fixes it.
var unrecoverableImageWaitingReasons = map[string]bool{
	"ImagePullBackOff":   true,
	"ErrImagePull":       true,
	"InvalidImageName":   true,
	"ImageInspectError":  true,
	"RegistryUnavailable": true,
}

// stringSlice is a flag.Value that accumulates repeated --service flags.
type stringSlice []string

func (s *stringSlice) String() string     { return fmt.Sprintf("%v", []string(*s)) }
func (s *stringSlice) Set(v string) error { *s = append(*s, v); return nil }

// loggerIface decouples internal/log from this binary so unit tests can
// supply a silent logger instead of polluting test output.
type loggerIface interface {
	Info(string, ...any)
	Error(string, ...any)
}

func main() {
	var (
		correlationID = flag.String("correlation-id", "", "drill correlation_id (e.g. drill-mte-staging-3a7f9c2b)")
		maxWait       = flag.Duration("max-wait", defaultMaxWait, "give up if all services not ready by this duration")
		pollInterval  = flag.Duration("poll-interval", defaultPollInterval, "interval between /health polls per service")
		maxRestarts   = flag.Int("max-restarts", defaultMaxRestarts, "restartCount threshold for --watch-pod targets — exceeding this = unrecoverable")
		services      stringSlice
		watchPods     stringSlice
	)
	flag.Var(&services, "service", "fully-qualified URL whose /health must return 200 (repeatable; ≥1 required)")
	flag.Var(&watchPods, "watch-pod", "namespace/labelSelector to watch for unrecoverable failure (repeatable; e.g. 'restore-X/app=saas-pitr-auth'); empty = no Pod watch")
	flag.Parse()

	if *correlationID == "" {
		fmt.Fprintln(os.Stderr, "wait-for-deps: --correlation-id is required")
		os.Exit(2)
	}
	if len(services) == 0 {
		fmt.Fprintln(os.Stderr, "wait-for-deps: at least one --service URL required")
		os.Exit(2)
	}

	logger := log.New(*correlationID).With("job", "wait-for-deps")
	logger.Info("waiting for dependencies",
		"services", []string(services),
		"watch_pods", []string(watchPods),
		"max_wait", maxWait.String(),
		"poll_interval", pollInterval.String(),
		"max_restarts", *maxRestarts,
	)

	ctx, cancel := context.WithTimeout(context.Background(), *maxWait)
	defer cancel()

	// In-cluster client only set up if we actually need to watch Pods —
	// keeps the binary usable in test setups without K8s API access.
	var clientset *kubernetes.Clientset
	if len(watchPods) > 0 {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			logger.Error("in-cluster config unavailable; --watch-pod requires it", "error", err.Error())
			os.Exit(2)
		}
		clientset, err = kubernetes.NewForConfig(cfg)
		if err != nil {
			logger.Error("kubernetes client init failed", "error", err.Error())
			os.Exit(2)
		}
	}

	if err := waitAll(ctx, logger, services, watchPods, *pollInterval, *maxRestarts, clientset); err != nil {
		logger.Error("dependencies not ready", "error", err.Error())
		os.Exit(1)
	}
	logger.Info("all dependencies ready")
}

// waitAll spawns concurrent watchers:
//   - one HTTP /health poll per --service URL (success = all 200 at least once)
//   - one K8s Pod watch per --watch-pod target (success = ctx done before any
//     unrecoverable state; failure = unrecoverable state detected)
//
// Returns nil only when every --service has responded 200 AND no Pod watcher
// has reported an unrecoverable state. Returns the first error otherwise.
func waitAll(
	ctx context.Context,
	logger loggerIface,
	services []string,
	watchPods []string,
	interval time.Duration,
	maxRestarts int,
	clientset *kubernetes.Clientset,
) error {
	// failFast cancels the whole waitAll when any watcher reports unrecoverable.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, len(services)+len(watchPods))
	client := &http.Client{Timeout: requestTimeout}

	for _, svc := range services {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			err := waitOne(ctx, logger, client, url, interval)
			errs <- err
		}(svc)
	}

	for _, target := range watchPods {
		wg.Add(1)
		go func(spec string) {
			defer wg.Done()
			err := watchPod(ctx, logger, clientset, spec, maxRestarts)
			if err != nil {
				// Unrecoverable Pod state — cancel everyone else.
				cancel()
				errs <- err
			}
		}(target)
	}

	wg.Wait()
	close(errs)

	var combined []error
	for err := range errs {
		if err != nil {
			combined = append(combined, err)
		}
	}
	return errors.Join(combined...)
}

// waitOne polls a single URL until it returns 200 or ctx is done.
func waitOne(ctx context.Context, logger loggerIface, client *http.Client, url string, interval time.Duration) error {
	attempt := 0
	for {
		attempt++
		if err := probe(ctx, client, url); err == nil {
			logger.Info("service ready", "service", url, "attempts", attempt)
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("service not ready: %s (after %d attempts): %w", url, attempt, ctx.Err())
		case <-time.After(interval):
			// next iteration
		}
	}
}

// probe issues a single GET and returns nil iff status is 200.
func probe(ctx context.Context, client *http.Client, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// watchPod runs a K8s API watch on the parsed `namespace/labelSelector` spec
// and returns an unrecoverable error the moment any matching Pod enters one
// of the failure modes (ImagePullBackOff, restartCount over threshold,
// CreateContainerError, init container error). Returns nil on context
// cancellation (which happens when waitAll is winding down successfully).
func watchPod(ctx context.Context, logger loggerIface, clientset *kubernetes.Clientset, spec string, maxRestarts int) error {
	ns, sel, err := parsePodSpec(spec)
	if err != nil {
		return fmt.Errorf("invalid --watch-pod %q: %w", spec, err)
	}
	logger.Info("watching pod for unrecoverable state", "namespace", ns, "selector", sel, "max_restarts", maxRestarts)

	w, err := clientset.CoreV1().Pods(ns).Watch(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return fmt.Errorf("pod watch failed (%s/%s): %w", ns, sel, err)
	}
	defer w.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil // not an error — caller is shutting down
		case evt, ok := <-w.ResultChan():
			if !ok {
				return nil // watch closed cleanly
			}
			if evt.Type == watch.Error {
				continue // transient watch errors don't fail the binary
			}
			pod, ok := evt.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			if reason := unrecoverableReason(pod, maxRestarts); reason != "" {
				logger.Error("pod unrecoverable",
					"namespace", ns,
					"pod", pod.Name,
					"reason", reason,
				)
				return fmt.Errorf("pod %s/%s unrecoverable: %s", ns, pod.Name, reason)
			}
		}
	}
}

// parsePodSpec splits "namespace/labelSelector" into its parts. The selector
// is the K8s label-selector syntax (key=value or key1=value1,key2=value2).
func parsePodSpec(spec string) (namespace, selector string, err error) {
	parts := strings.SplitN(spec, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("expected 'namespace/labelSelector' (got: %q)", spec)
	}
	return parts[0], parts[1], nil
}

// unrecoverableReason inspects a Pod's status and returns a non-empty string
// describing the unrecoverable cause if one is detected, "" otherwise.
//
// The checks are cumulative: any matching condition triggers a return.
// They're chosen to be high-confidence signals — short-lived transient
// errors should not match (e.g. RestartCount of 1 doesn't trigger; only
// > maxRestarts does).
func unrecoverableReason(pod *corev1.Pod, maxRestarts int) string {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			if unrecoverableImageWaitingReasons[cs.State.Waiting.Reason] {
				return fmt.Sprintf("container %s: %s — %s", cs.Name, cs.State.Waiting.Reason, cs.State.Waiting.Message)
			}
		}
		if int(cs.RestartCount) > maxRestarts {
			return fmt.Sprintf("container %s: restartCount=%d exceeds threshold %d (CrashLoop)", cs.Name, cs.RestartCount, maxRestarts)
		}
	}
	for _, ics := range pod.Status.InitContainerStatuses {
		if ics.State.Waiting != nil {
			if unrecoverableImageWaitingReasons[ics.State.Waiting.Reason] {
				return fmt.Sprintf("init container %s: %s — %s", ics.Name, ics.State.Waiting.Reason, ics.State.Waiting.Message)
			}
			if ics.State.Waiting.Reason == "CreateContainerError" || ics.State.Waiting.Reason == "CreateContainerConfigError" {
				return fmt.Sprintf("init container %s: %s — %s", ics.Name, ics.State.Waiting.Reason, ics.State.Waiting.Message)
			}
		}
	}
	return ""
}
