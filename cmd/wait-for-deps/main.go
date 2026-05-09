// Command wait-for-deps blocks until every supplied HTTP /health endpoint
// returns 200 OK or until --max-wait elapses. Designed for Pod initContainer
// use: replaces shell `until curl ... do sleep ...` loops with a typed Go
// binary using the project's standard internal/log structured logging.
//
// Per Decision 19 (Job/initContainer runtime — Go binaries via the
// pitr-tools multi-binary image). Mirrors the shape of canary-create /
// canary-delete / verify / diagnostic-collect / notify so deployers
// pull a single image and select via container.command.
//
//	/wait-for-deps --correlation-id drill-mte-staging-3a7f9c2b \
//	               --service http://saas-pitr-auth-service-ingress.restore-3a7f9c2b.svc.cluster.local/health \
//	               --service http://saas-pitr-uam-service-ingress.restore-3a7f9c2b.svc.cluster.local/health \
//	               --max-wait 10m
//
// Behavior:
//
//   - For each --service URL, runs a goroutine that polls every --poll-interval.
//   - Exits 0 when every URL has returned 200 at least once.
//   - Exits 1 if --max-wait elapses before all URLs are ready.
//   - Exits 2 on flag parse error.
//
// Used by: chart 0.9.0 KCL Composition (initContainers on KFM/UAM/GATOR
// saas Pods + verify Job). Eliminates the cascade-race we mis-diagnosed
// in chart 0.8.6 by replacing in-process retry budgets with deterministic
// dependency gates.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/pleme-io/pitr-tools/internal/log"
)

const (
	defaultMaxWait      = 10 * time.Minute
	defaultPollInterval = 5 * time.Second
	requestTimeout      = 2 * time.Second
)

// stringSlice is a flag.Value that accumulates repeated --service flags.
type stringSlice []string

func (s *stringSlice) String() string     { return fmt.Sprintf("%v", []string(*s)) }
func (s *stringSlice) Set(v string) error { *s = append(*s, v); return nil }

func main() {
	var (
		correlationID = flag.String("correlation-id", "", "drill correlation_id (e.g. drill-mte-staging-3a7f9c2b)")
		maxWait       = flag.Duration("max-wait", defaultMaxWait, "give up if all services not ready by this duration")
		pollInterval  = flag.Duration("poll-interval", defaultPollInterval, "interval between /health polls per service")
		services      stringSlice
	)
	flag.Var(&services, "service", "fully-qualified URL whose /health must return 200 (repeatable; ≥1 required)")
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
		"max_wait", maxWait.String(),
		"poll_interval", pollInterval.String(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), *maxWait)
	defer cancel()

	if err := waitAll(ctx, logger, services, *pollInterval); err != nil {
		logger.Error("dependencies not ready in time", "error", err.Error())
		os.Exit(1)
	}
	logger.Info("all dependencies ready")
}

// waitAll polls every service URL concurrently and returns nil only when
// every one has responded 200 at least once. Returns ctx.Err() if the
// deadline fires before all are ready.
func waitAll(ctx context.Context, logger interface{ Info(string, ...any); Error(string, ...any) }, services []string, interval time.Duration) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(services))
	client := &http.Client{Timeout: requestTimeout}

	for _, svc := range services {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			errs <- waitOne(ctx, logger, client, url, interval)
		}(svc)
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
func waitOne(ctx context.Context, logger interface{ Info(string, ...any); Error(string, ...any) }, client *http.Client, url string, interval time.Duration) error {
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
