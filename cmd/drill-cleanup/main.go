// Command drill-cleanup finds and deletes orphaned AWS RDS instances left
// over from PITR drills whose Crossplane MR teardown didn't propagate to
// AWS. The recurring failure mode (drills #4, #5, #6 in ASM-17780):
//
//  1. Operator deletes PITRSession (or helm-upgrades during teardown)
//  2. Crossplane GCs the K8s MR for the RDS Instance
//  3. AWS-side `DeleteDBInstance` doesn't propagate
//  4. AWS instance stays `available` indefinitely, costing money
//
// drill-cleanup audits AWS for RDS instances tagged with the drill
// `pitr-correlation-id` label and confirms which are truly orphaned (no
// matching Crossplane MR in K8s) before deleting. Operator-side tool.
//
// Modes:
//
//	# List all drill-tagged AWS RDS instances + their orphan status
//	drill-cleanup list
//
//	# Delete every orphan (no live K8s MR, drill correlation present)
//	drill-cleanup clean [--dry-run]
//
//	# Delete every RDS instance for a specific drill correlation
//	drill-cleanup clean --correlation-id 3054b9e0 [--dry-run]
//
// Per project idiom (Decision 19): typed Go, structured internal/log
// output, no shell loops. Replaces the ad-hoc `aws rds delete-db-instance
// --skip-final-snapshot` × 4 ritual we've run after every failed drill.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/pleme-io/pitr-tools/internal/log"
)

const (
	correlationTagKey = "pitr-correlation-id"
	rdsAPIGroup       = "rds.aws.upbound.io"
	rdsAPIVersion     = "v1beta1"
	rdsResource       = "instances"
)

// orphanInstance pairs an AWS RDS instance with its drill correlation
// (extracted from tags) and a flag indicating whether a matching
// Crossplane MR still exists in K8s (= NOT orphaned, drill is live).
type orphanInstance struct {
	Identifier    string
	Status        string
	CorrelationID string
	HasLiveMR     bool
}

func main() {
	var (
		region        = flag.String("region", "us-east-2", "AWS region")
		correlationID = flag.String("correlation-id", "", "filter to a specific drill correlation; empty = all drill-tagged instances")
		kubeconfig    = flag.String("kubeconfig", os.Getenv("KUBECONFIG"), "path to kubeconfig (defaults to $KUBECONFIG)")
		dryRun        = flag.Bool("dry-run", false, "report only; do not delete")
		// feasibility-mode flags
		restoreTime  = flag.String("restore-time", "", "[feasibility] candidate PITR restoreTime (RFC3339, e.g. 2026-05-09T19:42:09Z)")
		sourceDbs    = flag.String("source-dbs", "", "[feasibility] comma-separated DBInstanceIdentifiers to validate against")
		safetyMargin = flag.Duration("safety-margin", 60*time.Second, "[feasibility] subtract from each source's LatestRestorableTime; guards against the time gap between this check and the actual PITR call")
	)
	flag.Parse()

	args := flag.Args()
	mode := "list"
	if len(args) > 0 {
		mode = args[0]
	}

	logger := log.New(*correlationID).With("job", "drill-cleanup")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(*region))
	if err != nil {
		logger.Error("aws config load failed", "error", err.Error())
		os.Exit(1)
	}
	rdsClient := rds.NewFromConfig(awsCfg)

	// feasibility mode skips the k8s dependency — pure AWS query, useful
	// in pre-flight contexts where the operator hasn't applied a
	// PITRSession yet (kubeconfig may be stale or pointed at a different
	// cluster). Branch BEFORE the k8s dynamic client init.
	if mode == "feasibility" {
		if err := checkFeasibility(ctx, logger, rdsClient, *restoreTime, *sourceDbs, *safetyMargin); err != nil {
			logger.Error("feasibility check failed", "error", err.Error())
			os.Exit(2)
		}
		logger.Info("restoreTime is feasible across all source DBs", "restore_time", *restoreTime)
		return
	}

	dynClient, err := newDynamicClient(*kubeconfig)
	if err != nil {
		logger.Error("k8s dynamic client init failed (is KUBECONFIG set?)", "error", err.Error())
		os.Exit(1)
	}

	orphans, err := findOrphans(ctx, logger, rdsClient, dynClient, *correlationID)
	if err != nil {
		logger.Error("orphan scan failed", "error", err.Error())
		os.Exit(1)
	}

	switch mode {
	case "list":
		printList(orphans)
		// non-zero exit if any true orphans (no live MR) — operator can
		// hook this into CI/cron.
		for _, o := range orphans {
			if !o.HasLiveMR {
				os.Exit(2)
			}
		}
	case "clean":
		if err := cleanOrphans(ctx, logger, rdsClient, orphans, *dryRun); err != nil {
			logger.Error("cleanup failed", "error", err.Error())
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "drill-cleanup: unknown mode %q (use 'list', 'clean', or 'feasibility')\n", mode)
		os.Exit(2)
	}
}

// checkFeasibility queries DescribeDBInstances for each source DB and
// confirms the requested restoreTime is at least safetyMargin earlier
// than every source's LatestRestorableTime. Pre-flight gate operators
// run BEFORE applying a PITRSession — runs in seconds, no AWS PITR cost,
// no chart deploy. Saves the full ~30s drill cycle when the chosen
// restoreTime would have been rejected by AWS.
//
//	drill-cleanup -region us-east-2 \
//	  -restore-time 2026-05-09T19:42:09Z \
//	  -source-dbs mte-staging-auth,mte-staging-uam,mte-staging-unified \
//	  feasibility
//
// Exit codes:
//
//	0 — restoreTime is feasible across ALL listed sources
//	2 — at least one source's LatestRestorableTime < restoreTime + safety margin;
//	    diagnostic identifies which DB and how many seconds short
func checkFeasibility(ctx context.Context, logger interface {
	Info(string, ...any)
	Error(string, ...any)
}, rdsClient *rds.Client, restoreTimeStr, sourceDbsStr string, safetyMargin time.Duration) error {
	if restoreTimeStr == "" {
		return fmt.Errorf("--restore-time required (RFC3339 e.g. 2026-05-09T19:42:09Z)")
	}
	if sourceDbsStr == "" {
		return fmt.Errorf("--source-dbs required (comma-separated DBInstanceIdentifiers)")
	}
	restoreTime, err := time.Parse(time.RFC3339, restoreTimeStr)
	if err != nil {
		return fmt.Errorf("parse --restore-time: %w", err)
	}
	dbs := strings.Split(sourceDbsStr, ",")
	type result struct {
		db     string
		latest time.Time
		ok     bool
		lagSec int64
	}
	results := make([]result, 0, len(dbs))
	allOK := true
	for _, db := range dbs {
		db = strings.TrimSpace(db)
		if db == "" {
			continue
		}
		out, err := rdsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(db),
		})
		if err != nil {
			results = append(results, result{db: db, ok: false})
			logger.Error("describe-db-instances failed", "db", db, "error", err.Error())
			allOK = false
			continue
		}
		if len(out.DBInstances) == 0 || out.DBInstances[0].LatestRestorableTime == nil {
			results = append(results, result{db: db, ok: false})
			logger.Error("LatestRestorableTime not available", "db", db)
			allOK = false
			continue
		}
		latest := *out.DBInstances[0].LatestRestorableTime
		ceiling := latest.Add(-safetyMargin)
		ok := !restoreTime.After(ceiling)
		lag := int64(restoreTime.Sub(latest).Seconds())
		results = append(results, result{db: db, latest: latest, ok: ok, lagSec: lag})
		if !ok {
			allOK = false
		}
	}
	for _, r := range results {
		if r.ok {
			fmt.Printf("OK     %-40s latest=%s\n", r.db, r.latest.UTC().Format(time.RFC3339))
		} else if r.latest.IsZero() {
			fmt.Printf("ERROR  %-40s describe failed or no LatestRestorableTime\n", r.db)
		} else {
			fmt.Printf("FAIL   %-40s latest=%s  restore-time is %ds AHEAD of latest (need negative)\n",
				r.db, r.latest.UTC().Format(time.RFC3339), r.lagSec)
		}
	}
	if !allOK {
		return fmt.Errorf("at least one source DB rejects the requested restoreTime")
	}
	return nil
}

// findOrphans queries AWS RDS for every instance tagged with a drill
// correlation_id, then cross-checks each against K8s Crossplane MRs to
// determine "live" vs "orphaned" status.
func findOrphans(
	ctx context.Context,
	logger interface{ Info(string, ...any); Error(string, ...any) },
	rdsClient *rds.Client,
	dynClient dynamic.Interface,
	filterCorrelation string,
) ([]orphanInstance, error) {
	// 1. List ALL RDS instances; tag-based filtering happens client-side
	//    because RDS API can't query by tag value efficiently.
	out, err := rdsClient.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{})
	if err != nil {
		return nil, fmt.Errorf("describe-db-instances: %w", err)
	}

	// 2. Filter to drill-tagged + extract correlation per instance
	var candidates []orphanInstance
	for _, inst := range out.DBInstances {
		corr := extractCorrelation(inst.TagList)
		if corr == "" {
			continue // not a drill instance
		}
		if filterCorrelation != "" && !strings.Contains(corr, filterCorrelation) {
			continue
		}
		candidates = append(candidates, orphanInstance{
			Identifier:    aws.ToString(inst.DBInstanceIdentifier),
			Status:        aws.ToString(inst.DBInstanceStatus),
			CorrelationID: corr,
		})
	}

	// 3. Cross-check K8s for live Crossplane MRs per correlation
	liveCorrelations, err := liveCrossplaneCorrelations(ctx, dynClient)
	if err != nil {
		// If K8s lookup fails, we conservatively treat ALL as live (don't
		// delete) — operator must investigate manually.
		logger.Error("k8s cross-check failed; treating all instances as live to avoid accidental deletion", "error", err.Error())
		for i := range candidates {
			candidates[i].HasLiveMR = true
		}
		return candidates, nil
	}

	for i := range candidates {
		// "Live" if the correlation has at least one MR in K8s.
		candidates[i].HasLiveMR = liveCorrelations[candidates[i].CorrelationID]
	}
	return candidates, nil
}

// extractCorrelation looks for the pitr-correlation-id tag in an RDS
// instance's TagList and returns its value, or "" if not present.
func extractCorrelation(tags []rdstypes.Tag) string {
	for _, t := range tags {
		if aws.ToString(t.Key) == correlationTagKey {
			return aws.ToString(t.Value)
		}
	}
	return ""
}

// liveCrossplaneCorrelations returns the set of correlation_ids that
// currently have at least one Crossplane RDS Instance MR in K8s. Used to
// determine which AWS instances still have a "live" K8s side.
func liveCrossplaneCorrelations(ctx context.Context, dynClient dynamic.Interface) (map[string]bool, error) {
	gvr := schema.GroupVersionResource{Group: rdsAPIGroup, Version: rdsAPIVersion, Resource: rdsResource}
	list, err := dynClient.Resource(gvr).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("k8s list instances: %w", err)
	}
	live := map[string]bool{}
	for _, item := range list.Items {
		labels := item.GetLabels()
		if corr, ok := labels[correlationTagKey]; ok && corr != "" {
			live[corr] = true
		}
	}
	return live, nil
}

// cleanOrphans deletes every RDS instance in the orphans slice that has
// HasLiveMR=false. Skip-final-snapshot is set so deletion is immediate.
// In --dry-run mode, prints what would be deleted without calling AWS.
func cleanOrphans(
	ctx context.Context,
	logger interface{ Info(string, ...any); Error(string, ...any) },
	rdsClient *rds.Client,
	orphans []orphanInstance,
	dryRun bool,
) error {
	deleted := 0
	skipped := 0
	for _, o := range orphans {
		if o.HasLiveMR {
			logger.Info("SKIP — live Crossplane MR still references this instance",
				"identifier", o.Identifier,
				"correlation", o.CorrelationID,
				"status", o.Status,
			)
			skipped++
			continue
		}
		if dryRun {
			logger.Info("DRY-RUN — would delete",
				"identifier", o.Identifier,
				"correlation", o.CorrelationID,
				"status", o.Status,
			)
			deleted++
			continue
		}
		_, err := rdsClient.DeleteDBInstance(ctx, &rds.DeleteDBInstanceInput{
			DBInstanceIdentifier:   aws.String(o.Identifier),
			SkipFinalSnapshot:      aws.Bool(true),
			DeleteAutomatedBackups: aws.Bool(true),
		})
		if err != nil {
			logger.Error("delete failed",
				"identifier", o.Identifier,
				"correlation", o.CorrelationID,
				"error", err.Error(),
			)
			continue
		}
		logger.Info("deleted",
			"identifier", o.Identifier,
			"correlation", o.CorrelationID,
		)
		deleted++
	}
	logger.Info("cleanup summary", "deleted", deleted, "skipped_live", skipped, "dry_run", dryRun)
	return nil
}

// printList outputs all candidates in a fixed-column format. Operator-
// readable; use `drill-cleanup clean` for action.
func printList(orphans []orphanInstance) {
	if len(orphans) == 0 {
		fmt.Println("No drill-tagged AWS RDS instances found.")
		return
	}
	fmt.Printf("%-45s %-12s %-32s %s\n", "IDENTIFIER", "STATUS", "CORRELATION", "ORPHAN?")
	for _, o := range orphans {
		marker := "ORPHAN"
		if o.HasLiveMR {
			marker = "live"
		}
		fmt.Printf("%-45s %-12s %-32s %s\n", o.Identifier, o.Status, o.CorrelationID, marker)
	}
}

// newDynamicClient builds a dynamic K8s client from a kubeconfig path.
// Uses the operator's current context.
func newDynamicClient(kubeconfigPath string) (dynamic.Interface, error) {
	if kubeconfigPath == "" {
		// Fall back to default ~/.kube/config or in-cluster
		loader := clientcmd.NewDefaultClientConfigLoadingRules()
		cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loader, &clientcmd.ConfigOverrides{}).ClientConfig()
		if err != nil {
			return nil, err
		}
		return dynamic.NewForConfig(cfg)
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(cfg)
}
