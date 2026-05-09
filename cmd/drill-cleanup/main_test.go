package main

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

func TestExtractCorrelation_Present(t *testing.T) {
	tags := []rdstypes.Tag{
		{Key: aws.String("env"), Value: aws.String("staging")},
		{Key: aws.String("pitr-correlation-id"), Value: aws.String("drill-smoke-d6f2d450")},
		{Key: aws.String("pitr-tenant"), Value: aws.String("smoke")},
	}
	got := extractCorrelation(tags)
	if got != "drill-smoke-d6f2d450" {
		t.Errorf("expected drill-smoke-d6f2d450, got %q", got)
	}
}

func TestExtractCorrelation_Missing(t *testing.T) {
	tags := []rdstypes.Tag{
		{Key: aws.String("env"), Value: aws.String("staging")},
		{Key: aws.String("Name"), Value: aws.String("mte-staging-auth")},
	}
	if got := extractCorrelation(tags); got != "" {
		t.Errorf("expected empty for non-drill tags, got %q", got)
	}
}

func TestExtractCorrelation_EmptyTags(t *testing.T) {
	if got := extractCorrelation(nil); got != "" {
		t.Errorf("expected empty for nil tags, got %q", got)
	}
}

func TestOrphanInstance_OrphanLogic(t *testing.T) {
	// orphan = drill-tagged AWS instance with no live K8s MR
	live := map[string]bool{
		"drill-smoke-aaa111": true,
	}
	cases := []struct {
		correlation string
		wantOrphan  bool
	}{
		{"drill-smoke-aaa111", false},
		{"drill-smoke-bbb222", true},
		{"drill-smoke-ccc333", true},
	}
	for _, tc := range cases {
		got := !live[tc.correlation]
		if got != tc.wantOrphan {
			t.Errorf("correlation %q: orphan=%v, want %v", tc.correlation, got, tc.wantOrphan)
		}
	}
}

func TestPrintList_NoPanic(t *testing.T) {
	printList(nil)
	printList([]orphanInstance{
		{Identifier: "terraform-12345", Status: "available", CorrelationID: "drill-smoke-abc", HasLiveMR: false},
		{Identifier: "pitr-d6f2d450-auth", Status: "available", CorrelationID: "drill-smoke-d6f2d450", HasLiveMR: true},
	})
}

func TestStringsContains_FilterPattern(t *testing.T) {
	if !strings.Contains("drill-smoke-d6f2d450", "d6f2d450") {
		t.Error("strings.Contains should match short-hash within full correlation")
	}
}
