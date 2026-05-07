package main

import (
	"strings"
	"testing"
)

func TestComposeMessage(t *testing.T) {
	tests := []struct {
		name          string
		phase         string
		correlationID string
		tenant        string
		bundleURL     string
		wantContains  []string
		wantOmits     []string
	}{
		{
			name:          "Succeeded — bundle URL ignored",
			phase:         "Succeeded",
			correlationID: "drill-mte-staging-3a7f9c2b",
			tenant:        "mte-staging",
			bundleURL:     "https://bundle.example/x",
			wantContains:  []string{":white_check_mark:", "Succeeded", "mte-staging", "3a7f9c2b"},
			wantOmits:     []string{"bundle.example", "diagnostic"},
		},
		{
			name:          "Failed — bundle URL surfaced",
			phase:         "Failed",
			correlationID: "drill-mte-staging-7d8e1a02",
			tenant:        "mte-staging",
			bundleURL:     "https://bundle.example/y",
			wantContains:  []string{":x:", "Failed", "mte-staging", "bundle.example/y", "diagnostic bundle"},
		},
		{
			name:          "Failed without bundle URL — no dangling bundle line",
			phase:         "Failed",
			correlationID: "drill-mte-staging-c4f50bd9",
			tenant:        "mte-staging",
			bundleURL:     "",
			wantContains:  []string{":x:", "Failed", "c4f50bd9"},
			wantOmits:     []string{"diagnostic bundle"},
		},
		{
			name:          "Started",
			phase:         "Started",
			correlationID: "drill-mte-staging-aaaa0001",
			tenant:        "mte-staging",
			wantContains:  []string{":hourglass_flowing_sand:", "Started"},
		},
		{
			name:          "Canceled",
			phase:         "Canceled",
			correlationID: "drill-mte-staging-bbbb0002",
			tenant:        "mte-staging",
			wantContains:  []string{":warning:", "Canceled"},
		},
		{
			name:          "Unknown phase falls back to info emoji",
			phase:         "InProgress",
			correlationID: "drill-mte-staging-cccc0003",
			tenant:        "mte-staging",
			wantContains:  []string{":information_source:", "InProgress"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := composeMessage(tc.phase, tc.correlationID, tc.tenant, tc.bundleURL)
			for _, want := range tc.wantContains {
				if !strings.Contains(got, want) {
					t.Errorf("composeMessage missing %q: %s", want, got)
				}
			}
			for _, omit := range tc.wantOmits {
				if strings.Contains(got, omit) {
					t.Errorf("composeMessage should not contain %q: %s", omit, got)
				}
			}
		})
	}
}
