package main

import (
	"strings"
	"testing"
)

func TestRequireFlags(t *testing.T) {
	tests := []struct {
		name                                                      string
		correlationID, pitrsessionName, diagnosticsBucket, tenant string
		wantErr                                                   bool
	}{
		{
			name:              "all set",
			correlationID:     "drill-mte-staging-3a7f9c2b",
			pitrsessionName:   "drill-2026-q2-mte-staging",
			diagnosticsBucket: "mte-staging-pitr-diagnostics-us-east-2",
			tenant:            "mte-staging",
			wantErr:           false,
		},
		{
			name:    "all empty",
			wantErr: true,
		},
		{
			name:              "missing tenant only",
			correlationID:     "drill-mte-staging-3a7f9c2b",
			pitrsessionName:   "drill-2026-q2-mte-staging",
			diagnosticsBucket: "mte-staging-pitr-diagnostics-us-east-2",
			wantErr:           true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := requireFlags(tc.correlationID, tc.pitrsessionName, tc.diagnosticsBucket, tc.tenant)
			if (err != nil) != tc.wantErr {
				t.Fatalf("requireFlags wantErr=%v got=%v", tc.wantErr, err)
			}
		})
	}
}

func TestReadPITRSessionPhase_Stub(t *testing.T) {
	// Stub returns "Unknown" for any non-empty name during integration-branch
	// test era. Verify the contract.
	got, err := readPITRSessionPhase("drill-2026-q2-mte-staging")
	if err != nil {
		t.Fatalf("readPITRSessionPhase: %v", err)
	}
	if got != "Unknown" {
		t.Errorf("stub should return 'Unknown' until drill iteration replaces with K8s API; got %q", got)
	}
}

func TestReadPITRSessionPhase_EmptyName(t *testing.T) {
	_, err := readPITRSessionPhase("")
	if err == nil {
		t.Fatal("readPITRSessionPhase: expected error on empty name; got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error should mention 'empty'; got: %v", err)
	}
}
