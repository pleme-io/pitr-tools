package main

import (
	"strings"
	"testing"
)

func TestRequireFlags(t *testing.T) {
	tests := []struct {
		name                                           string
		correlationID, sourceURL, accessID, configName string
		wantErr                                        bool
		wantMentions                                   []string
	}{
		{
			name:          "all set",
			correlationID: "drill-mte-staging-3a7f9c2b",
			sourceURL:     "https://gw",
			accessID:      "p-abc",
			configName:    "mte-staging-pitr",
			wantErr:       false,
		},
		{
			name:         "all empty",
			wantErr:      true,
			wantMentions: []string{"--correlation-id", "--source-akeyless-url"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := requireFlags(tc.correlationID, tc.sourceURL, tc.accessID, tc.configName)
			if (err != nil) != tc.wantErr {
				t.Fatalf("requireFlags wantErr=%v got=%v", tc.wantErr, err)
			}
			if err != nil {
				for _, m := range tc.wantMentions {
					if !strings.Contains(err.Error(), m) {
						t.Errorf("error should mention %q; got: %v", m, err)
					}
				}
			}
		})
	}
}
