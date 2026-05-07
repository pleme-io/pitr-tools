package akeyless

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Most exported behavior is wrapper-thin around generated SDK calls — the
// SDK itself is well-tested upstream. These tests cover the input
// validation + SA-token-loading code path which is our actual contract.

func TestNewClient_RejectsEmptyConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  AuthConfig
		want string
	}{
		{
			name: "empty gateway URL",
			cfg:  AuthConfig{AccessID: "p-abc", ConfigName: "mte-staging"},
			want: "GatewayURL",
		},
		{
			name: "empty access ID",
			cfg:  AuthConfig{GatewayURL: "https://gw", ConfigName: "mte-staging"},
			want: "AccessID",
		},
		{
			name: "empty config name",
			cfg:  AuthConfig{GatewayURL: "https://gw", AccessID: "p-abc"},
			want: "ConfigName",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewClient(context.Background(), tc.cfg)
			if err == nil {
				t.Fatal("NewClient: expected error, got nil")
			}
			if !contains(err.Error(), tc.want) {
				t.Errorf("error should mention %q; got: %v", tc.want, err)
			}
		})
	}
}

func TestNewClient_MissingSATokenFile(t *testing.T) {
	_, err := NewClient(context.Background(), AuthConfig{
		GatewayURL:  "https://gw",
		AccessID:    "p-abc",
		ConfigName:  "mte-staging",
		SATokenPath: "/does/not/exist/token",
	})
	if err == nil {
		t.Fatal("NewClient: expected error on missing SA token, got nil")
	}
	if !contains(err.Error(), "read SA token") {
		t.Errorf("error should mention 'read SA token'; got: %v", err)
	}
}

func TestNewClient_EmptySATokenFile(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("   \n"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := NewClient(context.Background(), AuthConfig{
		GatewayURL:  "https://gw",
		AccessID:    "p-abc",
		ConfigName:  "mte-staging",
		SATokenPath: tokenPath,
	})
	if err == nil {
		t.Fatal("NewClient: expected error on empty SA token, got nil")
	}
	if !contains(err.Error(), "is empty") {
		t.Errorf("error should mention 'is empty'; got: %v", err)
	}
}

func TestErrNotFound_IsComparable(t *testing.T) {
	wrapped := errors.New("akeyless: item not found: /drill-canary/abc")
	// Sanity that our naming convention works with errors.Is when wrapped properly
	// elsewhere in the package (the wrapping happens via fmt.Errorf("%w: ...", ErrNotFound)).
	chained := errors.Join(ErrNotFound, wrapped)
	if !errors.Is(chained, ErrNotFound) {
		t.Error("errors.Is should find ErrNotFound in joined error")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
