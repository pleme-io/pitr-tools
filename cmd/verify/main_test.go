package main

import (
	"strings"
	"testing"
)

func TestResolveSecretPaths_AutoCanary(t *testing.T) {
	id := "drill-mte-staging-3a7f9c2b"
	got, err := resolveSecretPaths(id, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "/drill-canary/3a7f9c2b" {
		t.Errorf("auto-canary path: got %v, want [/drill-canary/3a7f9c2b]", got)
	}
}

func TestResolveSecretPaths_OperatorSupplied(t *testing.T) {
	got, err := resolveSecretPaths("drill-x-y-deadbeef", "/foo, /bar/baz, /spaces/around ")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/foo", "/bar/baz", "/spaces/around"}
	if len(got) != len(want) {
		t.Fatalf("len mismatch: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %q want %q", i, got[i], want[i])
		}
	}
}

func TestResolveSecretPaths_Trims(t *testing.T) {
	got, err := resolveSecretPaths("drill-x-y-deadbeef", "  /a , , /b ")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "/a" || got[1] != "/b" {
		t.Errorf("trim+filter empties: got %v want [/a /b]", got)
	}
}

func TestResolveSecretPaths_AllEmptyAfterTrim(t *testing.T) {
	_, err := resolveSecretPaths("drill-x-y-deadbeef", " , , ,")
	if err == nil {
		t.Fatal("expected error for all-empty entries; got nil")
	}
}

func TestResolveSecretPaths_BadCorrelationID(t *testing.T) {
	_, err := resolveSecretPaths("not-a-correlation-id", "")
	if err == nil {
		t.Fatal("expected error for malformed correlation-id; got nil")
	}
	if !strings.Contains(err.Error(), "invalid correlation-id format") {
		t.Errorf("error should mention 'invalid correlation-id format'; got: %v", err)
	}
}

func TestRequireFlags(t *testing.T) {
	if err := requireFlags("", "", "", ""); err == nil {
		t.Fatal("requireFlags expected error on all-empty; got nil")
	}
	if err := requireFlags("drill-x-y-deadbeef", "https://gw", "p-abc", "mte-staging-pitr"); err != nil {
		t.Errorf("requireFlags expected nil on full inputs; got: %v", err)
	}
}
