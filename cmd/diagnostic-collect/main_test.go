package main

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestRequireFlags_AllPresent(t *testing.T) {
	if err := requireFlags("drill-smoke-abc", "smoke", "smoke"); err != nil {
		t.Fatalf("expected nil for fully-populated flags, got: %v", err)
	}
}

func TestRequireFlags_MissingCorrelation(t *testing.T) {
	err := requireFlags("", "smoke", "smoke")
	if err == nil || !strings.Contains(err.Error(), "--correlation-id") {
		t.Fatalf("expected error mentioning --correlation-id, got: %v", err)
	}
}

func TestRequireFlags_MissingPITRSessionName(t *testing.T) {
	err := requireFlags("drill-smoke-abc", "", "smoke")
	if err == nil || !strings.Contains(err.Error(), "--pitrsession-name") {
		t.Fatalf("expected error mentioning --pitrsession-name, got: %v", err)
	}
}

func TestRequireFlags_MissingTenant(t *testing.T) {
	err := requireFlags("drill-smoke-abc", "smoke", "")
	if err == nil || !strings.Contains(err.Error(), "--tenant") {
		t.Fatalf("expected error mentioning --tenant, got: %v", err)
	}
}

func TestBuildManifest_NilXR(t *testing.T) {
	// XR can't be read — manifest still gets the operator-supplied bits.
	m := buildManifest(nil, "drill-smoke-abc", "smoke")
	if m.CorrelationID != "drill-smoke-abc" {
		t.Errorf("correlation_id mismatch: %s", m.CorrelationID)
	}
	if m.Tenant != "smoke" {
		t.Errorf("tenant mismatch: %s", m.Tenant)
	}
	if m.Phase != "Unknown" {
		t.Errorf("phase should default to Unknown when XR nil: %s", m.Phase)
	}
	if len(m.SecretsRequested) != 0 {
		t.Errorf("secretsRequested should be empty slice not nil; got: %v", m.SecretsRequested)
	}
}

func TestBuildManifest_FullXR(t *testing.T) {
	xr := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{
				"restoreTime": "2026-05-09T00:31:50Z",
				"secretNames": []any{"/pitr/smoke/baseline"},
			},
			"status": map[string]any{
				"phase":            "Failed",
				"restoreNamespace": "restore-d6f2d450",
				"retrievedSecrets": []any{"/pitr/smoke/baseline"},
			},
		},
	}
	m := buildManifest(xr, "drill-smoke-d6f2d450", "smoke")
	if m.Phase != "Failed" {
		t.Errorf("phase mismatch: %s", m.Phase)
	}
	if m.RestoreTime != "2026-05-09T00:31:50Z" {
		t.Errorf("restoreTime mismatch: %s", m.RestoreTime)
	}
	if m.RestoreNamespace != "restore-d6f2d450" {
		t.Errorf("restoreNamespace mismatch: %s", m.RestoreNamespace)
	}
	if len(m.SecretsRequested) != 1 || m.SecretsRequested[0] != "/pitr/smoke/baseline" {
		t.Errorf("secretsRequested mismatch: %v", m.SecretsRequested)
	}
	if len(m.SecretsRetrieved) != 1 {
		t.Errorf("secretsRetrieved mismatch: %v", m.SecretsRetrieved)
	}
}

func TestBuildManifest_PartialStatus(t *testing.T) {
	// Mid-flight XR: spec but no status fields populated.
	xr := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{
				"restoreTime": "2026-05-09T00:31:50Z",
			},
		},
	}
	m := buildManifest(xr, "drill-smoke-x", "smoke")
	if m.Phase != "Unknown" {
		t.Errorf("phase should default to Unknown when status missing: %s", m.Phase)
	}
	if m.RestoreTime != "2026-05-09T00:31:50Z" {
		t.Errorf("restoreTime should still be extracted from spec: %s", m.RestoreTime)
	}
}

func TestHasPrevious(t *testing.T) {
	statuses := []corev1.ContainerStatus{
		{Name: "auth", RestartCount: 0},
		{Name: "wait-for-deps", RestartCount: 2},
	}
	if hasPrevious(statuses, "auth") {
		t.Error("auth has restartCount=0; should NOT have a previous container log")
	}
	if !hasPrevious(statuses, "wait-for-deps") {
		t.Error("wait-for-deps has restartCount=2; SHOULD have a previous container log")
	}
	if hasPrevious(statuses, "missing") {
		t.Error("missing container should report false (not found)")
	}
}
