package result

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	corev1typed "k8s.io/client-go/kubernetes/typed/core/v1"
)

// runWithFakeAPI overrides the in-cluster client construction with a
// httptest server-backed fake so Create/Update flows are exercised
// against a real api shape, including the IsAlreadyExists branch.
//
// We don't depend on InClusterConfig (which would only work inside a
// Pod). Instead we test the Outcome marshaling + key composition
// directly against a fake clientset, then verify the produced
// ConfigMap has the right shape.
func TestOutcomeMarshalsExpectedKeys(t *testing.T) {
	out := Outcome{
		CorrelationID:    "drill-test-abcd1234",
		RetrievedSecrets: []string{"/canary/x", "/canary/y"},
		MissingSecrets:   []string{},
		Phase:            PhaseSucceeded,
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	for _, want := range []string{
		`"correlation_id":"drill-test-abcd1234"`,
		`"retrieved_secrets":["/canary/x","/canary/y"]`,
		`"missing_secrets":[]`,
		`"phase":"Succeeded"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("outcome JSON missing %q in %q", want, s)
		}
	}
}

func TestWriteConfigMapEmptyNamespaceErrors(t *testing.T) {
	_, err := WriteConfigMap(context.Background(), "", Outcome{
		CorrelationID:    "drill-x-12345678",
		RetrievedSecrets: []string{},
	})
	if err == nil || !strings.Contains(err.Error(), "pod namespace is empty") {
		t.Errorf("expected empty-namespace error, got %v", err)
	}
}

func TestWriteConfigMapEmptyCorrelationErrors(t *testing.T) {
	_, err := WriteConfigMap(context.Background(), "pitr-akeyless", Outcome{
		CorrelationID:    "",
		RetrievedSecrets: []string{},
	})
	if err == nil || !strings.Contains(err.Error(), "empty correlation id") {
		t.Errorf("expected empty-correlation error, got %v", err)
	}
}

// TestWriteConfigMapCreateThenUpdate exercises the create-or-update
// branch via a fake clientset (bypasses the in-cluster-config path
// because we can't reach a real kube-apiserver from unit tests).
func TestWriteConfigMapCreateThenUpdate(t *testing.T) {
	const (
		ns            = "pitr-akeyless"
		correlationID = "drill-test-deadbeef"
		expectedCM    = "drill-result-drill-test-deadbeef"
	)
	cs := fake.NewSimpleClientset()

	out := Outcome{
		CorrelationID:    correlationID,
		RetrievedSecrets: []string{"/canary/seed"},
		MissingSecrets:   []string{},
		Phase:            PhaseSucceeded,
	}

	if err := writeWithClient(context.Background(), cs.CoreV1().ConfigMaps(ns), ns, out); err != nil {
		t.Fatalf("first write: %v", err)
	}
	cm, err := cs.CoreV1().ConfigMaps(ns).Get(context.Background(), expectedCM, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after create: %v", err)
	}
	if cm.Data["phase"] != string(PhaseSucceeded) {
		t.Errorf("phase: got %q want Succeeded", cm.Data["phase"])
	}
	if cm.Data["totalRetrieved"] != "1" {
		t.Errorf("totalRetrieved: got %q want 1", cm.Data["totalRetrieved"])
	}

	out.RetrievedSecrets = append(out.RetrievedSecrets, "/canary/seed2")
	out.MissingSecrets = []string{"/canary/missing"}
	out.Phase = PhaseFailed
	if err := writeWithClient(context.Background(), cs.CoreV1().ConfigMaps(ns), ns, out); err != nil {
		t.Fatalf("second write (update branch): %v", err)
	}
	cm, err = cs.CoreV1().ConfigMaps(ns).Get(context.Background(), expectedCM, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if cm.Data["phase"] != string(PhaseFailed) {
		t.Errorf("phase after update: got %q want Failed", cm.Data["phase"])
	}
	if cm.Data["totalRetrieved"] != "2" {
		t.Errorf("totalRetrieved after update: got %q want 2", cm.Data["totalRetrieved"])
	}
	if cm.Data["totalMissing"] != "1" {
		t.Errorf("totalMissing after update: got %q want 1", cm.Data["totalMissing"])
	}
}

// silence unused-import warnings on builds that strip test files
var (
	_ = httptest.NewServer
	_ = http.StatusOK
	_ = corev1.ConfigMap{}
	_ = errors.New
	_ corev1typed.ConfigMapInterface
)
