package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// silentLogger discards log calls — keeps test output clean.
type silentLogger struct{}

func (silentLogger) Info(string, ...any)  {}
func (silentLogger) Error(string, ...any) {}

func TestProbe200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := probe(context.Background(), srv.Client(), srv.URL); err != nil {
		t.Fatalf("probe should succeed on 200, got: %v", err)
	}
}

func TestProbeNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	if err := probe(context.Background(), srv.Client(), srv.URL); err == nil {
		t.Fatal("probe should fail on 503")
	}
}

func TestWaitOneReadyAfterFlap(t *testing.T) {
	// First N attempts return 503, then 200 — common cold-start shape.
	var count int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&count, 1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := waitOne(ctx, silentLogger{}, srv.Client(), srv.URL, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("waitOne should succeed once service starts returning 200, got: %v", err)
	}
	if atomic.LoadInt32(&count) < 3 {
		t.Errorf("expected ≥3 attempts, got %d", count)
	}
}

func TestWaitOneTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := waitOne(ctx, silentLogger{}, srv.Client(), srv.URL, 50*time.Millisecond)
	if err == nil {
		t.Fatal("waitOne should fail when ctx deadline elapses without 200")
	}
}

func TestWaitAllConcurrent(t *testing.T) {
	srvOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srvOK.Close()

	urls := []string{srvOK.URL, srvOK.URL}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := waitAll(ctx, silentLogger{}, urls, nil, 50*time.Millisecond, 5, nil); err != nil {
		t.Fatalf("waitAll should succeed when all services ready, got: %v", err)
	}
}

func TestWaitAllPartialFailure(t *testing.T) {
	srvOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srvOK.Close()

	srvBad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srvBad.Close()

	urls := []string{srvOK.URL, srvBad.URL}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := waitAll(ctx, silentLogger{}, urls, nil, 50*time.Millisecond, 5, nil)
	if err == nil {
		t.Fatal("waitAll should fail when one URL never returns 200")
	}
}

// — Pod-watch / unrecoverable-state tests —

func TestParsePodSpec_Valid(t *testing.T) {
	ns, sel, err := parsePodSpec("restore-abc/app=saas-pitr-auth")
	if err != nil {
		t.Fatalf("expected nil err, got: %v", err)
	}
	if ns != "restore-abc" || sel != "app=saas-pitr-auth" {
		t.Errorf("ns=%q sel=%q", ns, sel)
	}
}

func TestParsePodSpec_Invalid(t *testing.T) {
	for _, bad := range []string{"", "no-slash", "/no-namespace", "no-selector/"} {
		if _, _, err := parsePodSpec(bad); err == nil {
			t.Errorf("expected error for %q, got nil", bad)
		}
	}
}

func TestUnrecoverableReason_ImagePullBackOff(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "saas-pitr-kfm1-x"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "kfm1",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "ImagePullBackOff",
							Message: "Back-off pulling image akeyless/kfm:bogus",
						},
					},
				},
			},
		},
	}
	reason := unrecoverableReason(pod, 5)
	if !strings.Contains(reason, "ImagePullBackOff") {
		t.Errorf("expected ImagePullBackOff in reason, got: %q", reason)
	}
}

func TestUnrecoverableReason_CrashLoop(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "saas-pitr-auth-x"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:         "auth",
					RestartCount: 6, // exceeds threshold of 5
				},
			},
		},
	}
	reason := unrecoverableReason(pod, 5)
	if !strings.Contains(reason, "restartCount=6") {
		t.Errorf("expected restartCount=6 in reason, got: %q", reason)
	}
}

func TestUnrecoverableReason_BelowThreshold(t *testing.T) {
	// restartCount within threshold = NOT unrecoverable (transient flap)
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "auth", RestartCount: 3},
			},
		},
	}
	if reason := unrecoverableReason(pod, 5); reason != "" {
		t.Errorf("transient flap should NOT be unrecoverable, got: %q", reason)
	}
}

func TestUnrecoverableReason_InitContainerError(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "wait-for-deps",
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason:  "CreateContainerError",
							Message: "failed to mount volume X",
						},
					},
				},
			},
		},
	}
	reason := unrecoverableReason(pod, 5)
	if !strings.Contains(reason, "CreateContainerError") {
		t.Errorf("expected CreateContainerError in reason, got: %q", reason)
	}
}

func TestUnrecoverableReason_HealthyPod(t *testing.T) {
	// All containers running, no restarts → not unrecoverable.
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "auth", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, RestartCount: 0},
			},
		},
	}
	if reason := unrecoverableReason(pod, 5); reason != "" {
		t.Errorf("healthy pod should NOT be unrecoverable, got: %q", reason)
	}
}
