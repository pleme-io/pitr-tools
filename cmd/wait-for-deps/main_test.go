package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
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
	// Always 503 — should hit ctx deadline.
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

	// Two URLs, both ready. Should succeed quickly.
	urls := []string{srvOK.URL, srvOK.URL}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := waitAll(ctx, silentLogger{}, urls, 50*time.Millisecond); err != nil {
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

	err := waitAll(ctx, silentLogger{}, urls, 50*time.Millisecond)
	if err == nil {
		t.Fatal("waitAll should fail when one URL never returns 200")
	}
}
