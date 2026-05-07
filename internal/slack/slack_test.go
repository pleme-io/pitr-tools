package slack

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Decision 15: any Slack failure must surface as a Go error so the calling
// binary exits non-zero, cascading to Job failure → Object NotReady →
// composite Ready=False → drill phase=Failed.

func TestPost_2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "drill") {
			t.Errorf("payload missing 'drill': %q", body)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := New(srv.URL).Post(Message{Text: "drill complete"}); err != nil {
		t.Fatalf("Post: %v", err)
	}
}

func TestPost_5xxCascades(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"channel deleted"}`))
	}))
	defer srv.Close()

	err := New(srv.URL).Post(Message{Text: "x"})
	if err == nil {
		t.Fatal("Post: expected error on 5xx, got nil")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error should reference status 503; got: %v", err)
	}
	if !strings.Contains(err.Error(), "channel deleted") {
		t.Errorf("error should include response body; got: %v", err)
	}
}

func TestPost_4xxCascades(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	if err := New(srv.URL).Post(Message{Text: "x"}); err == nil {
		t.Fatal("Post: expected error on 4xx, got nil")
	}
}

func TestPost_EmptyURL(t *testing.T) {
	if err := New("").Post(Message{Text: "x"}); err == nil {
		t.Fatal("Post: expected error on empty webhook URL, got nil")
	}
}

func TestPost_NetworkUnreachable(t *testing.T) {
	// 127.0.0.1:1 is reserved + closed — connection refused immediately.
	if err := New("http://127.0.0.1:1/").Post(Message{Text: "x"}); err == nil {
		t.Fatal("Post: expected error on connection refused, got nil")
	}
}
