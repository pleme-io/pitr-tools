package correlation

import (
	"strings"
	"testing"
)

func TestHashDeterministic(t *testing.T) {
	a := Hash("mte-staging", "2026-05-04T14:30:00Z", "abc-123")
	b := Hash("mte-staging", "2026-05-04T14:30:00Z", "abc-123")
	if a != b {
		t.Fatalf("Hash should be deterministic: a=%s b=%s", a, b)
	}
	if len(a) != 8 {
		t.Errorf("Hash should be 8 chars; got %d (%q)", len(a), a)
	}
	// Hex characters only.
	for _, c := range a {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("Hash contains non-hex char %q in %q", c, a)
		}
	}
}

func TestHashSensitiveToInputs(t *testing.T) {
	base := Hash("mte-staging", "2026-05-04T14:30:00Z", "uid-1")
	cases := []struct {
		name            string
		tenant, ts, uid string
	}{
		{"different tenant", "meu-production", "2026-05-04T14:30:00Z", "uid-1"},
		{"different timestamp", "mte-staging", "2026-05-04T14:30:01Z", "uid-1"},
		{"different uid", "mte-staging", "2026-05-04T14:30:00Z", "uid-2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := Hash(tc.tenant, tc.ts, tc.uid)
			if h == base {
				t.Errorf("Hash should differ when %s changes; got same value %q", tc.name, h)
			}
		})
	}
}

func TestID(t *testing.T) {
	id := ID("mte-staging", "2026-05-04T14:30:00Z", "uid-1")
	if !strings.HasPrefix(id, "drill-mte-staging-") {
		t.Errorf("ID prefix wrong: %q", id)
	}
	if len(id) != len("drill-mte-staging-")+8 {
		t.Errorf("ID length wrong: %q (len=%d)", id, len(id))
	}
}

func TestRestoreNamespace(t *testing.T) {
	ns := RestoreNamespace("3a7f9c2b")
	if ns != "restore-3a7f9c2b" {
		t.Errorf("RestoreNamespace: %q", ns)
	}
	// K8s namespace limit is 63 chars; verify under.
	if len(ns) > 63 {
		t.Errorf("RestoreNamespace exceeds K8s limit: %d chars", len(ns))
	}
}

func TestCanaryPath(t *testing.T) {
	p := CanaryPath("3a7f9c2b")
	if p != "/drill-canary/3a7f9c2b" {
		t.Errorf("CanaryPath: %q", p)
	}
}

func TestHashFromID(t *testing.T) {
	tests := []struct {
		id   string
		want string
	}{
		{"drill-mte-staging-3a7f9c2b", "3a7f9c2b"},
		{"drill-meu-production-7d8e1a02", "7d8e1a02"},
		{"drill-mte-staging-c4f50bd9", "c4f50bd9"},
		{"not-a-drill-id", ""},
		{"drill-onlytenant", ""},
		{"drill-mte-staging-shorthash", ""},
		{"", ""},
	}
	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			if got := HashFromID(tc.id); got != tc.want {
				t.Errorf("HashFromID(%q) = %q, want %q", tc.id, got, tc.want)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	tenant, ts, uid := "mte-staging", "2026-05-04T14:30:00Z", "abc-123"
	hash := Hash(tenant, ts, uid)
	id := ID(tenant, ts, uid)
	if HashFromID(id) != hash {
		t.Errorf("round-trip mismatch: hash=%q id=%q HashFromID=%q", hash, id, HashFromID(id))
	}
}
