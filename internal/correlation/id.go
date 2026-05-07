// Package correlation builds and parses the deterministic correlation IDs that
// thread through every drill resource (Conventions Section 5; PITR as a
// Packaged Function page 4060807173).
//
// A correlation ID has the shape:
//
//	drill-<tenant>-<8-char-hex>
//
// where the 8-char hex is the first 8 characters of sha256(tenant || restoreTime
// || pitrsession_uid). The hash is deterministic — re-reconciliation of the
// same PITRSession produces the same correlation ID, which gives Crossplane
// idempotent resource emission across reconciliation passes.
//
// The correlation ID is written into:
//   - PITRSession.status.correlationId
//   - K8s resource names (RDS instance, namespace, Jobs, Helm release)
//   - K8s resource labels (pitr-correlation-id)
//   - AWS resource tags (pitr-correlation-id)
//   - Slack message text + S3 paths
//
// Operators grep one value across kubectl, AWS console, S3, Loki, Slack to
// trace a drill end-to-end.
package correlation

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Hash returns the first 8 hex characters of sha256(tenant || restoreTime || uid).
// Identical inputs always produce the identical hash — this is the basis of
// Crossplane's idempotent reconciliation.
func Hash(tenant, restoreTime, uid string) string {
	sum := sha256.Sum256([]byte(tenant + restoreTime + uid))
	return hex.EncodeToString(sum[:])[:8]
}

// ID returns the full correlation ID for a drill: drill-<tenant>-<8-char-hex>.
func ID(tenant, restoreTime, uid string) string {
	return fmt.Sprintf("drill-%s-%s", tenant, Hash(tenant, restoreTime, uid))
}

// RestoreNamespace returns the K8s namespace for a drill's restored env:
// restore-<8-char-hex>. Stays under the K8s 63-char namespace limit.
func RestoreNamespace(hash string) string {
	return fmt.Sprintf("restore-%s", hash)
}

// CanaryPath returns the akeyless secret path for a drill's auto-generated
// canary: /drill-canary/<8-char-hex>.
func CanaryPath(hash string) string {
	return fmt.Sprintf("/drill-canary/%s", hash)
}

// HashFromID extracts the 8-char hex from a full correlation ID. Returns the
// empty string if the ID does not match the expected shape.
func HashFromID(id string) string {
	const prefix = "drill-"
	if !strings.HasPrefix(id, prefix) {
		return ""
	}
	rest := id[len(prefix):]
	// Last "-" separates tenant from hash.
	idx := strings.LastIndex(rest, "-")
	if idx < 0 || idx+1 >= len(rest) {
		return ""
	}
	hash := rest[idx+1:]
	if len(hash) != 8 {
		return ""
	}
	return hash
}
