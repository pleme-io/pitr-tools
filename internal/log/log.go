// Package log provides a structured logger with PITR-specific fields
// (correlation_id, tenant) threaded through. Used by all pitr-akeyless Go
// binaries (Decision 19; PITR Project Hub Confluence page 4042457094).
//
// All log lines emitted by every binary running for a single drill carry the
// same correlation_id, which the operator greps across kubectl, AWS, Slack,
// S3, Loki to trace a drill end-to-end (Conventions Section 5).
package log

import (
	"log/slog"
	"os"
)

// New returns a slog.Logger emitting JSON to stderr with the supplied
// correlation_id baked in as a default attribute. Caller can chain further
// .With() calls for binary-specific context (e.g. .With("phase", "Succeeded")).
func New(correlationID string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(h).With("correlation_id", correlationID)
}
