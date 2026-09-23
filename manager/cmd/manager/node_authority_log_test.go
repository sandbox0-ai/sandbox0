package main

import (
	"errors"
	"strings"
	"testing"
)

func TestTerminalErrorSummaryBoundsAndRedactsJoinedFailures(t *testing.T) {
	first := strings.Repeat("x", 600) + " token=private-value"
	joined := errors.Join(errors.New(first), errors.New("second-slot-secret"))
	summary := terminalErrorSummary(joined)
	if len([]rune(summary)) != 512 || strings.Contains(summary, "private-value") ||
		strings.Contains(summary, "second-slot-secret") {
		t.Fatalf("unsafe terminal error summary: %q", summary)
	}
	if got := terminalErrorSummary(errors.New("Bearer private-value")); got != "[REDACTED]" {
		t.Fatalf("bearer token was not redacted: %q", got)
	}
}
