package sandboxobservability

import (
	"context"
	"errors"
	"testing"
)

func TestDisabledRepositoryReturnsBackendDisabled(t *testing.T) {
	repo := NewDisabledRepository()

	if _, err := repo.ListEvents(context.Background(), EventQuery{}); !errors.Is(err, ErrBackendDisabled) {
		t.Fatalf("ListEvents error = %v, want ErrBackendDisabled", err)
	}
	if _, err := repo.ListAuditEvents(context.Background(), EventQuery{}); !errors.Is(err, ErrBackendDisabled) {
		t.Fatalf("ListAuditEvents error = %v, want ErrBackendDisabled", err)
	}
}

func TestFilterValidation(t *testing.T) {
	if !ValidSource(SourceNetd) || ValidSource(Source("files")) || ValidSource(Source("metering")) {
		t.Fatal("source validation mismatch")
	}
	if !ValidEventType(EventTypeNetworkAudit) || ValidEventType(EventType("usage_window")) || ValidEventType(EventType("file_audit")) {
		t.Fatal("event type validation mismatch")
	}
	if !ValidOutcome(OutcomeDenied) || ValidOutcome(Outcome("unknown")) {
		t.Fatal("outcome validation mismatch")
	}
}
