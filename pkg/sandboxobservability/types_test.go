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
}

func TestFilterValidation(t *testing.T) {
	if !ValidSource(SourceNetd) || ValidSource(Source("files")) || ValidSource(Source("metering")) {
		t.Fatal("source validation mismatch")
	}
	if !ValidEventType(EventTypeNetworkAudit) || ValidEventType(EventType("usage_window")) || ValidEventType(EventType("file_audit")) {
		t.Fatal("event type validation mismatch")
	}
	if !ValidOutcome(OutcomeDenied) || !ValidOutcome(OutcomeUnknown) || ValidOutcome(Outcome("pending")) {
		t.Fatal("outcome validation mismatch")
	}
}

func TestNormalizeAuditDeliveryMode(t *testing.T) {
	for _, tt := range []struct {
		name string
		mode AuditDeliveryMode
		want AuditDeliveryMode
	}{
		{name: "empty defaults to durable async", want: AuditDeliveryModeDurableAsync},
		{name: "unknown fails closed", mode: AuditDeliveryMode("unknown"), want: AuditDeliveryModeCanonicalSync},
		{name: "durable async", mode: AuditDeliveryModeDurableAsync, want: AuditDeliveryModeDurableAsync},
		{name: "canonical sync", mode: AuditDeliveryModeCanonicalSync, want: AuditDeliveryModeCanonicalSync},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeAuditDeliveryMode(tt.mode); got != tt.want {
				t.Fatalf("NormalizeAuditDeliveryMode(%q) = %q, want %q", tt.mode, got, tt.want)
			}
		})
	}
}
