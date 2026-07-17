package sandboxobservability

import (
	"crypto/ed25519"
	"strings"
	"testing"
	"time"
)

func TestValidateSignedEventRejectsInvalidDomainBeforeCustody(t *testing.T) {
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	event := validAuditValidationEvent()
	if err := SignEvent(&event, key); err != nil {
		t.Fatalf("SignEvent() error = %v", err)
	}
	if err := ValidateSignedEvent(event); err != nil {
		t.Fatalf("ValidateSignedEvent() error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Event)
		want   string
	}{
		{name: "invalid event id", mutate: func(event *Event) { event.EventID = "not-a-uuid" }, want: "event_id must be a UUID"},
		{name: "missing operation id", mutate: func(event *Event) { event.OperationID = "" }, want: "operation_id is required"},
		{name: "missing outcome", mutate: func(event *Event) { event.Outcome = "" }, want: "invalid outcome"},
		{name: "invalid parent id", mutate: func(event *Event) { event.ParentEventID = "not-a-uuid" }, want: "parent_event_id must be a UUID"},
		{name: "occurred at outside storage range", mutate: func(event *Event) {
			event.OccurredAt = time.Date(1500, time.January, 1, 0, 0, 0, 0, time.UTC)
		}, want: "occurred_at is outside the DateTime64(9) range"},
		{name: "status code below storage range", mutate: func(event *Event) { event.Request.StatusCode = -1 }, want: "status_code must be between"},
		{name: "status code above storage range", mutate: func(event *Event) { event.Request.StatusCode = 65536 }, want: "status_code must be between"},
		{name: "negative producer sequence", mutate: func(event *Event) { event.Producer.Sequence = -1 }, want: "producer sequence must not be negative"},
		{name: "partial execution scope", mutate: func(event *Event) {
			event.ExecutionScope = &ExecutionScope{Namespace: "codex"}
		}, want: "execution_scope: kind is required"},
		{name: "execution scope whitespace", mutate: func(event *Event) {
			event.ExecutionScope = &ExecutionScope{
				Namespace:   " codex",
				Kind:        "native_session",
				ID:          "thread-1",
				Attribution: ExecutionScopeAttributionProcessEnvironment,
			}
		}, want: "execution_scope: namespace must not contain surrounding whitespace"},
		{name: "invalid execution scope attribution", mutate: func(event *Event) {
			event.ExecutionScope = &ExecutionScope{
				Namespace:   "codex",
				Kind:        "native_session",
				ID:          "thread-1",
				Attribution: "unknown",
			}
		}, want: "execution_scope: invalid attribution"},
		{name: "oversized execution scope namespace", mutate: func(event *Event) {
			event.ExecutionScope = &ExecutionScope{
				Namespace:   strings.Repeat("n", MaxExecutionScopeNamespaceBytes+1),
				Kind:        "native_session",
				ID:          "thread-1",
				Attribution: ExecutionScopeAttributionProcessEnvironment,
			}
		}, want: "execution_scope: namespace exceeds"},
		{name: "oversized execution scope kind", mutate: func(event *Event) {
			event.ExecutionScope = &ExecutionScope{
				Namespace:   "codex",
				Kind:        strings.Repeat("k", MaxExecutionScopeKindBytes+1),
				ID:          "thread-1",
				Attribution: ExecutionScopeAttributionProcessEnvironment,
			}
		}, want: "execution_scope: kind exceeds"},
		{name: "oversized execution scope id", mutate: func(event *Event) {
			event.ExecutionScope = &ExecutionScope{
				Namespace:   "codex",
				Kind:        "native_session",
				ID:          strings.Repeat("i", MaxExecutionScopeIDBytes+1),
				Attribution: ExecutionScopeAttributionProcessEnvironment,
			}
		}, want: "execution_scope: id exceeds"},
		{name: "v2 execution scope", mutate: func(event *Event) {
			event.SchemaVersion = LegacyEventSchemaVersion
			event.ExecutionScope = &ExecutionScope{
				Namespace:   "codex",
				Kind:        "native_session",
				ID:          "thread-1",
				Attribution: ExecutionScopeAttributionProcessEnvironment,
			}
		}, want: "does not support execution_scope"},
		{name: "v3 without execution scope", mutate: func(event *Event) {
			event.SchemaVersion = CurrentEventSchemaVersion
			event.ExecutionScope = nil
		}, want: "requires execution_scope"},
		{name: "oversized attributes", mutate: func(event *Event) {
			event.Attributes = map[string]any{"value": strings.Repeat("x", MaxAuditAttributesBytes)}
		}, want: "attributes exceed"},
		{name: "malformed signature", mutate: func(event *Event) { event.Integrity.Signature = "bad" }, want: "valid Ed25519 signature"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := event
			tt.mutate(&candidate)
			if err := ValidateSignedEvent(candidate); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateSignedEvent() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestValidExecutionScope(t *testing.T) {
	scope := ExecutionScope{
		Namespace:   "codex",
		Kind:        "native_session",
		ID:          "thread-1",
		Attribution: ExecutionScopeAttributionProcessEnvironment,
	}
	if scope.IsZero() {
		t.Fatal("ExecutionScope.IsZero() = true for populated scope")
	}
	if !ValidExecutionScope(scope) {
		t.Fatalf("ValidExecutionScope(%+v) = false", scope)
	}
	if ValidExecutionScope(ExecutionScope{}) {
		t.Fatal("ValidExecutionScope() = true for zero scope")
	}
}

func TestSignEventDoesNotSetQueryVerificationState(t *testing.T) {
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	event := validAuditValidationEvent()
	if err := SignEvent(&event, key); err != nil {
		t.Fatalf("SignEvent() error = %v", err)
	}
	if event.Integrity.SignatureStatus != "" || event.Integrity.EventIDConflict {
		t.Fatalf("signing populated query state: %+v", event.Integrity)
	}
}

func TestValidDateTime64NanoUsesClickHousePrecisionNineRange(t *testing.T) {
	max := time.Unix(0, int64(^uint64(0)>>1)).UTC()
	tests := []struct {
		name  string
		value time.Time
		want  bool
	}{
		{name: "before minimum", value: time.Date(1899, time.December, 31, 23, 59, 59, 999999999, time.UTC)},
		{name: "minimum", value: time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC), want: true},
		{name: "maximum", value: max, want: true},
		{name: "after maximum", value: max.Add(time.Nanosecond)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidDateTime64Nano(tt.value); got != tt.want {
				t.Fatalf("ValidDateTime64Nano(%s) = %t, want %t", tt.value, got, tt.want)
			}
		})
	}
}

func validAuditValidationEvent() Event {
	return Event{
		EventID:       "11111111-1111-4111-8111-111111111111",
		SchemaVersion: CurrentEventSchemaVersion,
		TeamID:        "team-1",
		SandboxID:     "sandbox-1",
		RegionID:      "region-1",
		ClusterID:     "cluster-1",
		OccurredAt:    time.Date(2026, 7, 13, 1, 2, 3, 0, time.UTC),
		Source:        SourceClusterGateway,
		EventType:     EventTypeAPIAccess,
		Phase:         EventPhaseAttempt,
		Outcome:       OutcomeAccepted,
		Actor:         AuditActor{Kind: ActorKindHuman, ID: "user-1"},
		ExecutionScope: &ExecutionScope{
			Namespace:   "codex",
			Kind:        "native_session",
			ID:          "thread-1",
			Attribution: ExecutionScopeAttributionProcessEnvironment,
		},
		Action:      "sandbox.read",
		Resource:    AuditResource{Type: "sandbox", ID: "sandbox-1"},
		OperationID: "operation-1",
		Producer:    AuditProducer{Service: "cluster-gateway"},
	}
}
