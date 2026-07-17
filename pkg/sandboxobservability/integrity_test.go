package sandboxobservability

import (
	"crypto/ed25519"
	"testing"
	"time"
)

func TestEventIntegrityIsCanonicalAndDetectsTampering(t *testing.T) {
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	event := Event{
		EventID:       "11111111-1111-4111-8111-111111111111",
		SchemaVersion: CurrentEventSchemaVersion,
		TeamID:        "team-1",
		SandboxID:     "sb-1",
		RegionID:      "region-1",
		ClusterID:     "cluster-1",
		OccurredAt:    time.Date(2026, 7, 13, 1, 2, 3, 4, time.FixedZone("offset", 8*60*60)),
		IngestedAt:    time.Date(2026, 7, 13, 1, 2, 4, 4, time.UTC),
		Source:        SourceNetd,
		EventType:     EventTypeNetworkAudit,
		Phase:         EventPhaseResult,
		Outcome:       OutcomeCompleted,
		Actor:         AuditActor{Kind: ActorKindSandboxWorkload, ID: "sb-1"},
		Action:        "network.connect",
		Resource:      AuditResource{Type: "sandbox_network", ID: "sb-1"},
		OperationID:   "operation-1",
		Producer:      AuditProducer{Service: "netd", Instance: "node-1", Sequence: 42},
		Attributes:    map[string]any{"z": "last", "a": "first"},
	}
	if err := SignEvent(&event, key); err != nil {
		t.Fatalf("SignEvent() error = %v", err)
	}
	if err := VerifyEventIntegrity(event, key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("VerifyEventIntegrity() error = %v", err)
	}

	reordered := event
	reordered.Attributes = map[string]any{"a": "first", "z": "last"}
	if err := VerifyEventIntegrity(reordered, key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatalf("map order changed integrity: %v", err)
	}

	tampered := event
	tampered.Action = "network.deny"
	if err := VerifyEventIntegrity(tampered, key.Public().(ed25519.PublicKey)); err == nil {
		t.Fatal("VerifyEventIntegrity() error = nil after protected field mutation")
	}
}

func TestVerifyEventIntegrityRejectsMalformedPublicKey(t *testing.T) {
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	event := Event{
		EventID:       "11111111-1111-4111-8111-111111111111",
		SchemaVersion: CurrentEventSchemaVersion,
		TeamID:        "team-1",
		SandboxID:     "sb-1",
		RegionID:      "region-1",
		ClusterID:     "cluster-1",
		OccurredAt:    time.Date(2026, 7, 13, 1, 2, 3, 4, time.UTC),
		Source:        SourceNetd,
		EventType:     EventTypeNetworkAudit,
		Phase:         EventPhaseAttempt,
		Outcome:       OutcomeAccepted,
		Actor:         AuditActor{Kind: ActorKindSandboxWorkload, ID: "sb-1"},
		Action:        "network.connect",
		Resource:      AuditResource{Type: "sandbox_network", ID: "sb-1"},
		OperationID:   "operation-1",
		Producer:      AuditProducer{Service: "netd"},
	}
	if err := SignEvent(&event, key); err != nil {
		t.Fatalf("SignEvent() error = %v", err)
	}
	if err := VerifyEventIntegrity(event, ed25519.PublicKey{1}); err == nil {
		t.Fatal("VerifyEventIntegrity() error = nil for malformed public key")
	}
}
