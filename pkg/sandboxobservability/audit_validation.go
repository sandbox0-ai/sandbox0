package sandboxobservability

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

const MaxAuditAttributesBytes = 64 * 1024

var (
	dateTime64NanoMin = time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
	dateTime64NanoMax = time.Unix(0, math.MaxInt64).UTC()
)

// ValidateEventForSigning verifies the canonical fields that every audit
// producer must set before the gateway signs and accepts the event.
func ValidateEventForSigning(event Event) error {
	canonicalFields := []struct {
		name     string
		value    string
		required bool
	}{
		{name: "event_id", value: event.EventID, required: true},
		{name: "team_id", value: event.TeamID, required: true},
		{name: "sandbox_id", value: event.SandboxID, required: true},
		{name: "region_id", value: event.RegionID, required: true},
		{name: "cluster_id", value: event.ClusterID, required: true},
		{name: "actor.id", value: event.Actor.ID},
		{name: "action", value: event.Action, required: true},
		{name: "resource.type", value: event.Resource.Type, required: true},
		{name: "resource.id", value: event.Resource.ID, required: true},
		{name: "operation_id", value: event.OperationID, required: true},
		{name: "parent_event_id", value: event.ParentEventID},
		{name: "producer.service", value: event.Producer.Service, required: true},
		{name: "producer.instance", value: event.Producer.Instance},
	}
	for _, field := range canonicalFields {
		if field.value != strings.TrimSpace(field.value) {
			return fmt.Errorf("%s must not contain surrounding whitespace", field.name)
		}
		if field.required && field.value == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}

	parsedEventID, err := uuid.Parse(event.EventID)
	if err != nil || parsedEventID.String() != event.EventID {
		return fmt.Errorf("event_id must be a UUID")
	}
	if event.ParentEventID != "" {
		parsedParentID, err := uuid.Parse(event.ParentEventID)
		if err != nil || parsedParentID.String() != event.ParentEventID {
			return fmt.Errorf("parent_event_id must be a UUID")
		}
	}
	if event.SchemaVersion != CurrentEventSchemaVersion {
		return fmt.Errorf("schema_version must be %d", CurrentEventSchemaVersion)
	}
	if event.OccurredAt.IsZero() {
		return fmt.Errorf("occurred_at is required")
	}
	if !ValidDateTime64Nano(event.OccurredAt) {
		return fmt.Errorf("occurred_at is outside the DateTime64(9) range")
	}
	if !event.IngestedAt.IsZero() && !ValidDateTime64Nano(event.IngestedAt) {
		return fmt.Errorf("ingested_at is outside the DateTime64(9) range")
	}
	if !ValidSource(event.Source) {
		return fmt.Errorf("invalid source")
	}
	if !ValidEventType(event.EventType) {
		return fmt.Errorf("invalid event_type")
	}
	if !ValidEventPhase(event.Phase) {
		return fmt.Errorf("invalid phase")
	}
	if !ValidOutcome(event.Outcome) {
		return fmt.Errorf("invalid outcome")
	}
	if !ValidActorKind(event.Actor.Kind) {
		return fmt.Errorf("invalid actor kind")
	}
	if event.Producer.Sequence < 0 {
		return fmt.Errorf("producer sequence must not be negative")
	}
	if event.Request.StatusCode < 0 || event.Request.StatusCode > 65535 {
		return fmt.Errorf("request status_code must be between 0 and 65535")
	}

	attributes, err := json.Marshal(event.Attributes)
	if err != nil {
		return fmt.Errorf("encode attributes: %w", err)
	}
	if len(attributes) > MaxAuditAttributesBytes {
		return fmt.Errorf("attributes exceed %d bytes", MaxAuditAttributesBytes)
	}
	return nil
}

// ValidDateTime64Nano reports whether value can be represented by the
// ClickHouse DateTime64(9) columns used by the canonical audit ledger.
func ValidDateTime64Nano(value time.Time) bool {
	value = value.UTC()
	return !value.Before(dateTime64NanoMin) && !value.After(dateTime64NanoMax)
}

// ValidateSignedEvent verifies the domain fields and the structural integrity
// envelope. Cryptographic verification remains the responsibility of
// VerifyEventIntegrity because readers may resolve keys independently.
func ValidateSignedEvent(event Event) error {
	if err := ValidateEventForSigning(event); err != nil {
		return err
	}
	if event.Integrity.Algorithm != integrityAlgorithm {
		return fmt.Errorf("unsupported integrity algorithm %q", event.Integrity.Algorithm)
	}
	if !validHexDigest(event.Integrity.PayloadHash) {
		return fmt.Errorf("payload_hash must be a 64-character hexadecimal digest")
	}
	if !validHexDigest(event.Integrity.SigningKeyID) {
		return fmt.Errorf("signing_key_id must be a 64-character hexadecimal digest")
	}
	signature, err := base64.RawURLEncoding.DecodeString(event.Integrity.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("signature must be a valid Ed25519 signature")
	}
	return nil
}

func validHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}
