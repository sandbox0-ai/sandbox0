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
	return validateEvent(event)
}

func validateEvent(event Event) error {
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
	switch event.SchemaVersion {
	case LegacyEventSchemaVersion:
		if event.ExecutionScope != nil {
			return fmt.Errorf("schema_version %d does not support execution_scope", LegacyEventSchemaVersion)
		}
	case CurrentEventSchemaVersion:
		if event.ExecutionScope == nil {
			return fmt.Errorf("schema_version %d requires execution_scope", CurrentEventSchemaVersion)
		}
	default:
		return fmt.Errorf("unsupported schema_version %d", event.SchemaVersion)
	}
	if event.ExecutionScope != nil {
		if err := ValidateExecutionScope(*event.ExecutionScope); err != nil {
			return fmt.Errorf("execution_scope: %w", err)
		}
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

// ValidEventMaxSchemaVersion reports whether a requested response schema can
// be negotiated. Versions newer than this server are accepted and capped to
// the current schema.
func ValidEventMaxSchemaVersion(version int) bool {
	return version >= LegacyEventSchemaVersion
}

// NormalizeEventMaxSchemaVersion returns the effective schema supported by
// this server. Zero represents an omitted query parameter.
func NormalizeEventMaxSchemaVersion(version int) (int, bool) {
	if version == 0 {
		return DefaultEventMaxSchemaVersion, true
	}
	if !ValidEventMaxSchemaVersion(version) {
		return 0, false
	}
	if version > CurrentEventSchemaVersion {
		return CurrentEventSchemaVersion, true
	}
	return version, true
}

// ValidateExecutionScope verifies that a present execution scope is complete
// and canonical before it enters the signed audit payload.
func ValidateExecutionScope(scope ExecutionScope) error {
	if err := ValidateExecutionScopeFilter(scope.Namespace, scope.Kind, scope.ID, scope.Attribution); err != nil {
		return err
	}
	fields := []struct {
		name  string
		value string
	}{
		{name: "namespace", value: scope.Namespace},
		{name: "kind", value: scope.Kind},
		{name: "id", value: scope.ID},
		{name: "attribution", value: string(scope.Attribution)},
	}
	for _, field := range fields {
		if field.value == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}
	return nil
}

// ValidateExecutionScopeFilter validates optional exact-match query fields.
func ValidateExecutionScopeFilter(namespace, kind, id string, attribution ExecutionScopeAttribution) error {
	fields := []struct {
		name     string
		value    string
		maxBytes int
	}{
		{name: "namespace", value: namespace, maxBytes: MaxExecutionScopeNamespaceBytes},
		{name: "kind", value: kind, maxBytes: MaxExecutionScopeKindBytes},
		{name: "id", value: id, maxBytes: MaxExecutionScopeIDBytes},
	}
	for _, field := range fields {
		if field.value != strings.TrimSpace(field.value) {
			return fmt.Errorf("%s must not contain surrounding whitespace", field.name)
		}
		if len(field.value) > field.maxBytes {
			return fmt.Errorf("%s exceeds %d bytes", field.name, field.maxBytes)
		}
	}
	attributionValue := string(attribution)
	if attributionValue != strings.TrimSpace(attributionValue) {
		return fmt.Errorf("attribution must not contain surrounding whitespace")
	}
	if attribution != "" && !ValidExecutionScopeAttribution(attribution) {
		return fmt.Errorf("invalid attribution")
	}
	return nil
}

// ValidExecutionScope reports whether a present execution scope is complete
// and canonical.
func ValidExecutionScope(scope ExecutionScope) bool {
	return ValidateExecutionScope(scope) == nil
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
	if err := validateEvent(event); err != nil {
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
