package sandboxobservability

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const integrityAlgorithm = "ed25519-sha256-v1"

type integrityPayload struct {
	EventID       string         `json:"event_id"`
	SchemaVersion int            `json:"schema_version"`
	TeamID        string         `json:"team_id"`
	SandboxID     string         `json:"sandbox_id"`
	RegionID      string         `json:"region_id"`
	ClusterID     string         `json:"cluster_id"`
	OccurredAt    string         `json:"occurred_at"`
	Source        Source         `json:"source"`
	EventType     EventType      `json:"event_type"`
	Phase         EventPhase     `json:"phase"`
	Outcome       Outcome        `json:"outcome,omitempty"`
	Actor         AuditActor     `json:"actor"`
	Action        string         `json:"action"`
	Resource      AuditResource  `json:"resource"`
	OperationID   string         `json:"operation_id,omitempty"`
	ParentEventID string         `json:"parent_event_id,omitempty"`
	Producer      AuditProducer  `json:"producer"`
	Request       AuditRequest   `json:"request,omitempty"`
	Attributes    map[string]any `json:"attributes,omitempty"`
}

// CanonicalEventPayload returns the stable bytes protected by an event
// signature. encoding/json sorts map keys, and the producer timestamp is
// normalized to RFC3339Nano UTC so retries produce identical payloads. The
// gateway receipt timestamp is storage metadata and intentionally excluded so
// replaying one producer event keeps the same canonical hash.
func CanonicalEventPayload(event Event) ([]byte, error) {
	payload := integrityPayload{
		EventID:       event.EventID,
		SchemaVersion: event.SchemaVersion,
		TeamID:        event.TeamID,
		SandboxID:     event.SandboxID,
		RegionID:      event.RegionID,
		ClusterID:     event.ClusterID,
		OccurredAt:    event.OccurredAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		Source:        event.Source,
		EventType:     event.EventType,
		Phase:         event.Phase,
		Outcome:       event.Outcome,
		Actor:         event.Actor,
		Action:        event.Action,
		Resource:      event.Resource,
		OperationID:   event.OperationID,
		ParentEventID: event.ParentEventID,
		Producer:      event.Producer,
		Request:       event.Request,
		Attributes:    event.Attributes,
	}
	return json.Marshal(payload)
}

// SignEvent computes and attaches the canonical digest and Ed25519 signature.
func SignEvent(event *Event, privateKey ed25519.PrivateKey) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}
	if len(privateKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("invalid audit signing key")
	}
	payload, err := CanonicalEventPayload(*event)
	if err != nil {
		return fmt.Errorf("canonicalize audit event: %w", err)
	}
	digest := sha256.Sum256(payload)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	keyDigest := sha256.Sum256(publicKey)
	event.Integrity = AuditIntegrity{
		Algorithm:    integrityAlgorithm,
		PayloadHash:  hex.EncodeToString(digest[:]),
		Signature:    base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, digest[:])),
		SigningKeyID: hex.EncodeToString(keyDigest[:]),
		Status:       "verified",
	}
	return nil
}

// VerifyEventIntegrity verifies both the canonical payload digest and its
// signature against the supplied audit public key.
func VerifyEventIntegrity(event Event, publicKey ed25519.PublicKey) error {
	if event.Integrity.Algorithm != integrityAlgorithm {
		return fmt.Errorf("unsupported integrity algorithm %q", event.Integrity.Algorithm)
	}
	payload, err := CanonicalEventPayload(event)
	if err != nil {
		return fmt.Errorf("canonicalize audit event: %w", err)
	}
	digest := sha256.Sum256(payload)
	if event.Integrity.PayloadHash != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("audit payload hash mismatch")
	}
	signature, err := base64.RawURLEncoding.DecodeString(event.Integrity.Signature)
	if err != nil {
		return fmt.Errorf("decode audit signature: %w", err)
	}
	if !ed25519.Verify(publicKey, digest[:], signature) {
		return fmt.Errorf("invalid audit signature")
	}
	keyDigest := sha256.Sum256(publicKey)
	if event.Integrity.SigningKeyID != hex.EncodeToString(keyDigest[:]) {
		return fmt.Errorf("audit signing key id mismatch")
	}
	return nil
}
