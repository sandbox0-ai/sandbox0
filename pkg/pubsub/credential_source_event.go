package pubsub

// CredentialSourceRotationChannel is the PostgreSQL NOTIFY channel for
// credential source version rotation events.
const CredentialSourceRotationChannel = "credential_source_rotation_events"

// CredentialSourceRotatedEvent identifies a new active credential source
// version. It carries only invalidation metadata, never secret material.
type CredentialSourceRotatedEvent struct {
	EventBase
	TeamID        string `json:"team_id"`
	SourceID      int64  `json:"source_id"`
	SourceRef     string `json:"source_ref"`
	SourceVersion int64  `json:"source_version"`
}
