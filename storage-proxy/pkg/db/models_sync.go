package db

import (
	"encoding/json"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
)

const (
	SyncSourceReplica = "replica"
	SyncSourceSandbox = "sandbox"

	SyncEventCreate     = "create"
	SyncEventWrite      = "write"
	SyncEventRemove     = "remove"
	SyncEventRename     = "rename"
	SyncEventChmod      = "chmod"
	SyncEventInvalidate = "invalidate"

	SyncConflictStatusOpen     = "open"
	SyncConflictStatusResolved = "resolved"
	SyncConflictStatusIgnored  = "ignored"
)

// SyncReplica tracks durable sync cursor state for one local-first replica.
type SyncReplica struct {
	ID            string                          `json:"id"`
	VolumeID      string                          `json:"volume_id"`
	TeamID        string                          `json:"team_id"`
	DisplayName   string                          `json:"display_name,omitempty"`
	Platform      string                          `json:"platform,omitempty"`
	RootPath      string                          `json:"root_path,omitempty"`
	CaseSensitive bool                            `json:"case_sensitive"`
	Capabilities  pathnorm.FilesystemCapabilities `json:"capabilities"`

	LastSeenAt     time.Time `json:"last_seen_at"`
	LastAppliedSeq int64     `json:"last_applied_seq"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// SyncJournalEntry is a durable change-log entry for one sandbox volume.
type SyncJournalEntry struct {
	Seq               int64            `json:"seq"`
	VolumeID          string           `json:"volume_id"`
	TeamID            string           `json:"team_id"`
	Source            string           `json:"source"`
	ReplicaID         *string          `json:"replica_id,omitempty"`
	EventType         string           `json:"event_type"`
	Path              string           `json:"path,omitempty"`
	NormalizedPath    string           `json:"normalized_path,omitempty"`
	OldPath           *string          `json:"old_path,omitempty"`
	NormalizedOldPath *string          `json:"normalized_old_path,omitempty"`
	Tombstone         bool             `json:"tombstone"`
	ContentSHA256     *string          `json:"content_sha256,omitempty"`
	SizeBytes         *int64           `json:"size_bytes,omitempty"`
	Metadata          *json.RawMessage `json:"metadata,omitempty"`
	CreatedAt         time.Time        `json:"created_at"`
}

// SyncConflict records a deterministic conflict artifact for clients to resolve.
type SyncConflict struct {
	ID              string           `json:"id"`
	VolumeID        string           `json:"volume_id"`
	TeamID          string           `json:"team_id"`
	ReplicaID       *string          `json:"replica_id,omitempty"`
	Path            string           `json:"path"`
	NormalizedPath  string           `json:"normalized_path"`
	ArtifactPath    string           `json:"artifact_path"`
	IncomingPath    *string          `json:"incoming_path,omitempty"`
	IncomingOldPath *string          `json:"incoming_old_path,omitempty"`
	ExistingSeq     *int64           `json:"existing_seq,omitempty"`
	Reason          string           `json:"reason"`
	Status          string           `json:"status"`
	Metadata        *json.RawMessage `json:"metadata,omitempty"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
}

// SyncRequest stores an idempotency key and the committed response for retry-safe replica mutations.
type SyncRequest struct {
	VolumeID           string           `json:"volume_id"`
	ReplicaID          string           `json:"replica_id"`
	RequestID          string           `json:"request_id"`
	RequestFingerprint string           `json:"request_fingerprint"`
	ResponsePayload    *json.RawMessage `json:"response_payload,omitempty"`
	CreatedAt          time.Time        `json:"created_at"`
}

// SyncRetentionState tracks the durable retained journal floor for one volume.
type SyncRetentionState struct {
	VolumeID            string    `json:"volume_id"`
	TeamID              string    `json:"team_id"`
	CompactedThroughSeq int64     `json:"compacted_through_seq"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// SyncNamespacePolicy tracks the effective filesystem compatibility policy for one volume.
type SyncNamespacePolicy struct {
	VolumeID     string                          `json:"volume_id"`
	TeamID       string                          `json:"team_id"`
	Capabilities pathnorm.FilesystemCapabilities `json:"capabilities"`
	UpdatedAt    time.Time                       `json:"updated_at"`
}

// SyncVolumeHead summarizes the current sync head for one active volume.
type SyncVolumeHead struct {
	VolumeID string `json:"volume_id"`
	TeamID   string `json:"team_id"`
	HeadSeq  int64  `json:"head_seq"`
}
