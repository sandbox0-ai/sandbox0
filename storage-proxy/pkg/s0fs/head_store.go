package s0fs

import (
	"context"
	"time"
)

// CommittedHead points to the current committed immutable manifest for one volume.
type CommittedHead struct {
	VolumeID       string
	ManifestSeq    uint64
	CheckpointSeq  uint64
	ManifestKey    string
	ManifestDigest string
	CommitID       string
	Generation     uint64
	UpdatedAt      time.Time
}

// HeadStore stores committed manifest pointers outside the object store hot path.
// CompareAndSwapCommittedHead inserts when expected is nil and no row exists.
// Otherwise it advances only when every authoritative identity field matches.
type HeadStore interface {
	LoadCommittedHead(ctx context.Context, volumeID string) (*CommittedHead, error)
	CompareAndSwapCommittedHead(ctx context.Context, volumeID string, expected, head *CommittedHead) error
}

// CommitCoordinator fences object publication against garbage collection.
// Implementations must serialize BeginCommit and AcquireGarbageCollection for
// the same volume and must validate the exact committed head identity.
type CommitCoordinator interface {
	BeginCommit(ctx context.Context, volumeID, commitID string, expected *CommittedHead, expiresAt time.Time) error
	RenewCommit(ctx context.Context, volumeID, commitID string, expiresAt time.Time) error
	AbortCommit(ctx context.Context, volumeID, commitID string) error
	AcquireGarbageCollection(ctx context.Context, volumeID, token string, expected *CommittedHead, expiresAt time.Time) error
	ValidateGarbageCollection(ctx context.Context, volumeID, token string, expected *CommittedHead) error
	ReleaseGarbageCollection(ctx context.Context, volumeID, token string) error
	StageGarbageCollection(ctx context.Context, volumeID, token string, expected *CommittedHead, candidates []string, deleteAfter time.Time) ([]string, error)
}

func cloneCommittedHead(head *CommittedHead) *CommittedHead {
	if head == nil {
		return nil
	}
	clone := *head
	return &clone
}

func sameCommittedHeadIdentity(left, right *CommittedHead) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.VolumeID == right.VolumeID &&
		left.ManifestSeq == right.ManifestSeq &&
		left.CheckpointSeq == right.CheckpointSeq &&
		left.ManifestKey == right.ManifestKey &&
		left.ManifestDigest == right.ManifestDigest &&
		left.CommitID == right.CommitID &&
		left.Generation == right.Generation
}
