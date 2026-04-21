package s0fs

import (
	"context"
	"time"
)

// CommittedHead points to the current committed immutable manifest for one volume.
type CommittedHead struct {
	VolumeID      string
	ManifestSeq   uint64
	CheckpointSeq uint64
	ManifestKey   string
	UpdatedAt     time.Time
}

// HeadStore stores committed manifest pointers outside the object store hot path.
// CompareAndSwapCommittedHead must insert when expectedManifestSeq is zero and no
// row exists, and otherwise only advance the committed head when the existing
// manifest sequence exactly matches expectedManifestSeq.
type HeadStore interface {
	LoadCommittedHead(ctx context.Context, volumeID string) (*CommittedHead, error)
	CompareAndSwapCommittedHead(ctx context.Context, volumeID string, expectedManifestSeq uint64, head *CommittedHead) error
}
