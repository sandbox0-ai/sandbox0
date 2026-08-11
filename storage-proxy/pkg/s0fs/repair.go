package s0fs

import (
	"context"
	"errors"
	"fmt"
)

// RepairCommittedState is the explicit recovery boundary for replacing a
// committed manifest that cannot be opened. It advances the exact current head
// and rewrites every referenced segment before publishing the replacement.
func RepairCommittedState(ctx context.Context, cfg Config, state *SnapshotState) (*Manifest, error) {
	ctx = nonNilContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if state == nil || cfg.ObjectStore == nil || cfg.HeadStore == nil {
		return nil, fmt.Errorf("%w: repair requires state, object storage, and a committed head store", ErrInvalidInput)
	}
	materializer := NewMaterializer(cfg.VolumeID, cfg.ObjectStore, cfg.HeadStore, cfg.ObjectStoreForVolume)
	materializer.SetEncryption(cfg.Encryption)
	materializer.SetSegmentTargetSize(cfg.SegmentTargetSize)
	materializer.SetStateFormatVersion(cfg.StateFormatVersion)
	expected, err := materializer.loadCommittedHead(ctx)
	if err != nil && !errors.Is(err, ErrCommittedHeadNotFound) {
		return nil, err
	}
	state = cloneState(state)
	normalizeState(state)
	expectedSeq := uint64(0)
	if expected != nil {
		expectedSeq = expected.ManifestSeq
	}
	if state.NextSeq <= expectedSeq+1 {
		state.NextSeq = expectedSeq + 2
	}
	manifest, _, err := materializer.compact(ctx, state, expected, CompactionOptions{
		SegmentTargetSize: cfg.SegmentTargetSize,
		Force:             true,
	}, false)
	return manifest, err
}
