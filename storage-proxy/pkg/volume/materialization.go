package volume

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

// MaterializationResult separates durable materialization failures from
// best-effort storage observation failures.
type MaterializationResult struct {
	Manifest         *s0fs.Manifest
	ObservationError error
}

// SyncMaterialize persists pending S0FS changes and observes the resulting
// storage state. Volume callers should use this method instead of calling the
// S0FS engine directly so metering remains consistent across runtimes.
func (v *VolumeContext) SyncMaterialize(ctx context.Context) (MaterializationResult, error) {
	if v == nil || v.S0FS == nil {
		return MaterializationResult{}, fmt.Errorf("volume does not expose an s0fs engine")
	}
	manifest, err := v.S0FS.SyncMaterialize(ctx)
	if err != nil {
		return MaterializationResult{Manifest: manifest}, err
	}
	return MaterializationResult{
		Manifest:         manifest,
		ObservationError: v.observeMaterializedManifest(ctx, manifest),
	}, nil
}

// Compact materializes pending S0FS changes, compacts persisted segments, and
// observes the resulting storage state.
func (v *VolumeContext) Compact(ctx context.Context, opts s0fs.CompactionOptions) (MaterializationResult, *s0fs.CompactionResult, error) {
	if v == nil || v.S0FS == nil {
		return MaterializationResult{}, nil, fmt.Errorf("volume does not expose an s0fs engine")
	}
	materialization, err := v.SyncMaterialize(ctx)
	if err != nil {
		return materialization, nil, err
	}
	manifest, result, err := v.S0FS.Compact(ctx, opts)
	if err != nil {
		return materialization, result, err
	}
	if manifest != nil {
		materialization.Manifest = manifest
	}
	materialization.ObservationError = errors.Join(
		materialization.ObservationError,
		v.observeMaterializedManifest(ctx, manifest),
	)
	return materialization, result, nil
}

func (v *VolumeContext) observeMaterializedManifest(ctx context.Context, manifest *s0fs.Manifest) error {
	if v == nil || v.Observer == nil || manifest == nil || manifest.State == nil {
		return nil
	}
	return v.Observer.ObserveVolumeState(ctx, v.VolumeID, v.TeamID, manifest.State, time.Now().UTC())
}
