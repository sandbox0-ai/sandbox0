package volume

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota"
)

// MaterializationResult separates durable materialization failures from
// best-effort storage observation failures.
type MaterializationResult struct {
	Manifest         *s0fs.Manifest
	ObservationError error
}

// SetStorageQuota updates the coordinator used by background and direct volume
// storage rewrites.
func (v *VolumeContext) SetStorageQuota(service *storagequota.Service) {
	if v == nil {
		return
	}
	v.storageQuotaMu.Lock()
	v.storageQuota = service
	v.storageQuotaMu.Unlock()
}

func (v *VolumeContext) configuredStorageQuota() *storagequota.Service {
	if v == nil {
		return nil
	}
	v.storageQuotaMu.RLock()
	defer v.storageQuotaMu.RUnlock()
	return v.storageQuota
}

// StorageQuotaTarget measures the complete quota target for this mounted
// volume, including its catalog row.
func (v *VolumeContext) StorageQuotaTarget() (teamquota.Values, error) {
	if v == nil || v.S0FS == nil {
		return nil, fmt.Errorf("volume does not expose an s0fs engine")
	}
	usage, err := v.S0FS.StorageUsage()
	if err != nil {
		return nil, err
	}
	return volumeStorageQuotaTarget(usage)
}

// MutateStorage runs one volume mutation behind the durable owner fence. The
// caller supplies a complete-target bound rather than a guessed delta.
func (v *VolumeContext) MutateStorage(
	ctx context.Context,
	service *storagequota.Service,
	operationKind string,
	maximum storagequota.Bound,
	mutate func() error,
) error {
	if v == nil || v.S0FS == nil {
		return fmt.Errorf("volume does not expose an s0fs engine")
	}
	if service == nil {
		service = v.configuredStorageQuota()
	}
	if service == nil {
		return &teamquota.UnavailableError{
			Operation: "enforce volume storage quota",
			Err:       errors.New("storage quota service is not configured"),
		}
	}
	if maximum == nil || mutate == nil {
		return &teamquota.UnavailableError{
			Operation: "enforce volume storage quota",
			Err:       errors.New("storage mutation bound and callback are required"),
		}
	}
	return service.Mutate(
		ctx,
		service.VolumeOwner(v.TeamID, v.VolumeID),
		operationKind,
		v.StorageQuotaTarget,
		maximum,
		mutate,
		v.StorageQuotaTarget,
	)
}

// SyncMaterialize persists pending S0FS changes behind the volume's durable
// TeamQuota fence and observes the resulting storage state.
func (v *VolumeContext) SyncMaterialize(ctx context.Context) (MaterializationResult, error) {
	return v.syncMaterialize(ctx, false)
}

// EnsureMaterialized quota-fences forced conversion of inline file data to
// immutable segments.
func (v *VolumeContext) EnsureMaterialized(ctx context.Context) (MaterializationResult, error) {
	return v.syncMaterialize(ctx, true)
}

func (v *VolumeContext) syncMaterialize(ctx context.Context, force bool) (MaterializationResult, error) {
	if v == nil || v.S0FS == nil {
		return MaterializationResult{}, fmt.Errorf("volume does not expose an s0fs engine")
	}
	if _, required, err := v.S0FS.PlannedMaterializationStorageUsage(ctx, force); err != nil {
		return MaterializationResult{}, err
	} else if !required {
		return MaterializationResult{}, nil
	}

	var manifest *s0fs.Manifest
	operationKind := "volume_materialize"
	if force {
		operationKind = "volume_ensure_materialized"
	}
	err := v.MutateStorage(
		ctx,
		nil,
		operationKind,
		func(before teamquota.Values) (teamquota.Values, error) {
			usage, required, err := v.S0FS.PlannedMaterializationStorageUsage(ctx, force)
			if err != nil {
				return nil, err
			}
			if !required {
				return before.Clone(), nil
			}
			target, err := volumeStorageQuotaTarget(usage)
			if err != nil {
				return nil, err
			}
			return quotaTargetAtLeast(before, target), nil
		},
		func() error {
			var err error
			if force {
				manifest, err = v.S0FS.EnsureMaterialized(ctx)
			} else {
				manifest, err = v.S0FS.SyncMaterialize(ctx)
			}
			return err
		},
	)
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
	if _, required, err := v.S0FS.PlannedCompactionStorageUsage(ctx, opts); err != nil {
		return materialization, nil, err
	} else if !required {
		return materialization, nil, nil
	}

	var (
		manifest *s0fs.Manifest
		result   *s0fs.CompactionResult
	)
	err = v.MutateStorage(
		ctx,
		nil,
		"volume_compact",
		func(before teamquota.Values) (teamquota.Values, error) {
			usage, required, err := v.S0FS.PlannedCompactionStorageUsage(ctx, opts)
			if err != nil {
				return nil, err
			}
			if !required {
				return before.Clone(), nil
			}
			target, err := volumeStorageQuotaTarget(usage)
			if err != nil {
				return nil, err
			}
			return quotaTargetAtLeast(before, target), nil
		},
		func() error {
			var err error
			manifest, result, err = v.S0FS.Compact(ctx, opts)
			return err
		},
	)
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

func volumeStorageQuotaTarget(usage s0fs.StorageUsage) (teamquota.Values, error) {
	if usage.Bytes < 0 || usage.Objects < 0 {
		return nil, fmt.Errorf("volume storage usage must be non-negative")
	}
	if usage.Objects > math.MaxInt64-storagequota.CatalogObjectCount {
		return nil, fmt.Errorf("volume storage object count overflow")
	}
	return storagequota.VolumeTarget(
		usage.Bytes,
		usage.Objects+storagequota.CatalogObjectCount,
	), nil
}

func quotaTargetAtLeast(before, candidate teamquota.Values) teamquota.Values {
	target := before.Clone()
	if target == nil {
		target = make(teamquota.Values, len(candidate))
	}
	for key, value := range candidate {
		if value > target[key] {
			target[key] = value
		}
	}
	return target
}
