package snapshot

import (
	"context"
	"fmt"
	"math"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota"
)

func (m *Manager) quotaService() (*storagequota.Service, error) {
	if m == nil || m.storageQuota == nil {
		return nil, &teamquota.UnavailableError{
			Operation: "enforce storage quota",
			Err:       fmt.Errorf("storage quota service is not configured"),
		}
	}
	return m.storageQuota, nil
}

func volumeStateTarget(state *s0fs.SnapshotState) (teamquota.Values, error) {
	usage, err := s0fs.StateStorageUsage(state)
	if err != nil {
		return nil, err
	}
	return volumeUsageTarget(usage)
}

func plannedMaterializedVolumeTarget(volumeID string, state *s0fs.SnapshotState, segmentTargetSize uint64) (teamquota.Values, error) {
	usage, err := s0fs.PlanMaterializationStorageUsage(volumeID, state, segmentTargetSize)
	if err != nil {
		return nil, err
	}
	return volumeUsageTarget(usage)
}

func volumeUsageTarget(usage s0fs.StorageUsage) (teamquota.Values, error) {
	if usage.Objects > math.MaxInt64-storagequota.CatalogObjectCount {
		return nil, fmt.Errorf("volume storage object count overflow")
	}
	return storagequota.VolumeTarget(
		usage.Bytes,
		usage.Objects+storagequota.CatalogObjectCount,
	), nil
}

func snapshotStateTarget(state *s0fs.SnapshotState) (teamquota.Values, error) {
	usage, err := s0fs.StateStorageUsage(state)
	if err != nil {
		return nil, err
	}
	if usage.Objects > math.MaxInt64-storagequota.CatalogObjectCount {
		return nil, fmt.Errorf("snapshot storage object count overflow")
	}
	return storagequota.SnapshotTarget(
		usage.Bytes,
		usage.Objects+storagequota.CatalogObjectCount,
	), nil
}

func volumeEngineTarget(engine *s0fs.Engine) (teamquota.Values, error) {
	if engine == nil {
		return nil, fmt.Errorf("s0fs engine is unavailable")
	}
	usage, err := engine.StorageUsage()
	if err != nil {
		return nil, err
	}
	return volumeUsageTarget(usage)
}

func targetAtLeast(desired teamquota.Values) storagequota.Bound {
	return func(before teamquota.Values) (teamquota.Values, error) {
		target := before.Clone()
		for key, value := range desired {
			if value > target[key] {
				target[key] = value
			}
		}
		return target, nil
	}
}

func (m *Manager) mutateVolumeStorage(
	ctx context.Context,
	teamID string,
	volumeID string,
	operationKind string,
	before storagequota.Measure,
	maximum storagequota.Bound,
	mutate func() error,
	exact storagequota.Measure,
) error {
	service, err := m.quotaService()
	if err != nil {
		return err
	}
	return service.Mutate(
		ctx,
		service.VolumeOwner(teamID, volumeID),
		operationKind,
		before,
		maximum,
		mutate,
		exact,
	)
}

func (m *Manager) mutateSnapshotStorage(
	ctx context.Context,
	teamID string,
	snapshotID string,
	operationKind string,
	target teamquota.Values,
	mutate func() error,
	exact storagequota.Measure,
) error {
	service, err := m.quotaService()
	if err != nil {
		return err
	}
	return service.Mutate(
		ctx,
		service.SnapshotOwner(teamID, snapshotID),
		operationKind,
		func() (teamquota.Values, error) {
			return storagequota.SnapshotTarget(0, 0), nil
		},
		func(teamquota.Values) (teamquota.Values, error) {
			return target.Clone(), nil
		},
		mutate,
		exact,
	)
}
