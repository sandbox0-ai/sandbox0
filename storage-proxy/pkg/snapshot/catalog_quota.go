package snapshot

import (
	"context"
	"errors"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
)

type catalogStorageRepository interface {
	ListSandboxVolumes(context.Context) ([]*db.SandboxVolume, error)
	ListSnapshots(context.Context) ([]*db.Snapshot, error)
}

const (
	catalogQuotaRecoveryBatchSize = 100
	maxCatalogQuotaRecoveryErrors = 16
)

type catalogQuotaRecoveryErrors struct {
	errs    []error
	omitted int
}

func (e *catalogQuotaRecoveryErrors) add(err error) {
	if err == nil {
		return
	}
	if len(e.errs) < maxCatalogQuotaRecoveryErrors {
		e.errs = append(e.errs, err)
		return
	}
	e.omitted++
}

func (e *catalogQuotaRecoveryErrors) err() error {
	if e == nil || (len(e.errs) == 0 && e.omitted == 0) {
		return nil
	}
	errs := append([]error(nil), e.errs...)
	if e.omitted > 0 {
		errs = append(errs, fmt.Errorf(
			"%d additional storage TeamQuota recovery errors omitted",
			e.omitted,
		))
	}
	return errors.Join(errs...)
}

// ReconcileCatalogStorage adopts every catalog-backed physical storage object
// into TeamQuota before storage-proxy accepts traffic. This intentionally
// performs a strict full catalog scan. Periodic recovery must use
// RecoverDueCatalogStorage instead so its cost is proportional to interrupted
// operations rather than the complete catalog.
func (m *Manager) ReconcileCatalogStorage(ctx context.Context) error {
	if m == nil || m.repo == nil {
		return fmt.Errorf("storage catalog repository is not configured")
	}
	if m.storageQuota == nil {
		return fmt.Errorf("storage TeamQuota coordinator is not configured")
	}
	catalog, ok := any(m.repo).(catalogStorageRepository)
	if !ok {
		return fmt.Errorf("storage catalog repository does not support startup reconciliation")
	}

	volumes, err := catalog.ListSandboxVolumes(ctx)
	if err != nil {
		return fmt.Errorf("list catalog volumes for TeamQuota reconciliation: %w", err)
	}
	for _, catalogVolume := range volumes {
		if err := m.reconcileCatalogVolume(ctx, catalogVolume); err != nil {
			return err
		}
	}

	snapshots, err := catalog.ListSnapshots(ctx)
	if err != nil {
		return fmt.Errorf("list catalog snapshots for TeamQuota reconciliation: %w", err)
	}
	for _, catalogSnapshot := range snapshots {
		if err := m.reconcileCatalogSnapshot(ctx, catalogSnapshot); err != nil {
			return err
		}
	}
	return m.RecoverDueCatalogStorage(ctx)
}

// RecoverDueCatalogStorage resolves only expired PostgreSQL TeamQuota
// operations. Each stable keyset page is followed by point reads of the
// corresponding catalog owners, so a periodic pass never lists the complete
// volume or snapshot catalog.
func (m *Manager) RecoverDueCatalogStorage(ctx context.Context) error {
	if m == nil || m.repo == nil {
		return fmt.Errorf("storage catalog repository is not configured")
	}
	if m.storageQuota == nil {
		return fmt.Errorf("storage TeamQuota coordinator is not configured")
	}
	return errors.Join(
		m.recoverDueCatalogStorageKind(ctx, storagequota.OwnerKindVolume),
		m.recoverDueCatalogStorageKind(ctx, storagequota.OwnerKindSnapshot),
	)
}

func (m *Manager) recoverDueCatalogStorageKind(
	ctx context.Context,
	ownerKind string,
) error {
	afterAllocationID := ""
	var recoveryErrs catalogQuotaRecoveryErrors
	for {
		allocations, err := m.storageQuota.ListDueRecoveryAllocations(
			ctx,
			ownerKind,
			afterAllocationID,
			catalogQuotaRecoveryBatchSize,
		)
		if err != nil {
			return errors.Join(recoveryErrs.err(), fmt.Errorf(
				"list expired %s TeamQuota operations: %w",
				ownerKind,
				err,
			))
		}
		if len(allocations) == 0 {
			return recoveryErrs.err()
		}
		for i := range allocations {
			if err := ctx.Err(); err != nil {
				return errors.Join(recoveryErrs.err(), err)
			}
			if err := m.recoverDueCatalogAllocation(ctx, ownerKind, &allocations[i]); err != nil {
				// One corrupt or temporarily unreadable owner must not starve
				// later allocations in the same stable page.
				recoveryErrs.add(err)
			}
		}
		if len(allocations) < catalogQuotaRecoveryBatchSize {
			return recoveryErrs.err()
		}
		nextAfter := allocations[len(allocations)-1].AllocationID
		if nextAfter <= afterAllocationID {
			return errors.Join(recoveryErrs.err(), fmt.Errorf(
				"%s TeamQuota recovery pagination did not advance after %q",
				ownerKind,
				afterAllocationID,
			))
		}
		afterAllocationID = nextAfter
	}
}

func (m *Manager) recoverDueCatalogAllocation(
	ctx context.Context,
	ownerKind string,
	allocation *teamquota.RecoveryAllocation,
) error {
	if allocation == nil {
		return fmt.Errorf("expired %s TeamQuota page contains a nil allocation", ownerKind)
	}
	if allocation.Owner.Kind != ownerKind {
		return fmt.Errorf(
			"expired %s TeamQuota page contains owner kind %q",
			ownerKind,
			allocation.Owner.Kind,
		)
	}
	if allocation.Operation == nil || !allocation.ReconcileDue {
		return fmt.Errorf(
			"expired %s TeamQuota page contains non-due allocation %s",
			ownerKind,
			allocation.AllocationID,
		)
	}

	switch ownerKind {
	case storagequota.OwnerKindVolume:
		return m.recoverDueCatalogVolume(ctx, allocation)
	case storagequota.OwnerKindSnapshot:
		return m.recoverDueCatalogSnapshot(ctx, allocation)
	default:
		return fmt.Errorf("unsupported storage TeamQuota owner kind %q", ownerKind)
	}
}

func (m *Manager) recoverDueCatalogVolume(
	ctx context.Context,
	allocation *teamquota.RecoveryAllocation,
) error {
	catalogVolume, err := m.repo.GetSandboxVolume(ctx, allocation.Owner.ID)
	switch {
	case err == nil:
		if catalogVolume.TeamID != allocation.Owner.TeamID {
			return fmt.Errorf(
				"catalog volume %s team %s conflicts with TeamQuota owner %s",
				catalogVolume.ID,
				catalogVolume.TeamID,
				allocation.Owner.TeamID,
			)
		}
		return m.reconcileCatalogVolume(ctx, catalogVolume)
	case errors.Is(err, db.ErrNotFound):
		aborted, abortErr := m.storageQuota.AbortAbsentCatalogVolumeCreate(
			ctx,
			allocation.Owner,
		)
		if abortErr != nil {
			return fmt.Errorf(
				"recover absent catalog volume %s TeamQuota operation: %w",
				allocation.Owner.ID,
				abortErr,
			)
		}
		if m.logger != nil {
			entry := m.logger.WithFields(logrus.Fields{
				"volume_id": allocation.Owner.ID,
				"team_id":   allocation.Owner.TeamID,
			})
			if aborted {
				entry.Warn("Aborted expired zero-base volume create with no catalog row")
			} else {
				entry.Debug("Retaining expired volume TeamQuota operation with no catalog row")
			}
		}
		return nil
	default:
		return fmt.Errorf(
			"read catalog volume %s for TeamQuota recovery: %w",
			allocation.Owner.ID,
			err,
		)
	}
}

func (m *Manager) recoverDueCatalogSnapshot(
	ctx context.Context,
	allocation *teamquota.RecoveryAllocation,
) error {
	catalogSnapshot, err := m.repo.GetSnapshot(ctx, allocation.Owner.ID)
	switch {
	case err == nil:
		if catalogSnapshot.TeamID != allocation.Owner.TeamID {
			return fmt.Errorf(
				"catalog snapshot %s team %s conflicts with TeamQuota owner %s",
				catalogSnapshot.ID,
				catalogSnapshot.TeamID,
				allocation.Owner.TeamID,
			)
		}
		return m.reconcileCatalogSnapshot(ctx, catalogSnapshot)
	case errors.Is(err, db.ErrNotFound):
		// Snapshot state is persisted before the catalog row. Absence of the
		// row therefore cannot prove that the operation consumed no physical
		// storage, so the reservation must remain conservative.
		if m.logger != nil {
			m.logger.WithFields(logrus.Fields{
				"snapshot_id": allocation.Owner.ID,
				"team_id":     allocation.Owner.TeamID,
			}).Debug("Retaining expired snapshot TeamQuota operation with no catalog row")
		}
		return nil
	default:
		return fmt.Errorf(
			"read catalog snapshot %s for TeamQuota recovery: %w",
			allocation.Owner.ID,
			err,
		)
	}
}

func (m *Manager) reconcileCatalogVolume(ctx context.Context, catalogVolume *db.SandboxVolume) error {
	if catalogVolume == nil {
		return fmt.Errorf("catalog contains a nil volume")
	}
	owner := m.storageQuota.VolumeOwner(catalogVolume.TeamID, catalogVolume.ID)
	if err := m.storageQuota.AdoptExisting(
		ctx,
		owner,
		func() (teamquota.Values, error) {
			target := storagequota.VolumeTarget(0, storagequota.CatalogObjectCount)
			if volume.NormalizeBackend(catalogVolume.Backend) != volume.BackendS0FS {
				return target, nil
			}
			state, err := m.resolveS0FSForkState(ctx, catalogVolume.TeamID, catalogVolume.ID)
			if err != nil {
				return nil, err
			}
			return volumeStateTarget(state)
		},
	); err != nil {
		if physicalStorageMissing(err) {
			return m.allowPendingCatalogRelease(
				ctx,
				owner,
				"volume",
				catalogVolume.ID,
			)
		}
		return fmt.Errorf("measure and adopt catalog volume %s into TeamQuota: %w", catalogVolume.ID, err)
	}
	return nil
}

func (m *Manager) reconcileCatalogSnapshot(ctx context.Context, catalogSnapshot *db.Snapshot) error {
	if catalogSnapshot == nil {
		return fmt.Errorf("catalog contains a nil snapshot")
	}
	owner := m.storageQuota.SnapshotOwner(catalogSnapshot.TeamID, catalogSnapshot.ID)
	if err := m.storageQuota.AdoptExisting(
		ctx,
		owner,
		func() (teamquota.Values, error) {
			cfg, err := m.s0fsConfig(catalogSnapshot.TeamID, catalogSnapshot.VolumeID)
			if err != nil {
				return nil, err
			}
			state, err := m.loadS0FSSnapshotState(ctx, cfg, catalogSnapshot)
			if err != nil {
				return nil, err
			}
			return snapshotStateTarget(state)
		},
	); err != nil {
		if physicalStorageMissing(err) {
			return m.allowPendingCatalogRelease(
				ctx,
				owner,
				"snapshot",
				catalogSnapshot.ID,
			)
		}
		return fmt.Errorf("measure and adopt catalog snapshot %s into TeamQuota: %w", catalogSnapshot.ID, err)
	}
	return nil
}

func (m *Manager) allowPendingCatalogRelease(
	ctx context.Context,
	owner teamquota.Owner,
	resourceKind string,
	resourceID string,
) error {
	pending, err := m.storageQuota.PendingRelease(ctx, owner)
	if err != nil {
		return fmt.Errorf("inspect missing catalog %s %s TeamQuota release: %w", resourceKind, resourceID, err)
	}
	if !pending {
		return fmt.Errorf("catalog %s %s has no physical state and no pending TeamQuota release", resourceKind, resourceID)
	}
	if m.logger != nil {
		m.logger.WithFields(logrus.Fields{
			"resource_kind": resourceKind,
			"resource_id":   resourceID,
			"team_id":       owner.TeamID,
		}).Warn("Leaving interrupted storage release pending for idempotent delete retry")
	}
	return nil
}

func physicalStorageMissing(err error) bool {
	return errors.Is(err, s0fs.ErrMaterializedManifestNotFound) ||
		errors.Is(err, s0fs.ErrSnapshotNotFound) ||
		objectstore.IsNotFound(err)
}
