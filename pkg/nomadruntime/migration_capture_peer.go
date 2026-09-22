package nomadruntime

import (
	"context"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type MigrationCapturePeerPreparer interface {
	PrepareMigrationCapturePeer(context.Context, protocol.MigrationCapturePeerRequest) (*protocol.MigrationCapturePeerPrepared, error)
}

// PrepareMigrationCapturePeer records the exact regional grant before any
// tentative bytes are accepted. Its acknowledgement grants no execution and
// does not attest that a stream has started or that any image is complete.
func (d *nodeRuntime) PrepareMigrationCapturePeer(ctx context.Context, request protocol.MigrationCapturePeerRequest) (*protocol.MigrationCapturePeerPrepared, error) {
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if err := d.validateMigrationStagingRequest(ctx, request.Staging); err != nil {
		return nil, err
	}
	if d.migrationPeer == nil || d.migrationPeer.endpoint != request.LocalReceipt().Peer ||
		runtimecheckpoint.PeerCertificateDigest(d.migrationPeer.identity) != request.LocalReceipt().PeerCertificateSHA256 {
		return nil, errdefs.ErrFailedPrecondition
	}
	slot := request.Staging.Target.SlotID
	if !d.beginReconciliation(slot, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(slot)
	if err := d.checkMigrationStaging(false); err != nil {
		return nil, err
	}
	record, err := d.journal.Get(slot)
	if err != nil {
		return nil, err
	}
	if (record.MigrationStaging == nil || record.MigrationStaging.CapturePeer == nil) && !request.Staging.IsSource() {
		if d.runtime == nil || d.runner == nil {
			return nil, errdefs.ErrUnavailable
		}
		if err := d.requireUnusedMigrationDestination(ctx, record.Registration, request.Staging.Target); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.journal.recordMigrationCapturePeer(request); err != nil {
		return nil, err
	}
	return &protocol.MigrationCapturePeerPrepared{RequestDigest: want}, nil
}

// The caller holds exclusive slot reconciliation after canceling and joining
// any stream. Physical absence is synchronized before the durable tombstone.
func (d *nodeRuntime) discardMigrationCapturePeer(ctx context.Context, slot string, c *MigrationCapturePeerCustody) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.checkMigrationStaging(false); err != nil {
		return err
	}
	if err := d.closeMigrationCapturePeerCache(slot, c.RequestDigest); err != nil {
		return err
	}
	if err := ensureMigrationStagingDirectory(d.journal.migrationRoot); err != nil {
		return err
	}
	if err := removeMigrationImage(d.journal.migrationRoot, c.ImageDirectory); err != nil {
		return err
	}
	return d.journal.recordMigrationCapturePeerRemoved(slot, c.RequestDigest)
}
