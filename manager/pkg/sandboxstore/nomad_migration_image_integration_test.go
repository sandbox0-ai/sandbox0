package sandboxstore

import (
	"sync"
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadMigrationImagePreparationGatesSourceFenceIntegration(t *testing.T) {
	f, reservation, publication := migrationPublicationStoreFixture(t, "target-image")
	_, err := f.store.AuthorizeNomadSandboxMigrationImagePreparation(f.ctx, publication.Assignment)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	require.NoError(t, err)
	published := migrationPublicationReceipt(t, publication)
	require.NoError(t, f.store.CommitNomadSandboxMigrationPublication(f.ctx, publication, published))
	fence := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: published}
	_, err = f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, fence)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "publication is not destination readiness")
	request, err := f.store.AuthorizeNomadSandboxMigrationImagePreparation(f.ctx, publication.Assignment)
	require.NoError(t, err)
	require.Equal(t, reservation.TargetSlot.ID, request.Target.SlotID)
	require.Equal(t, reservation.TargetResourceLease, request.Resources)
	_, err = f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, fence)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "download intent is not completion")
	digest, err := request.Digest()
	require.NoError(t, err)
	receipt := protocol.MigrationImagePrepared{RequestDigest: digest, ManifestDigest: published.Reference.ManifestDigest, TotalBytes: 8192}
	wrong := receipt
	wrong.RequestDigest = "changed"
	require.Error(t, f.store.CommitNomadSandboxMigrationImagePreparation(f.ctx, *request, wrong))
	const workers = 8
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			errs[i] = NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationImagePreparation(f.ctx, *request, receipt)
		})
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	requestAgain, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationImagePreparation(f.ctx, publication.Assignment)
	require.NoError(t, err)
	require.Equal(t, request, requestAgain)
	wrong = receipt
	wrong.TotalBytes++
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationImagePreparation(f.ctx, *request, wrong), ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET target_image_receipt=NULL WHERE operation_id=$1`, publication.Assignment.OperationID)
	require.Error(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, reservation.TargetSlot.ID)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, fence)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "a stale destination receipt cannot detach a source after destination admission expires")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()+INTERVAL '1 minute' WHERE slot_id=$1`, reservation.TargetSlot.ID)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, fence)
	require.NoError(t, err)
	target, err := f.store.GetRuntimeSlot(f.ctx, reservation.TargetSlot.ID)
	require.NoError(t, err)
	require.Empty(t, target.SandboxID)
	require.Empty(t, target.WriterGrantID, "image readiness is not writer authority")
}
