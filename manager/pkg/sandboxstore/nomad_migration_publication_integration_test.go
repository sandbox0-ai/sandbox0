package sandboxstore

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationPublicationStoreFixture(t *testing.T, suffix string) (*nomadPauseStoreFixture, *NomadSandboxMigrationReservation, protocol.MigrationPublicationRequest) {
	t.Helper()
	f, assignment := migrationStoreFixture(t, suffix)
	migrationReadyTarget(t, f, suffix, "b")
	reservation, err := f.store.ReserveNomadSandboxMigration(f.ctx, assignment)
	require.NoError(t, err)
	cpuLaunch := retainMigrationEligibilityFixture(t, f, assignment)
	cpuProfile, err := cpuLaunch.Observation.Profile.Digest()
	require.NoError(t, err)
	prepare, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, assignment, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID))
	require.NoError(t, err)
	captureRequest, err := f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *prepare, preparedMigrationResponse(t, *prepare))
	require.NoError(t, err)
	d, err := captureRequest.Digest()
	require.NoError(t, err)
	g := f.initial
	cutRequest := rootfshandoff.MigrationRootFSCutRequest{OperationID: assignment.OperationID, CaptureRequestDigest: d, SourceBindingDigest: captureRequest.BindingDigest, GenerationID: "migration-" + d}
	cut, err := rootfshandoff.NewMigrationRootFSCut(cutRequest, rootfshandoff.GenerationDescriptor{
		Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: cutRequest.GenerationID, FilesystemID: g.FilesystemID,
		SourceOCIDigest: g.SourceOCIDigest, BaseArtifactDigest: g.BaseArtifactDigest, BaseBlockRoot: g.BaseBlockRoot,
		CurrentBlockHead: g.CurrentBlockHead, WriterEpoch: f.writerEpoch, FormatGeneration: g.FormatGeneration,
		DurabilityState: g.DurabilityState, LocatorVersion: g.LocatorVersion + 1, Descriptor: g.Descriptor}, 4)
	require.NoError(t, err)
	request := protocol.MigrationPublicationRequest{Capture: protocol.MigrationCapture{Request: *captureRequest, RequestDigest: d, State: protocol.MigrationCaptureComplete, RootFS: &cut},
		Assignment: assignment, CompatibilityDigest: reservation.SourceSlot.CompatibilityDigest, CPUFeaturesDigest: cpuProfile, CPULaunch: cpuLaunch}
	_, err = request.Digest()
	require.NoError(t, err)
	return f, reservation, request
}

func migrationPublicationReceipt(t *testing.T, request protocol.MigrationPublicationRequest) protocol.MigrationPublication {
	t.Helper()
	binding, err := request.Binding()
	require.NoError(t, err)
	bindingDigest, err := binding.Digest()
	require.NoError(t, err)
	requestDigest, err := request.Digest()
	require.NoError(t, err)
	return protocol.MigrationPublication{RequestDigest: requestDigest, Binding: binding, Reference: runtimecheckpoint.Reference{BindingDigest: bindingDigest, ManifestDigest: digest.FromString("uploaded-manifest-fixture").String()}}
}

func TestNomadMigrationPublicationRequiresPriorAuthorityAndExactCutIntegration(t *testing.T) {
	f, _, request := migrationPublicationStoreFixture(t, "publish-authority")
	receipt := migrationPublicationReceipt(t, request)
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationPublication(f.ctx, request, receipt), ErrNomadSandboxMigrationConflict)
	for _, mutate := range []func(*rootfshandoff.MigrationRootFSCut){
		func(c *rootfshandoff.MigrationRootFSCut) { c.Generation.FilesystemID = "another-filesystem" },
		func(c *rootfshandoff.MigrationRootFSCut) { c.Generation.WriterEpoch++ },
		func(c *rootfshandoff.MigrationRootFSCut) {
			c.Generation.BaseArtifactDigest = digest.FromString("another-artifact").String()
		},
		func(c *rootfshandoff.MigrationRootFSCut) { c.Generation.LocatorVersion++ },
	} {
		changed := request.Capture
		cut := *changed.RootFS
		mutate(&cut)
		sealed, err := rootfshandoff.NewMigrationRootFSCut(cut.Request, cut.Generation, cut.Sequence)
		require.NoError(t, err)
		changed.RootFS = &sealed
		_, err = f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, request.Assignment, changed, request.CPUFeaturesDigest)
		require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	}
	authorized, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, request.Assignment, request.Capture, request.CPUFeaturesDigest)
	require.NoError(t, err)
	require.Equal(t, request, *authorized)
	prepared, err := f.store.GetRootFSGeneration(f.ctx, request.Capture.RootFS.Generation.GenerationID)
	require.NoError(t, err)
	require.Equal(t, f.initialGenerationID, prepared.ParentGenerationID)
	require.Equal(t, request.Capture.RootFS.Generation.Descriptor, prepared.Descriptor)
	lifecycle, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, prepared.ID, lifecycle.PreparedGenerationID)
	_, err = f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, request.Assignment, request.Capture, digest.FromString("other-cpu").String())
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	wrong := receipt
	wrong.Binding.TeamID = "another-team"
	require.Error(t, f.store.CommitNomadSandboxMigrationPublication(f.ctx, request, wrong))
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET publication_request=NULL,publication_digest=NULL WHERE operation_id=$1`, request.Assignment.OperationID)
	require.Error(t, err, "upload intent is immutable recovery evidence")
}

func TestNomadMigrationPublicationConcurrentReceiptSurvivesRestartIntegration(t *testing.T) {
	f, reservation, request := migrationPublicationStoreFixture(t, "publish-retry")
	_, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, request.Assignment, request.Capture, request.CPUFeaturesDigest)
	require.NoError(t, err)
	receipt := migrationPublicationReceipt(t, request)
	const workers = 8
	errors := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			errors[i] = NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationPublication(f.ctx, request, receipt)
		})
	}
	wg.Wait()
	for _, err := range errors {
		require.NoError(t, err)
	}
	var payload []byte
	var phase string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT publication_receipt,lifecycle.phase FROM manager.sandbox_runtime_migrations migration JOIN manager.sandbox_lifecycle_txns lifecycle ON lifecycle.txn_id=migration.operation_id WHERE migration.operation_id=$1`, request.Assignment.OperationID).Scan(&payload, &phase))
	var stored protocol.MigrationPublication
	require.NoError(t, json.Unmarshal(payload, &stored))
	require.Equal(t, receipt, stored)
	require.Equal(t, SandboxLifecyclePhasePublishing, phase)
	require.NoError(t, NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationPublication(f.ctx, request, receipt))
	wrong := receipt
	wrong.Reference.ManifestDigest = digest.FromString("other-image").String()
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationPublication(f.ctx, request, wrong), ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET publication_receipt=NULL WHERE operation_id=$1`, request.Assignment.OperationID)
	require.Error(t, err)
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, f.writerEpoch, filesystem.WriterEpoch)
	require.Equal(t, f.initialGenerationID, filesystem.HeadGenerationID)
	sandbox, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, int64(1), sandbox.RuntimeGeneration)
	require.Equal(t, f.allocationID, sandbox.RuntimeID)
	target, err := f.store.GetRuntimeSlot(f.ctx, reservation.TargetSlot.ID)
	require.NoError(t, err)
	require.Empty(t, target.WriterGrantID)
	require.Empty(t, target.SandboxID)
}

func TestNomadMigrationPublicationRejectsWriterChangeBeforeReceiptIntegration(t *testing.T) {
	f, _, request := migrationPublicationStoreFixture(t, "publish-fence")
	_, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, request.Assignment, request.Capture, request.CPUFeaturesDigest)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.rootfs_filesystems SET writer_epoch=writer_epoch+1 WHERE filesystem_id=$1`, f.filesystem.ID)
	require.NoError(t, err)
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationPublication(f.ctx, request, migrationPublicationReceipt(t, request)), ErrNomadSandboxMigrationConflict)
}
