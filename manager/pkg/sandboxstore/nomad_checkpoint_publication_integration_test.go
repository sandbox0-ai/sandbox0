package sandboxstore

import (
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func checkpointPublicationFixture(t *testing.T, name string) (*nomadPauseStoreFixture, protocol.MigrationPublicationRequest) {
	t.Helper()
	f, source, operation, policy := checkpointStoreFixture(t, name)
	c, prepare := prepareCheckpointStoreFixture(t, f, source, operation, policy)
	capture, err := f.store.AuthorizeNomadCheckpointCapture(f.ctx, *prepare, checkpointPreparedResponse(t, *prepare))
	require.NoError(t, err)
	request := checkpointWorkerPublication(t, f, c, source, *capture)
	return f, request
}

// checkpointWorkerPublication models the immutable disk cut returned by a node.
func checkpointWorkerPublication(t *testing.T, f *nomadPauseStoreFixture, c *NomadSandboxCheckpoint, source runtimecontrol.Assignment, capture protocol.MigrationCaptureRequest) protocol.MigrationPublicationRequest {
	t.Helper()
	d, err := capture.Digest()
	require.NoError(t, err)
	g := f.initial
	cutRequest := rootfshandoff.MigrationRootFSCutRequest{OperationID: capture.OperationID, CaptureRequestDigest: d,
		SourceBindingDigest: capture.BindingDigest, GenerationID: "migration-" + d}
	cut, err := rootfshandoff.NewMigrationRootFSCut(cutRequest, rootfshandoff.GenerationDescriptor{
		Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: cutRequest.GenerationID, FilesystemID: g.FilesystemID,
		SourceOCIDigest: g.SourceOCIDigest, BaseArtifactDigest: g.BaseArtifactDigest, BaseBlockRoot: g.BaseBlockRoot,
		CurrentBlockHead: g.CurrentBlockHead, WriterEpoch: f.writerEpoch, FormatGeneration: g.FormatGeneration,
		DurabilityState: g.DurabilityState, LocatorVersion: g.LocatorVersion + 1, Descriptor: g.Descriptor,
	}, 4)
	require.NoError(t, err)
	cpu := migrationCPUStoreResult(t, f, c.Evidence.Preflight).Launch
	profile, err := cpu.GuestCPUProfile().Digest()
	require.NoError(t, err)
	request := protocol.MigrationPublicationRequest{CheckpointSource: &source, CPULaunch: &cpu, CompatibilityDigest: c.CompatibilityDigest,
		CPUFeaturesDigest: profile, Capture: protocol.MigrationCapture{Request: capture, RequestDigest: d, State: protocol.MigrationCaptureComplete, RootFS: &cut}}
	_, err = request.Digest()
	require.NoError(t, err)
	return request
}

func TestNomadCheckpointPublicationPinsExactImageAndRootFSBeforeCleanupIntegration(t *testing.T) {
	f, request := checkpointPublicationFixture(t, "publication")
	publication := migrationPublicationReceipt(t, request)
	_, err := f.store.CommitNomadCheckpointPublication(f.ctx, request, publication)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "node publication is not regional retention")
	authorized, err := f.store.AuthorizeNomadCheckpointPublication(f.ctx, request.Capture)
	require.NoError(t, err)
	require.Equal(t, request, *authorized)
	require.Empty(t, authorized.Assignment)
	retained, err := f.store.CommitNomadCheckpointPublication(f.ctx, *authorized, publication)
	require.NoError(t, err)
	require.NoError(t, retained.ValidateFor(*authorized, publication))
	var generation, checkpoint string
	var runtimeGeneration int64
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT generation_id,checkpoint_id,runtime_generation
		FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID).Scan(&generation, &checkpoint, &runtimeGeneration))
	require.Equal(t, request.Capture.RootFS.Generation.GenerationID, generation)
	require.Equal(t, request.Capture.Request.OperationID, checkpoint)
	require.Equal(t, request.Capture.Request.SourceGeneration, runtimeGeneration)
	retry, err := NewPGSandboxStore(f.pool).CommitNomadCheckpointPublication(f.ctx, *authorized, publication)
	require.NoError(t, err)
	require.Equal(t, retained, retry)
	wrong := publication
	wrong.Reference.ManifestDigest = digest.FromString("different-memory").String()
	_, err = f.store.CommitNomadCheckpointPublication(f.ctx, *authorized, wrong)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, f.initialGenerationID, filesystem.HeadGenerationID, "publication cannot bypass physical source fencing")
	var active int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
	require.Equal(t, 1, active, "publication cannot release compute without cleanup")
	_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID)
	require.Error(t, err, "capture cannot lose its durable memory owner")
	_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.rootfs_generations WHERE generation_id=$1`, generation)
	require.Error(t, err, "memory retention must pin matching disk")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoints SET evidence=evidence-'published' WHERE operation_id=$1`, checkpoint)
	require.Error(t, err)
}

func TestNomadCheckpointPublicationRejectsDifferentCutOrCPUIntegration(t *testing.T) {
	f, request := checkpointPublicationFixture(t, "publication-binding")
	for _, mutate := range []func(*rootfshandoff.GenerationDescriptor){
		func(g *rootfshandoff.GenerationDescriptor) { g.WriterEpoch++ },
		func(g *rootfshandoff.GenerationDescriptor) { g.FilesystemID = "other-filesystem" },
		func(g *rootfshandoff.GenerationDescriptor) { g.LocatorVersion++ },
	} {
		changed := request.Capture
		g := changed.RootFS.Generation
		mutate(&g)
		cut, err := rootfshandoff.NewMigrationRootFSCut(changed.RootFS.Request, g, changed.RootFS.Sequence)
		require.NoError(t, err)
		changed.RootFS = &cut
		_, err = f.store.AuthorizeNomadCheckpointPublication(f.ctx, changed)
		require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	}
	authorized, err := f.store.AuthorizeNomadCheckpointPublication(f.ctx, request.Capture)
	require.NoError(t, err)
	wrong := *authorized
	launch := *wrong.CPULaunch
	launch.ExecutableDigest = digest.FromString("different-runtime").String()
	wrong.CPULaunch = &launch
	_, err = f.store.CommitNomadCheckpointPublication(f.ctx, wrong, migrationPublicationReceipt(t, wrong))
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	var refs int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.sandbox_runtime_checkpoint_refs`).Scan(&refs))
	require.Zero(t, refs)
}
