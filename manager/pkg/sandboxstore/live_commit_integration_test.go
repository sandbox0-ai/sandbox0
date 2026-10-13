package sandboxstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

func newLiveCommitFixture(t *testing.T) (*rootFSWriterCrashAbandonFixture, objectstore.ContextConditionalStore) {
	t.Helper()
	objects := objectstore.NewMemoryStore(t.Name()).(objectstore.ContextConditionalStore)
	image, err := os.CreateTemp(t.TempDir(), "base-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = image.Close() })
	require.NoError(t, image.Truncate(rootfsblock.LogicalBlockSize))
	imagePath := image.Name()
	require.NoError(t, image.Close())
	image, err = os.Open(imagePath)
	require.NoError(t, err)
	base, err := rootfsblock.BuildMaterializedSparseFileGeneration(t.Context(), image, 1<<30,
		rootfsblock.ObjectStorePublisher{Store: objects}, rootfsblock.BuildOptions{ObjectPrefix: "rootfs/live-test"}, nil)
	require.NoError(t, err)
	request := readyRootFSBaseArtifactTestRequest()
	request.Descriptor, request.BaseBlockRoot = base.Payload, base.Descriptor.MappingRoot.RootDigest
	f := newRootFSWriterCrashAbandonFixture(t, SandboxLifecycleSourceCrash, false, request)
	for _, ref := range base.References {
		_, err = f.pool.Exec(t.Context(), `INSERT INTO manager.rootfs_materialization_objects(object_key,object_kind,object_size,checksum,uploaded_at)
			VALUES($1,$2,$3,$4,NOW())`, ref.Key, ref.Kind, ref.Size, ref.Checksum)
		require.NoError(t, err)
		_, err = f.pool.Exec(t.Context(), `INSERT INTO manager.rootfs_base_artifact_objects VALUES($1,$2)`, request.ArtifactDigest, ref.Key)
		require.NoError(t, err)
	}
	return f, objects
}

func buildLiveCommit(t *testing.T, f *rootFSWriterCrashAbandonFixture, objects objectstore.ContextConditionalStore, base, parent *RootFSGeneration, sequence uint64, value byte) *PublishRootFSLiveCommitRequest {
	t.Helper()
	op := fmt.Sprintf("live/%s/%d", f.issued.Grant.ID, sequence)
	descriptor, err := rootfsblock.DecodeDescriptor(base.Descriptor)
	require.NoError(t, err)
	publisher := retentionNodePublisher{store: f.store, objects: objects, request: RootFSNodeUploadRequest{
		GrantID: f.issued.Grant.ID, NodeUID: "node-a", OperationID: op, WriterEpoch: f.issued.Grant.WriterEpoch,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: f.request.BindingDigest,
	}}
	built, err := rootfsblock.BuildIncrementalGeneration(t.Context(), objects, descriptor, []rootfsblock.BlockUpdate{
		{Sequence: sequence, Block: 0, Data: bytes.Repeat([]byte{value}, rootfsblock.LogicalBlockSize)},
	}, publisher, rootfsblock.BuildOptions{ObjectPrefix: "rootfs/live-test"})
	require.NoError(t, err)
	generation := &RootFSGeneration{ID: fmt.Sprintf("live-generation-%d", sequence), FilesystemID: f.filesystem.ID,
		ParentGenerationID: parent.ID, SourceOCIDigest: parent.SourceOCIDigest, BaseArtifactDigest: parent.BaseArtifactDigest,
		BaseBlockRoot: parent.BaseBlockRoot, CurrentBlockHead: built.Descriptor.MappingRoot.RootDigest, WriterEpoch: f.issued.Grant.WriterEpoch,
		FormatGeneration: parent.FormatGeneration, DurabilityState: RootFSGenerationStateS3Materialized, LocatorVersion: 1, Descriptor: built.Payload,
	}
	return &PublishRootFSLiveCommitRequest{GrantID: f.issued.Grant.ID, NodeUID: "node-a", OperationID: op,
		WriterEpoch: f.issued.Grant.WriterEpoch, BindingVersion: RootFSWriterBindingVersion, BindingDigest: f.request.BindingDigest,
		ExpectedCommittedGenerationID: parent.ID, BaseGenerationID: base.ID, CutSequence: sequence, Generation: generation,
	}
}

func liveInstallRequest(req *PublishRootFSLiveCommitRequest) *InstallRootFSLiveCommitRequest {
	return &InstallRootFSLiveCommitRequest{GrantID: req.GrantID, NodeUID: req.NodeUID, OperationID: req.OperationID, WriterEpoch: req.WriterEpoch,
		BindingVersion: req.BindingVersion, BindingDigest: req.BindingDigest, GenerationID: req.Generation.ID, CutSequence: req.CutSequence}
}

func TestRootFSLiveCommitProgressAndReceiptReplayIntegration(t *testing.T) {
	f, objects := newLiveCommitFixture(t)
	ctx := t.Context()
	first := buildLiveCommit(t, f, objects, f.initial, f.initial, 1, 11)
	forged := *first
	forged.NodeUID = "other-node"
	_, err := f.store.PublishRootFSLiveCommit(ctx, &forged)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	forged = *first
	forged.OperationID = protocol.LiveCommitOperationID(first.GrantID, 99)
	forged.CutSequence = 99
	_, err = f.store.PublishRootFSLiveCommit(ctx, &forged)
	require.ErrorIs(t, err, ErrRootFSGenerationConflict)
	p, err := f.store.PublishRootFSLiveCommit(ctx, first)
	require.NoError(t, err)
	require.Equal(t, first.Generation.ID, p.CommittedGenerationID)
	require.Equal(t, f.initial.ID, p.InstalledGenerationID)
	require.Zero(t, p.InstalledSequence)
	// Restart authority and lose the first reply. Publication remains exact and
	// neither advances the lifecycle head nor changes the immutable grant binding.
	p, err = NewPGSandboxStore(f.pool).PublishRootFSLiveCommit(ctx, first)
	require.NoError(t, err)
	require.Equal(t, first.Generation.ID, p.CommittedGenerationID)
	require.Equal(t, first.CutSequence, p.CommittedSequence)
	require.Equal(t, f.initial.ID, p.InstalledGenerationID)
	fs, err := f.store.GetRootFSFilesystem(ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, f.initial.ID, fs.HeadGenerationID)
	grant, err := f.store.GetRootFSWriterGrant(ctx, first.GrantID)
	require.NoError(t, err)
	require.Equal(t, f.initial.ID, grant.InitialGenerationID)
	forged = *first
	forged.Generation = &RootFSGeneration{}
	*forged.Generation = *first.Generation
	forged.Generation.LocatorVersion++
	_, err = f.store.PublishRootFSLiveCommit(ctx, &forged)
	require.ErrorIs(t, err, ErrRootFSGenerationConflict)
	// Recut a full WAL before local rotation to avoid duplicating a large suffix
	// when the normal node budget is exhausted. The old installed base stays pinned.
	second := buildLiveCommit(t, f, objects, f.initial, first.Generation, 2, 22)
	p, err = f.store.PublishRootFSLiveCommit(ctx, second)
	require.NoError(t, err)
	require.Equal(t, f.initial.ID, p.InstalledGenerationID)
	var pendingReceipts int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.rootfs_node_uploads WHERE grant_id=$1 AND state='published'`, first.GrantID).Scan(&pendingReceipts))
	require.Equal(t, 1, pendingReceipts, "recutting before installation must retain bounded publication receipts")
	rootObject, _ := rootfsblock.DecodeDescriptor(first.Generation.Descriptor)
	var reference rootfsblock.ObjectReference
	reference.Key = rootObject.MappingRoot.Object.Key
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT object_kind,object_size,checksum FROM manager.rootfs_materialization_objects WHERE object_key=$1`, reference.Key).Scan(&reference.Kind, &reference.Size, &reference.Checksum))
	require.ErrorIs(t, f.store.RecordRootFSNodeUpload(ctx, &RootFSNodeUploadRequest{GrantID: first.GrantID, NodeUID: first.NodeUID, OperationID: first.OperationID,
		WriterEpoch: first.WriterEpoch, BindingVersion: first.BindingVersion, BindingDigest: first.BindingDigest, Reference: reference}), ErrRootFSGenerationConflict,
		"a stale upload cannot recreate an obsolete operation after its receipt was compacted")
	_, err = f.store.InstallRootFSLiveCommit(ctx, liveInstallRequest(first))
	require.ErrorIs(t, err, ErrRootFSHeadConflict)
	p, err = f.store.InstallRootFSLiveCommit(ctx, liveInstallRequest(second))
	require.NoError(t, err)
	require.Equal(t, uint64(2), p.InstalledSequence)
	_, err = NewPGSandboxStore(f.pool).InstallRootFSLiveCommit(ctx, liveInstallRequest(second))
	require.NoError(t, err)
	third := buildLiveCommit(t, f, objects, second.Generation, second.Generation, 3, 33)
	p, err = f.store.PublishRootFSLiveCommit(ctx, third)
	require.NoError(t, err)
	require.Equal(t, second.Generation.ID, p.InstalledGenerationID)
	for range 10 {
		_, err = f.store.InventoryRootFSGeneration(ctx, objects, 100)
		require.NoError(t, err)
	}
	_, err = f.store.DeleteUnreferencedRootFSGenerations(ctx, "", 100)
	require.NoError(t, err)
	_, err = f.store.GetRootFSGeneration(ctx, second.Generation.ID)
	require.NoError(t, err, "installed old base must survive remote commit and inventory")
	_, err = f.store.GetRootFSGeneration(ctx, first.Generation.ID)
	require.ErrorIs(t, err, ErrRootFSFilesystemNotFound, "inventoried obsolete live checkpoints must be collectable")
	_, err = f.store.InstallRootFSLiveCommit(ctx, liveInstallRequest(third))
	require.NoError(t, err)
	_, err = f.store.DeleteUnreferencedRootFSGenerations(ctx, "", 100)
	require.NoError(t, err)
	_, err = f.store.GetRootFSGeneration(ctx, second.Generation.ID)
	require.ErrorIs(t, err, ErrRootFSFilesystemNotFound)
	var receipts int
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.rootfs_node_uploads WHERE grant_id=$1`, first.GrantID).Scan(&receipts))
	require.Equal(t, 1, receipts, "successful live operations retain bounded receipts")
	_, err = NewPGSandboxStore(f.pool).PublishRootFSLiveCommit(ctx, third)
	require.NoError(t, err, "exact retry survives parent and upload receipt collection")
	assertRetentionContents(t, objects, third.Generation, 33)
	require.ErrorContains(t, migrate.Down(ctx, f.pool, ".", migrate.WithBaseFS(storemigrations.FS), migrate.WithSchema(sandboxStoreSchemaName), migrate.WithLogger(noopSandboxStoreMigrateLogger{})),
		"live commit rollback requires retired writers")
	// Independent SQL fences protect against an older/incorrect authority writer.
	for _, statement := range []string{
		`UPDATE manager.rootfs_writer_live_progress SET installed_generation_ref=NULL`,
		`UPDATE manager.rootfs_writer_live_progress SET committed_sequence=1`,
		`UPDATE manager.rootfs_writer_live_progress SET operation_id='live/changed'`,
		`DELETE FROM manager.rootfs_writer_live_progress`,
		`UPDATE manager.rootfs_generations SET live_commit_sequence=live_commit_sequence+1 WHERE live_commit_sequence>0`,
	} {
		_, err = f.pool.Exec(ctx, statement)
		require.Error(t, err, statement)
	}
	_, err = f.pool.Exec(ctx, `DELETE FROM manager.rootfs_writer_live_progress`)
	require.ErrorContains(t, err, "live progress cannot be deleted under an active writer")
}

func TestRootFSLiveCommitTerminalPublicationRetainsInstalledBaseIntegration(t *testing.T) {
	f, objects := newLiveCommitFixture(t)
	ctx := t.Context()
	commit := buildLiveCommit(t, f, objects, f.initial, f.initial, 1, 11)
	_, err := f.store.PublishRootFSLiveCommit(ctx, commit)
	require.NoError(t, err)
	_, err = f.store.InstallRootFSLiveCommit(ctx, liveInstallRequest(commit))
	require.NoError(t, err)
	for range 5 {
		_, err = f.store.InventoryRootFSGeneration(ctx, objects, 100)
		require.NoError(t, err)
	}
	_, err = f.store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: commit.GrantID, WriterEpoch: commit.WriterEpoch, OperationID: f.request.OperationID, BindingVersion: commit.BindingVersion,
		BindingDigest: commit.BindingDigest, ExpectedOldGenerationID: f.initial.ID,
	})
	require.NoError(t, err)
	base, err := rootfsblock.DecodeDescriptor(commit.Generation.Descriptor)
	require.NoError(t, err)
	publisher := retentionNodePublisher{store: f.store, objects: objects, request: RootFSNodeUploadRequest{
		GrantID: commit.GrantID, NodeUID: commit.NodeUID, OperationID: f.request.OperationID, WriterEpoch: commit.WriterEpoch,
		BindingVersion: commit.BindingVersion, BindingDigest: commit.BindingDigest,
	}}
	built, err := rootfsblock.BuildIncrementalGeneration(ctx, objects, base, []rootfsblock.BlockUpdate{
		{Sequence: 2, Block: 1, Data: bytes.Repeat([]byte{22}, rootfsblock.LogicalBlockSize)},
	}, publisher, rootfsblock.BuildOptions{ObjectPrefix: "rootfs/live-test"})
	require.NoError(t, err)
	sealed := *commit.Generation
	sealed.ID = "terminal-after-live"
	sealed.ParentGenerationID = f.initial.ID
	sealed.CurrentBlockHead = built.Descriptor.MappingRoot.RootDigest
	sealed.Descriptor = built.Payload
	require.NoError(t, f.store.WithSandboxLock(ctx, f.sandboxID, func(ctx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		_, err := tx.(RootFSWriterGrantTx).CompleteRootFSWriterRetireAndPublishGeneration(ctx, &CompleteRootFSWriterRetireAndPublishGenerationRequest{
			LifecycleTxnID: f.request.LifecycleTxnID, GrantID: commit.GrantID, WriterEpoch: commit.WriterEpoch, OperationID: f.request.OperationID,
			BindingVersion: commit.BindingVersion, BindingDigest: commit.BindingDigest, ProofDigest: f.request.ProofDigest,
			ExpectedOldGenerationID: f.initial.ID, Generation: &sealed,
		})
		return err
	}))
	_, err = f.store.DeleteUnreferencedRootFSGenerations(ctx, "", 100)
	require.NoError(t, err)
	_, err = f.store.GetRootFSGeneration(ctx, commit.Generation.ID)
	require.NoError(t, err, "terminal descriptor inherits a live read base outside its launch-parent ancestry")
	for range 5 {
		_, err = f.store.InventoryRootFSGeneration(ctx, objects, 100)
		require.NoError(t, err)
	}
	_, err = f.store.DeleteUnreferencedRootFSGenerations(ctx, "", 100)
	require.NoError(t, err)
	_, err = f.store.GetRootFSGeneration(ctx, commit.Generation.ID)
	require.ErrorIs(t, err, ErrRootFSFilesystemNotFound)
	assertRetentionContents(t, objects, &sealed, 11)
	descriptor, err := rootfsblock.DecodeDescriptor(sealed.Descriptor)
	require.NoError(t, err)
	reader, err := rootfsblock.NewReader(objects, descriptor, 0)
	require.NoError(t, err)
	block := make([]byte, rootfsblock.LogicalBlockSize)
	_, err = reader.ReadAt(block, rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{22}, len(block)), block)
}

func TestRootFSLiveCommitMigrationEmptyRollbackIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPoolAt(t, 120)
	applySandboxStoreMigrationsThrough(t, pool, 121)
	require.NoError(t, migrate.Down(t.Context(), pool, ".", migrate.WithBaseFS(storemigrations.FS), migrate.WithSchema(sandboxStoreSchemaName), migrate.WithLogger(noopSandboxStoreMigrateLogger{})))
	var restored bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='manager.rootfs_generations'::regclass
		AND conname='rootfs_generations_filesystem_id_writer_epoch_key') AND to_regclass('manager.rootfs_writer_live_progress') IS NULL`).Scan(&restored))
	require.True(t, restored)
	applySandboxStoreMigrationsThrough(t, pool, 121)
}

func TestRootFSLiveCommitConcurrentCutsHaveOneCASWinnerIntegration(t *testing.T) {
	f, objects := newLiveCommitFixture(t)
	first := buildLiveCommit(t, f, objects, f.initial, f.initial, 1, 11)
	second := buildLiveCommit(t, f, objects, f.initial, f.initial, 2, 22)
	type outcome struct {
		request  *PublishRootFSLiveCommitRequest
		progress *RootFSLiveProgress
		err      error
	}
	results := make(chan outcome, 2)
	for _, request := range []*PublishRootFSLiveCommitRequest{first, second} {
		go func(request *PublishRootFSLiveCommitRequest) {
			progress, err := NewPGSandboxStore(f.pool).PublishRootFSLiveCommit(t.Context(), request)
			results <- outcome{request: request, progress: progress, err: err}
		}(request)
	}
	winners := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			require.ErrorIs(t, result.err, ErrRootFSHeadConflict)
			continue
		}
		winners++
		require.Equal(t, result.request.Generation.ID, result.progress.CommittedGenerationID)
		_, err := NewPGSandboxStore(f.pool).PublishRootFSLiveCommit(t.Context(), result.request)
		require.NoError(t, err)
	}
	require.Equal(t, 1, winners, "the previous remote generation fences concurrent submissions")
}

func TestRootFSLiveCommitCrashRestoresLastRemoteCutIntegration(t *testing.T) {
	f, objects := newLiveCommitFixture(t)
	ctx := t.Context()
	observe := &ObserveRootFSLiveCommitRequest{GrantID: f.issued.Grant.ID, NodeUID: "node-a", WriterEpoch: f.issued.Grant.WriterEpoch,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: f.request.BindingDigest}
	observation, err := f.store.ObserveRootFSLiveCommit(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, f.initial.ID, observation.Progress.CommittedGenerationID)
	require.Zero(t, observation.Progress.CommittedSequence)
	forged := *observe
	forged.NodeUID = "other-node"
	_, err = f.store.ObserveRootFSLiveCommit(ctx, &forged)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	commit := buildLiveCommit(t, f, objects, f.initial, f.initial, 9, 99)
	_, err = f.store.PublishRootFSLiveCommit(ctx, commit)
	require.NoError(t, err)
	// No local installation ack. Crash recovery must still use this durable cut,
	// after the usual physical terminal proof, rather than reverting to launch.
	_, err = f.pool.Exec(ctx, `UPDATE manager.rootfs_writer_grants SET lease_expires_at=NOW()-INTERVAL '1 minute' WHERE grant_id=$1`, commit.GrantID)
	require.NoError(t, err)
	_, err = f.store.BeginRootFSWriterCrashAbandon(ctx, f.beginRequest)
	require.NoError(t, err)
	observation, err = NewPGSandboxStore(f.pool).ObserveRootFSLiveCommit(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetiring, observation.WriterState)
	require.Equal(t, commit.Generation.ID, observation.Progress.CommittedGenerationID)
	require.Equal(t, f.initial.ID, observation.Progress.InstalledGenerationID)
	require.NoError(t, f.complete(t))
	observation, err = f.store.ObserveRootFSLiveCommit(ctx, observe)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, observation.WriterState)
	require.Equal(t, uint64(9), observation.Progress.CommittedSequence)
	fs, err := NewPGSandboxStore(f.pool).GetRootFSFilesystem(ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, commit.Generation.ID, fs.HeadGenerationID)
	var released bool
	require.NoError(t, f.pool.QueryRow(ctx, `SELECT committed_generation_ref IS NULL AND installed_generation_ref IS NULL FROM manager.rootfs_writer_live_progress WHERE grant_id=$1`, commit.GrantID).Scan(&released))
	require.True(t, released)
	require.NoError(t, f.complete(t), "terminal crash recovery remains an exact retry")
	_, err = f.store.PublishRootFSLiveCommit(ctx, commit)
	require.ErrorIs(t, err, ErrRootFSWriterGrantInvalidState)
	for range 5 {
		_, err = f.store.InventoryRootFSGeneration(ctx, objects, 100)
		require.NoError(t, err)
	}
	_, err = f.store.DeleteUnreferencedRootFSGenerations(ctx, "", 100)
	require.NoError(t, err)
	assertRetentionContents(t, objects, commit.Generation, 99)
	grant, err := f.store.GetRootFSWriterGrant(ctx, commit.GrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, grant.State)
	record, err := f.store.GetSandbox(ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, record.DesiredState)
	_, err = f.pool.Exec(ctx, `DELETE FROM manager.rootfs_writer_live_progress WHERE grant_id=$1`, commit.GrantID)
	require.NoError(t, err, "terminal audit retention can release progress after the crash head is installed")
}

func TestRootFSLiveCommitExpiredWriterCannotAdvanceIntegration(t *testing.T) {
	f, objects := newLiveCommitFixture(t)
	commit := buildLiveCommit(t, f, objects, f.initial, f.initial, 1, 1)
	_, err := f.pool.Exec(t.Context(), `UPDATE manager.rootfs_writer_grants SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE grant_id=$1`, commit.GrantID)
	require.NoError(t, err)
	_, err = f.store.PublishRootFSLiveCommit(t.Context(), commit)
	require.ErrorIs(t, err, ErrRootFSWriterLeaseExpired)
	// Renewal is a separate authority transition; node clocks cannot grant a
	// checkpoint permission that PostgreSQL's observation has already revoked.
	_, err = f.store.RenewRootFSWriterGrant(context.Background(), &RenewRootFSWriterGrantRequest{
		GrantID: commit.GrantID, WriterEpoch: commit.WriterEpoch, BindingVersion: commit.BindingVersion, BindingDigest: commit.BindingDigest, ConsumerNodeUID: commit.NodeUID,
	}, RootFSWriterLeaseRenewalPolicy{LeaseTTL: time.Minute, GracePeriod: RootFSWriterMaxRenewGrace})
	require.NoError(t, err)
	_, err = f.store.PublishRootFSLiveCommit(t.Context(), commit)
	require.NoError(t, err)
}
