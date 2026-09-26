package sandboxstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	"github.com/stretchr/testify/require"
)

// Legacy ctld builds use the same direct S3 builder as large retirement tails
// and memory cuts. They have no materialization batch catalog to rely on.
func TestRootFSNodeLegacyUploadRetentionS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, filesystem, current := newRetentionS3Filesystem(t, objects, prefix, "node-legacy")
	for i := 1; i <= 5; i++ {
		current = publishRetentionNodeCheckpoint(t, store, objects, prefix, current, filesystem.ID, fmt.Sprintf("node-legacy-%d", i), byte(i), false)
	}
	before := retentionS3Usage(t, objects, prefix)
	for range 20 {
		_, err := store.InventoryRootFSGeneration(t.Context(), objects, 100)
		require.NoError(t, err)
		_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
		require.NoError(t, err)
	}
	var count int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_generations`).Scan(&count))
	require.Equal(t, 1, count)
	assertRetentionContents(t, objects, current, 5)
	after := retentionS3Usage(t, objects, prefix)
	require.Less(t, after.bytes, before.bytes)
	require.Less(t, after.objects, before.objects)
	t.Logf("legacy direct node objects: %d -> %d; bytes: %d -> %d", before.objects, after.objects, before.bytes, after.bytes)
}

func TestRootFSNodeUploadCustodyS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, filesystem, current := newRetentionS3Filesystem(t, objects, prefix, "node-tracked")
	for i := 1; i <= 5; i++ {
		current = publishRetentionNodeCheckpoint(t, store, objects, prefix, current, filesystem.ID, fmt.Sprintf("node-tracked-%d", i), byte(i), true)
	}
	_, err := store.pool.Exec(t.Context(), `UPDATE manager.rootfs_node_uploads SET terminal_at=clock_timestamp()-INTERVAL '3 minutes' WHERE state='uploading'`)
	require.NoError(t, err)
	for range 20 {
		_, err := store.InventoryRootFSGeneration(t.Context(), objects, 100)
		require.NoError(t, err)
		_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
		require.NoError(t, err)
	}
	var holds int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_node_upload_objects`).Scan(&holds))
	require.Zero(t, holds)
	usage, err := store.ListRootFSStorageUsage(t.Context(), "team-1")
	require.NoError(t, err)
	require.Len(t, usage, 1)
	require.Positive(t, usage[0].StorageBytes)
	assertRetentionContents(t, objects, current, 5)
}

type retentionNodePublisher struct {
	store     *PGSandboxStore
	objects   objectstore.ContextConditionalStore
	request   RootFSNodeUploadRequest
	beforePut func(context.Context, string) error
}

func (p retentionNodePublisher) PutImmutable(ctx context.Context, key string, payload []byte) error {
	p.request.Reference = rootfsblock.ObjectReference{Key: key, Kind: rootfsblock.ObjectKindMappingPage, Size: int64(len(payload)), Checksum: digest.FromBytes(payload).String()}
	// The block builder uses these exact kind-specific content-addressed paths.
	if bytes.Contains([]byte(key), []byte("/packs/")) {
		p.request.Reference.Kind = rootfsblock.ObjectKindDataPack
	}
	if err := p.store.RecordRootFSNodeUpload(ctx, &p.request); err != nil {
		return err
	}
	if p.beforePut != nil {
		if err := p.beforePut(ctx, key); err != nil {
			return err
		}
	}
	if err := (rootfsblock.ObjectStorePublisher{Store: p.objects}).PutImmutable(ctx, key, payload); err != nil {
		return err
	}
	p.request.Uploaded = true
	return p.store.RecordRootFSNodeUpload(ctx, &p.request)
}

func publishRetentionNodeCheckpoint(t *testing.T, store *PGSandboxStore, objects objectstore.ContextConditionalStore, prefix string, parent *RootFSGeneration, filesystemID, id string, value byte, tracked bool, beforePut ...func(context.Context, string) error) *RootFSGeneration {
	t.Helper()
	ctx := t.Context()
	binding := sha256.Sum256([]byte(id))
	filesystem, err := store.GetRootFSFilesystem(ctx, filesystemID)
	require.NoError(t, err)
	issue := rootFSWriterGrantTestIssueRequest(filesystemID, id+"-writer", id+"-claim", id+"-slot", binding[:])
	issue.ExpectedFilesystemID, issue.InitialGenerationID, issue.ExpectedWriterEpoch = filesystemID, parent.ID, filesystem.WriterEpoch
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ConsumerNodeUID: "node-a", ConsumerAgentUID: "ctld-a", LeaseTTL: time.Minute})
	require.NoError(t, err)
	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: id + "-txn", BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ExpectedOldGenerationID: parent.ID})
	require.NoError(t, err)
	var publisher rootfsblock.ImmutableObjectPublisher = rootfsblock.ObjectStorePublisher{Store: objects}
	if tracked {
		publisher = retentionNodePublisher{store: store, objects: objects, request: RootFSNodeUploadRequest{GrantID: issue.GrantID, NodeUID: "node-a", OperationID: id + "-txn", WriterEpoch: issued.Grant.WriterEpoch, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:]}}
	}
	if tracked {
		orphanPayload := []byte("abandoned-upload-" + id)
		reference := rootfsblock.ObjectReference{Key: prefix + "/packs/sha256/" + digest.FromBytes(orphanPayload).Encoded(), Kind: rootfsblock.ObjectKindDataPack, Size: int64(len(orphanPayload)), Checksum: digest.FromBytes(orphanPayload).String()}
		request := RootFSNodeUploadRequest{GrantID: issue.GrantID, NodeUID: "node-a", OperationID: id + "-aborted", WriterEpoch: issued.Grant.WriterEpoch, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], Reference: reference}
		forged := request
		forged.NodeUID = "other-node"
		require.ErrorIs(t, store.RecordRootFSNodeUpload(ctx, &forged), ErrRootFSWriterGrantConflict)
		require.NoError(t, store.RecordRootFSNodeUpload(ctx, &request))
		require.NoError(t, (rootfsblock.ObjectStorePublisher{Store: objects}).PutImmutable(ctx, reference.Key, orphanPayload))
		// Lose the acknowledgement and restart the regional worker while owner live.
		require.NoError(t, NewPGSandboxStore(store.pool).ReconcileRootFSNodeUploadGarbage(ctx, "", 1))
		var protected int
		require.NoError(t, store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.rootfs_node_upload_objects WHERE object_key=$1`, reference.Key).Scan(&protected))
		require.Equal(t, 1, protected)
		usages, e := store.ListRootFSStorageUsage(ctx, "team-1")
		require.NoError(t, e)
		if len(usages) > 0 {
			var billed bool
			require.NoError(t, store.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_storage_team_objects WHERE object_key=$1)`, reference.Key).Scan(&billed))
			require.False(t, billed)
		}
	}
	if tracked && len(beforePut) > 0 {
		p := publisher.(retentionNodePublisher)
		p.beforePut = beforePut[0]
		publisher = p
	}
	base, err := rootfsblock.DecodeDescriptor(parent.Descriptor)
	require.NoError(t, err)
	built, err := rootfsblock.BuildIncrementalGeneration(ctx, objects, base, []rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{value}, rootfsblock.LogicalBlockSize)}}, publisher, rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.LogicalBlockSize})
	require.NoError(t, err)
	generation := &RootFSGeneration{ID: id, FilesystemID: filesystemID, ParentGenerationID: parent.ID, SourceOCIDigest: parent.SourceOCIDigest, BaseArtifactDigest: parent.BaseArtifactDigest, BaseBlockRoot: parent.BaseBlockRoot, CurrentBlockHead: built.Descriptor.MappingRoot.RootDigest, WriterEpoch: issued.Grant.WriterEpoch, FormatGeneration: parent.FormatGeneration, DurabilityState: RootFSGenerationStateS3Materialized, LocatorVersion: parent.LocatorVersion + 1, Descriptor: built.Payload}
	require.NoError(t, store.WithSandboxLock(ctx, filesystemID, func(ctx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		if err := tx.BeginLifecycleTxn(ctx, &SandboxLifecycleTxn{ID: id + "-txn", SandboxID: filesystemID, Kind: SandboxLifecycleKindPause, Phase: SandboxLifecyclePhasePublishing, ExpectedGenerationID: parent.ID}); err != nil {
			return err
		}
		_, err := tx.(RootFSWriterGrantTx).CompleteRootFSWriterRetireAndPublishGeneration(ctx, &CompleteRootFSWriterRetireAndPublishGenerationRequest{LifecycleTxnID: id + "-txn", GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: id + "-txn", BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ProofDigest: binding[:], ExpectedOldGenerationID: parent.ID, Generation: generation})
		return err
	}))
	return generation
}

func TestRootFSNodeUploadConcurrentCollectionS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, filesystem, initial := newRetentionS3Filesystem(t, objects, prefix, "node-race")
	first := publishRetentionNodeCheckpoint(t, store, objects, prefix, initial, filesystem.ID, "node-race-first", 1, true)
	second := publishRetentionNodeCheckpoint(t, store, objects, prefix, first, filesystem.ID, "node-race-second", 2, true)
	for range 3 {
		_, err := store.InventoryRootFSGeneration(t.Context(), objects, 100)
		require.NoError(t, err)
	}
	third := publishRetentionNodeCheckpoint(t, store, objects, prefix, second, filesystem.ID, "node-race-third", 1, true, func(ctx context.Context, key string) error {
		// Reuse the first version's content address. Collection runs after durable
		// reservation but before conditional PUT establishes the new generation.
		_, err := store.GarbageCollectRootFSFilesystemWithOptions(ctx, objects, "", 100, DeletePendingRootFSObjectsOptions{})
		if err != nil {
			return err
		}
		var pending bool
		if err = store.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.rootfs_object_deletions WHERE object_key=$1)`, key).Scan(&pending); err != nil {
			return err
		}
		require.False(t, pending, "a reserved object cannot enter physical deletion")
		assertRetentionContents(t, objects, second, 2)
		return nil
	})
	for range 3 {
		_, err := store.InventoryRootFSGeneration(t.Context(), objects, 100)
		require.NoError(t, err)
	}
	_, err := store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	assertRetentionContents(t, objects, third, 1)
}

// This starts with a real pre-custody database and raw node objects. More
// objects than one repair pass prove that partial custody survives restart.
func TestRootFSNodeLegacyInventoryMigrationAndRestartS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	pool := newSandboxStoreIntegrationPoolAt(t, 112)
	store, filesystem, parent := newRetentionS3FilesystemWithPool(t, objects, prefix, "node-legacy-migration", pool)
	base, err := rootfsblock.DecodeDescriptor(parent.Descriptor)
	require.NoError(t, err)
	var updates []rootfsblock.BlockUpdate
	for i := 0; i < 32; i++ {
		data := make([]byte, rootfsblock.LogicalBlockSize)
		_, err = rand.Read(data)
		require.NoError(t, err)
		updates = append(updates, rootfsblock.BlockUpdate{Sequence: uint64(i + 1), Block: uint64(i), Data: data})
	}
	built, err := rootfsblock.BuildIncrementalGeneration(t.Context(), objects, base, updates, rootfsblock.ObjectStorePublisher{Store: objects}, rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.LogicalBlockSize, PackBytes: 8192})
	require.NoError(t, err)
	require.Greater(t, len(built.References), 4)
	_, err = pool.Exec(t.Context(), `INSERT INTO manager.rootfs_generations(generation_id,filesystem_id,parent_generation_id,source_oci_digest,base_artifact_digest,base_block_root,current_block_head,writer_epoch,format_generation,durability_state,locator_version,descriptor)
 SELECT 'legacy-node-cut',filesystem_id,generation_id,source_oci_digest,base_artifact_digest,base_block_root,$2,writer_epoch+1,format_generation,'s3_materialized',locator_version+1,$3 FROM manager.rootfs_generations WHERE generation_id=$1`, parent.ID, built.Descriptor.MappingRoot.RootDigest, built.Payload)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_filesystems SET head_generation_id='legacy-node-cut' WHERE filesystem_id=$1`, filesystem.ID)
	require.NoError(t, err)
	require.NoError(t, RunSandboxStoreMigrations(t.Context(), pool, noopSandboxStoreMigrateLogger{}))
	var required bool
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT storage_inventory_required FROM manager.rootfs_generations WHERE generation_id='legacy-node-cut'`).Scan(&required))
	require.True(t, required)
	complete, err := store.InventoryRootFSGeneration(t.Context(), objects, 100)
	require.NoError(t, err)
	require.False(t, complete)
	var held int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_inventory_object_custody`).Scan(&held))
	require.Greater(t, held, 0)
	require.LessOrEqual(t, held, 4)
	// A worker restart and GC between repair pages cannot abandon verified
	// objects or collect the not-yet-authenticated legacy generation.
	restarted := NewPGSandboxStore(pool)
	for range 30 {
		_, err = restarted.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 1, DeletePendingRootFSObjectsOptions{})
		require.NoError(t, err)
		complete, err = restarted.InventoryRootFSGeneration(t.Context(), objects, 100)
		require.NoError(t, err)
		if complete {
			break
		}
	}
	require.True(t, complete)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_inventory_object_custody`).Scan(&held))
	require.Zero(t, held)
	reader, err := rootfsblock.NewReader(objects, built.Descriptor, 0)
	require.NoError(t, err)
	for _, index := range []int{0, 15, 31} {
		actual := make([]byte, rootfsblock.LogicalBlockSize)
		_, err = reader.ReadAt(actual, int64(index*rootfsblock.LogicalBlockSize))
		require.NoError(t, err)
		require.Equal(t, updates[index].Data, actual)
	}
	require.NoError(t, restarted.MarkSandboxDeleted(t.Context(), filesystem.ID, time.Now()))
	_, err = restarted.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	usage, err := restarted.ListRootFSStorageUsage(t.Context(), "team-1")
	require.NoError(t, err)
	require.Len(t, usage, 1)
	require.Zero(t, usage[0].StorageBytes)
	physical := retentionS3Usage(t, objects, prefix)
	require.Equal(t, int64(1), physical.objects, "only the platform base mapping remains")
	t.Logf("legacy migration authenticated %d direct-upload objects; partial custody %d -> 0; tenant bytes after last owner deletion 0", len(built.References), 4)
}

func TestRootFSNodeLegacyZeroCutS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, filesystem, parent := newRetentionS3Filesystem(t, objects, prefix, "legacy-node-zero-cut")
	// A zero-valued cut can have the same mapping digest as the base while a
	// raw node builder stores it under a different content-addressed prefix.
	publishRetentionNodeCheckpoint(t, store, objects, prefix+"/node", parent, filesystem.ID, "legacy-zero-cut", 0, false)
	require.NoError(t, store.MarkSandboxDeleted(t.Context(), filesystem.ID, time.Now()))
	for range 10 {
		_, err := store.InventoryRootFSGeneration(t.Context(), objects, 100)
		require.NoError(t, err)
		_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
		require.NoError(t, err)
	}
	physical := retentionS3Usage(t, objects, prefix)
	require.Equal(t, int64(1), physical.objects, "only the independently retained base object may remain")
	var filesystems int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_filesystems`).Scan(&filesystems))
	require.Zero(t, filesystems)
}

func TestRootFSRebaseNodeUploadRetentionS3Integration(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(fmt.Sprintf("rejected=%t", rejected), func(t *testing.T) {
			objects, prefix := generationRetentionS3Store(t)
			store, filesystem, initial := newRetentionS3Filesystem(t, objects, prefix, "rebase-upload")
			source := publishRetentionNodeCheckpoint(t, store, objects, prefix, initial, filesystem.ID, "rebase-source", 9, true)
			record, err := store.GetSandbox(t.Context(), filesystem.ID)
			require.NoError(t, err)
			record.ClusterID = "cluster-rebase"
			require.NoError(t, store.UpsertSandbox(t.Context(), record))
			_, err = store.pool.Exec(t.Context(), `INSERT INTO manager.sandbox_runtime_claims(sandbox_id,operation_id,phase,lease_expires_at) VALUES($1,'rebase-claim','ready',NULL)`, filesystem.ID)
			require.NoError(t, err)
			targetRequest := readyRootFSBaseArtifactTestRequest()
			targetRequest.ArtifactDigest = digest.FromString("actual-rebase-target").String()
			targetRequest.SourceOCIDigest = digest.FromString("actual-rebase-target-oci").String()
			targetRequest.SourceOCIRef = "registry.example/sandbox@" + targetRequest.SourceOCIDigest
			targetRequest.BaseBlockRoot = initial.BaseBlockRoot
			targetRequest.Descriptor = initial.Descriptor
			target, err := store.PutReadyRootFSBaseArtifact(t.Context(), targetRequest)
			require.NoError(t, err)
			_, err = store.pool.Exec(t.Context(), `INSERT INTO manager.rootfs_base_artifact_objects(artifact_digest,object_key) SELECT $1,object_key FROM manager.rootfs_base_artifact_objects WHERE artifact_digest=$2`, target.ArtifactDigest, initial.BaseArtifactDigest)
			require.NoError(t, err)
			request := &NomadPausedRebaseRequest{OperationID: "actual-rebase-upload", SandboxID: filesystem.ID, ExpectedTeamID: "team-1", TargetBaseArtifactDigest: target.ArtifactDigest, RollbackExpiresAt: time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond), WorkerClusterID: "cluster-rebase", WorkerNodeID: "rebase-node", WorkerNodeUID: "rebase-node-uid"}
			candidate, err := store.RequestNomadPausedRebase(t.Context(), request)
			require.NoError(t, err)
			upload := RootFSNodeUploadRequest{GrantID: rootfsrebase.UploadOwnerID(request.OperationID), NodeUID: request.WorkerNodeUID, OperationID: request.OperationID, RebaseOperationID: request.OperationID}
			proof := sha256.Sum256([]byte("actual-node-rebase-cleanup-proof"))
			if rejected {
				before := retentionS3Usage(t, objects, prefix)
				payload := []byte("lost rebase upload acknowledgement")
				upload.Reference = rootfsblock.ObjectReference{Key: prefix + "/packs/sha256/" + digest.FromBytes(payload).Encoded(), Kind: rootfsblock.ObjectKindDataPack, Size: int64(len(payload)), Checksum: digest.FromBytes(payload).String()}
				forged := upload
				forged.NodeUID = "another-node"
				require.ErrorIs(t, store.RecordRootFSNodeUpload(t.Context(), &forged), ErrRootFSWriterGrantConflict)
				require.NoError(t, store.RecordRootFSNodeUpload(t.Context(), &upload))
				require.NoError(t, (rootfsblock.ObjectStorePublisher{Store: objects}).PutImmutable(t.Context(), upload.Reference.Key, payload))
				_, err = store.RequestSandboxRuntimeClaimCleanup(t.Context(), filesystem.ID, "delete interrupted rebase")
				require.NoError(t, err)
				require.NoError(t, store.RejectNomadPausedRebaseWorker(t.Context(), request, proof[:]))
				require.NoError(t, store.ReconcileRootFSNodeUploadGarbage(t.Context(), "", 100))
				var held int
				require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_node_upload_objects WHERE grant_id=$1`, upload.GrantID).Scan(&held))
				require.Equal(t, 1, held, "rejection alone is not physical node acknowledgement")
				require.NoError(t, store.AcknowledgeNomadPausedRebaseWorker(t.Context(), request.OperationID, filesystem.ID, request.WorkerClusterID, request.WorkerNodeID, request.WorkerNodeUID, proof[:]))
				_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_node_uploads SET terminal_at=clock_timestamp()-INTERVAL '3 minutes' WHERE grant_id=$1`, upload.GrantID)
				require.NoError(t, err)
				require.NoError(t, NewPGSandboxStore(store.pool).ReconcileRootFSNodeUploadGarbage(t.Context(), "", 1))
				_, err = store.DeletePendingRootFSObjectsWithOptions(t.Context(), objects, DeletePendingRootFSObjectsOptions{Limit: 100})
				require.NoError(t, err)
				require.Equal(t, before, retentionS3Usage(t, objects, prefix))
				assertRetentionContents(t, objects, source, 9)
				return
			}
			base, err := rootfsblock.DecodeDescriptor(target.Descriptor)
			require.NoError(t, err)
			built, err := rootfsblock.BuildIncrementalGeneration(t.Context(), objects, base, []rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{5}, rootfsblock.LogicalBlockSize)}}, retentionNodePublisher{store: store, objects: objects, request: upload}, rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.LogicalBlockSize})
			require.NoError(t, err)
			generation := &RootFSGeneration{ID: candidate.TargetGenerationID, FilesystemID: filesystem.ID, ParentGenerationID: source.ID, SourceOCIDigest: target.SourceOCIDigest, BaseArtifactDigest: target.ArtifactDigest, BaseBlockRoot: target.BaseBlockRoot, CurrentBlockHead: built.Descriptor.MappingRoot.RootDigest, WriterEpoch: candidate.TargetWriterEpoch, FormatGeneration: target.FormatGeneration, DurabilityState: RootFSGenerationStateS3Materialized, LocatorVersion: source.LocatorVersion + 1, Descriptor: built.Payload}
			publication := &PublishPausedRootFSRebaseRequest{SandboxID: filesystem.ID, TeamID: "team-1", OperationID: request.OperationID, ExpectedSourceGenerationID: source.ID, ExpectedBaseArtifactDigest: source.BaseArtifactDigest, Generation: generation, HealthCheckDigest: proof[:], RollbackExpiresAt: request.RollbackExpiresAt, WorkerClusterID: request.WorkerClusterID, WorkerNodeID: request.WorkerNodeID, WorkerNodeUID: request.WorkerNodeUID, WorkerProofDigest: proof[:]}
			_, err = store.PublishPausedRootFSRebase(t.Context(), publication)
			require.NoError(t, err)
			_, err = store.PublishPausedRootFSRebase(t.Context(), publication)
			require.NoError(t, err, "exact publication retry")
			require.NoError(t, store.AcknowledgeNomadPausedRebaseWorker(t.Context(), request.OperationID, filesystem.ID, request.WorkerClusterID, request.WorkerNodeID, request.WorkerNodeUID, proof[:]))
			for range 5 {
				_, err = store.InventoryRootFSGeneration(t.Context(), objects, 100)
				require.NoError(t, err)
			}
			_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
			require.NoError(t, err)
			assertRetentionContents(t, objects, generation, 5)
			assertRetentionContents(t, objects, source, 9)
			rolled, err := store.RollbackRootFSHead(t.Context(), &RollbackRootFSHeadRequest{SandboxID: filesystem.ID, OperationID: request.OperationID, TeamID: "team-1"})
			require.NoError(t, err)
			require.Equal(t, source.ID, rolled.HeadGenerationID)
			_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
			require.NoError(t, err)
			assertRetentionContents(t, objects, source, 9)
		})
	}
}

func TestRootFSNodeBaseObjectDoesNotBecomeTenantCapacityS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, filesystem, parent := newRetentionS3Filesystem(t, objects, prefix, "node-base-object")
	current := publishRetentionNodeCheckpoint(t, store, objects, prefix, parent, filesystem.ID, "node-base-cut", 0, true)
	usage, err := store.ListRootFSStorageUsage(t.Context(), "team-1")
	require.NoError(t, err)
	var total int64
	for _, account := range usage {
		total += account.StorageBytes
	}
	require.Zero(t, total, "reusing the platform base mapping must not create tenant capacity")
	_, err = store.InventoryRootFSGeneration(t.Context(), objects, 100)
	require.NoError(t, err)
	assertRetentionContents(t, objects, current, 0)
}
