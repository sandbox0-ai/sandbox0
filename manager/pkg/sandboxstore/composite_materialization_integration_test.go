package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestRootFSCompositeBacklogUsageAndMaterializationIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	filesystem, initial := seedCompositeTestFilesystem(t, ctx, store, "materialize")
	composite := compositeTestGeneration(t, initial, filesystem.ID, "generation-composite", 1)
	require.NoError(t, insertCompositeTestGeneration(ctx, pool, composite))
	require.NoError(t, store.SetRootFSCompositeBacklogLimit(ctx, int64(2*len(composite.Descriptor))))

	usage, err := store.GetRootFSCompositeBacklogUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len(composite.Descriptor)), usage.UsedDescriptorBytes)
	require.Equal(t, int64(1), usage.GenerationCount)
	require.Equal(t, int64(2*len(composite.Descriptor)), usage.MaxDescriptorBytes)
	candidates, err := store.ListCompositeRootFSGenerations(ctx, 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, composite.ID, candidates[0].ID)

	request := beginRootFSMaterializationIntegrationBatch(t, ctx, store, composite, initial.Descriptor)
	require.NoError(t, store.PublishRootFSGenerationMaterializationBatch(ctx, request))
	require.NoError(t, store.PublishRootFSGenerationMaterializationBatch(ctx, request), "exact response-loss retry")
	materialized, err := store.GetRootFSGeneration(ctx, composite.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSGenerationStateS3Materialized, materialized.DurabilityState)
	require.Equal(t, composite.LocatorVersion+1, materialized.LocatorVersion)
	require.Equal(t, initial.Descriptor, materialized.Descriptor)
	usage, err = store.GetRootFSCompositeBacklogUsage(ctx)
	require.NoError(t, err)
	require.Zero(t, usage.UsedDescriptorBytes)
	require.Zero(t, usage.GenerationCount)

	changed := *request
	changed.Members = append([]RootFSGenerationMaterializationPublication(nil), request.Members...)
	changed.Members[0].MaterializedDescriptor = append([]byte(nil), initial.Descriptor...)
	changed.Members[0].MaterializedDescriptor[len(changed.Members[0].MaterializedDescriptor)-1] ^= 1
	require.Error(t, store.PublishRootFSGenerationMaterializationBatch(ctx, &changed))
	require.Error(t, store.SetRootFSCompositeBacklogLimit(ctx, 0))
	_, err = store.ListCompositeRootFSGenerations(ctx, 0)
	require.Error(t, err)
}

func TestRootFSMaterializationBatchPublishesAtomicallyAndRetriesExactlyIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	filesystemA, initialA := seedCompositeTestFilesystem(t, ctx, store, "batch-atomic-a")
	filesystemB, initialB := seedCompositeTestFilesystem(t, ctx, store, "batch-atomic-b")
	first := compositeTestGeneration(t, initialA, filesystemA.ID, "generation-batch-atomic-a", 1)
	second := compositeTestGeneration(t, initialB, filesystemB.ID, "generation-batch-atomic-b", 1)
	require.NoError(t, insertCompositeTestGeneration(ctx, pool, first))
	require.NoError(t, insertCompositeTestGeneration(ctx, pool, second))

	identities := []RootFSGenerationMaterializationIdentity{
		{GenerationID: first.ID, ExpectedLocatorVersion: first.LocatorVersion, ExpectedDescriptor: first.Descriptor},
		{GenerationID: second.ID, ExpectedLocatorVersion: second.LocatorVersion, ExpectedDescriptor: second.Descriptor},
	}
	lane := RootFSMaterializationPackLane("team-1", first.FormatGeneration)
	batchID, err := RootFSMaterializationBatchID(lane, identities)
	require.NoError(t, err)
	begin := &BeginRootFSGenerationMaterializationBatchRequest{
		BatchID: batchID, PackLane: lane, TeamID: "team-1",
		FormatGeneration: first.FormatGeneration, Members: identities,
	}
	batch, err := store.BeginRootFSGenerationMaterializationBatch(ctx, begin)
	require.NoError(t, err)
	require.Equal(t, "uploading", batch.State)
	batch, err = store.BeginRootFSGenerationMaterializationBatch(ctx, begin)
	require.NoError(t, err, "exact begin retry")
	require.Equal(t, identities, batch.Members)
	pending, err := store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, batchID, pending.BatchID)
	require.Equal(t, identities, pending.Members)

	descriptor, reference := rootFSMaterializationTestDescriptor(t, initialA.Descriptor, "rootfs/test/shared-map")
	require.NoError(t, store.RegisterRootFSGenerationMaterializationBatchObject(ctx, batchID, reference))
	require.NoError(t, store.MarkRootFSGenerationMaterializationBatchObjectUploaded(ctx, batchID, reference.Key))
	publication := &PublishRootFSGenerationMaterializationBatchRequest{
		BatchID: batchID,
		Members: []RootFSGenerationMaterializationPublication{
			{GenerationID: first.ID, ExpectedLocatorVersion: first.LocatorVersion,
				ExpectedDescriptor: first.Descriptor, MaterializedDescriptor: descriptor,
				References: []rootfsblock.ObjectReference{reference}},
			{GenerationID: second.ID, ExpectedLocatorVersion: second.LocatorVersion,
				ExpectedDescriptor: second.Descriptor, MaterializedDescriptor: descriptor,
				References: []rootfsblock.ObjectReference{reference}},
		},
	}
	changedGeometry := append([]byte(nil), descriptor...)
	changedDecoded, err := rootfsblock.DecodeDescriptor(changedGeometry)
	require.NoError(t, err)
	changedDecoded.LogicalSizeBytes += rootfsblock.LogicalBlockSize
	changedGeometry, err = rootfsblock.EncodeDescriptor(changedDecoded)
	require.NoError(t, err)
	invalid := *publication
	invalid.Members = append([]RootFSGenerationMaterializationPublication(nil), publication.Members...)
	invalid.Members[1].MaterializedDescriptor = changedGeometry
	require.Error(t, store.PublishRootFSGenerationMaterializationBatch(ctx, &invalid))
	storedFirst, err := store.GetRootFSGeneration(ctx, first.ID)
	require.NoError(t, err)
	storedSecond, err := store.GetRootFSGeneration(ctx, second.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSGenerationStateCompositeDurable, storedFirst.DurabilityState,
		"the first staged locator must roll back when a later member fails")
	require.Equal(t, RootFSGenerationStateCompositeDurable, storedSecond.DurabilityState)

	require.NoError(t, store.PublishRootFSGenerationMaterializationBatch(ctx, publication))
	require.NoError(t, store.PublishRootFSGenerationMaterializationBatch(ctx, publication), "exact commit-response-loss retry")
	storedFirst, err = store.GetRootFSGeneration(ctx, first.ID)
	require.NoError(t, err)
	storedSecond, err = store.GetRootFSGeneration(ctx, second.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSGenerationStateS3Materialized, storedFirst.DurabilityState)
	require.Equal(t, RootFSGenerationStateS3Materialized, storedSecond.DurabilityState)
	require.Equal(t, first.LocatorVersion+1, storedFirst.LocatorVersion)
	require.Equal(t, second.LocatorVersion+1, storedSecond.LocatorVersion)

	changedRetry := *publication
	changedRetry.Members = append([]RootFSGenerationMaterializationPublication(nil), publication.Members...)
	changedReference := reference
	changedReference.Size++
	changedRetry.Members[0].References = []rootfsblock.ObjectReference{changedReference}
	require.Error(t, store.PublishRootFSGenerationMaterializationBatch(ctx, &changedRetry))
	require.NoError(t, store.RegisterRootFSGenerationMaterializationBatchObject(ctx, batchID, reference),
		"a concurrent exact publisher may finish object registration after commit")
	require.NoError(t, store.MarkRootFSGenerationMaterializationBatchObjectUploaded(ctx, batchID, reference.Key))

	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_materialization_batches
		SET updated_at = NOW() - INTERVAL '2 hours'
		WHERE batch_id = $1
	`, batchID)
	require.NoError(t, err)
	garbage, err := store.ReconcileRootFSGenerationMaterializationGarbage(ctx, time.Minute, time.Minute, 100)
	require.NoError(t, err)
	require.Equal(t, &RootFSGenerationMaterializationGarbageResult{PurgedBatches: 1}, garbage)
	require.Equal(t, int64(0), rootFSMaterializationTestCount(t, pool, "rootfs_materialization_batches"))
	require.Equal(t, int64(2), rootFSMaterializationTestCount(t, pool, "rootfs_generation_materialization_objects"))
	require.Equal(t, int64(0), rootFSTestCountRows(t, pool, "rootfs_object_deletions"),
		"a published shared object remains live through both locator roots")
}

func TestRootFSMaterializationBatchConflictAndOrphanGarbageIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	filesystemA, initialA := seedCompositeTestFilesystem(t, ctx, store, "batch-gc-a")
	filesystemB, initialB := seedCompositeTestFilesystem(t, ctx, store, "batch-gc-b")
	first := compositeTestGeneration(t, initialA, filesystemA.ID, "generation-batch-gc-a", 1)
	second := compositeTestGeneration(t, initialB, filesystemB.ID, "generation-batch-gc-b", 1)
	require.NoError(t, insertCompositeTestGeneration(ctx, pool, first))
	require.NoError(t, insertCompositeTestGeneration(ctx, pool, second))
	lane := RootFSMaterializationPackLane("team-1", first.FormatGeneration)
	firstIdentity := RootFSGenerationMaterializationIdentity{
		GenerationID: first.ID, ExpectedLocatorVersion: first.LocatorVersion, ExpectedDescriptor: first.Descriptor,
	}
	firstBatchID, err := RootFSMaterializationBatchID(lane, []RootFSGenerationMaterializationIdentity{firstIdentity})
	require.NoError(t, err)
	_, err = store.BeginRootFSGenerationMaterializationBatch(ctx, &BeginRootFSGenerationMaterializationBatchRequest{
		BatchID: firstBatchID, PackLane: lane, TeamID: "team-1",
		FormatGeneration: first.FormatGeneration,
		Members:          []RootFSGenerationMaterializationIdentity{firstIdentity},
	})
	require.NoError(t, err)

	conflictingMembers := []RootFSGenerationMaterializationIdentity{
		firstIdentity,
		{GenerationID: second.ID, ExpectedLocatorVersion: second.LocatorVersion, ExpectedDescriptor: second.Descriptor},
	}
	conflictingID, err := RootFSMaterializationBatchID(lane, conflictingMembers)
	require.NoError(t, err)
	_, err = store.BeginRootFSGenerationMaterializationBatch(ctx, &BeginRootFSGenerationMaterializationBatchRequest{
		BatchID: conflictingID, PackLane: lane, TeamID: "team-1",
		FormatGeneration: first.FormatGeneration, Members: conflictingMembers,
	})
	require.ErrorIs(t, err, ErrRootFSGenerationConflict)
	require.Equal(t, int64(1), rootFSMaterializationTestCount(t, pool, "rootfs_materialization_batches"),
		"the losing batch transaction must not leave an empty journal")

	_, orphanReference := rootFSMaterializationTestDescriptor(t, initialA.Descriptor, "rootfs/test/orphan-map")
	require.NoError(t, store.RegisterRootFSGenerationMaterializationBatchObject(ctx, firstBatchID, orphanReference))
	require.NoError(t, store.MarkRootFSGenerationMaterializationBatchObjectUploaded(ctx, firstBatchID, orphanReference.Key))
	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_materialization_batches
		SET updated_at = NOW() - INTERVAL '2 hours'
		WHERE batch_id = $1
	`, firstBatchID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_generations
		SET locator_version = locator_version + 1, durability_state = $2
		WHERE generation_id = $1
	`, first.ID, RootFSGenerationStateS3Materialized)
	require.NoError(t, err)
	garbage, err := store.ReconcileRootFSGenerationMaterializationGarbage(ctx, time.Minute, time.Minute, 100)
	require.NoError(t, err)
	require.Equal(t, &RootFSGenerationMaterializationGarbageResult{
		AbandonedBatches: 1, PurgedBatches: 1, EnqueuedObjects: 1,
	}, garbage)
	require.Equal(t, int64(0), rootFSMaterializationTestCount(t, pool, "rootfs_materialization_batches"))
	require.Equal(t, int64(0), rootFSMaterializationTestCount(t, pool, "rootfs_materialization_objects"))
	require.Equal(t, int64(1), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))

	secondIdentity := RootFSGenerationMaterializationIdentity{
		GenerationID: second.ID, ExpectedLocatorVersion: second.LocatorVersion, ExpectedDescriptor: second.Descriptor,
	}
	secondBatchID, err := RootFSMaterializationBatchID(lane, []RootFSGenerationMaterializationIdentity{secondIdentity})
	require.NoError(t, err)
	_, err = store.BeginRootFSGenerationMaterializationBatch(ctx, &BeginRootFSGenerationMaterializationBatchRequest{
		BatchID: secondBatchID, PackLane: lane, TeamID: "team-1",
		FormatGeneration: second.FormatGeneration,
		Members:          []RootFSGenerationMaterializationIdentity{secondIdentity},
	})
	require.NoError(t, err)
	require.ErrorIs(t,
		store.RegisterRootFSGenerationMaterializationBatchObject(ctx, secondBatchID, orphanReference),
		ErrRootFSGenerationConflict,
		"a queued deletion must fence content-addressed resurrection",
	)
	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_materialization_batches
		SET updated_at = NOW() - INTERVAL '2 hours'
		WHERE batch_id = $1
	`, secondBatchID)
	require.NoError(t, err)
	garbage, err = store.ReconcileRootFSGenerationMaterializationGarbage(ctx, time.Minute, time.Minute, 100)
	require.NoError(t, err)
	require.Zero(t, garbage.AbandonedBatches,
		"a stale but still-current upload batch remains a strong crash-resume root")
	pending, err := store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, secondBatchID, pending.BatchID)
}

func TestSandboxDeletionWaitsForUploadingMaterializationAndReclaimsAfterRetentionIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	filesystem, initial := seedCompositeTestFilesystem(t, ctx, store, "batch-delete")
	composite := compositeTestGeneration(t, initial, filesystem.ID, "generation-batch-delete", 1)
	require.NoError(t, insertCompositeTestGeneration(ctx, pool, composite))
	identity := RootFSGenerationMaterializationIdentity{
		GenerationID: composite.ID, ExpectedLocatorVersion: composite.LocatorVersion,
		ExpectedDescriptor: composite.Descriptor,
	}
	lane := RootFSMaterializationPackLane("team-1", composite.FormatGeneration)
	batchID, err := RootFSMaterializationBatchID(lane, []RootFSGenerationMaterializationIdentity{identity})
	require.NoError(t, err)
	_, err = store.BeginRootFSGenerationMaterializationBatch(ctx, &BeginRootFSGenerationMaterializationBatchRequest{
		BatchID: batchID, PackLane: lane, TeamID: "team-1",
		FormatGeneration: composite.FormatGeneration,
		Members:          []RootFSGenerationMaterializationIdentity{identity},
	})
	require.NoError(t, err)
	err = store.MarkSandboxDeleted(ctx, "sandbox-batch-delete", time.Now().UTC())
	require.ErrorIs(t, err, ErrSandboxClaimCleanupPending)
	record, err := store.GetSandbox(ctx, "sandbox-batch-delete")
	require.NoError(t, err)
	require.True(t, record.DeletedAt.IsZero())

	descriptor, reference := rootFSMaterializationTestDescriptor(
		t, initial.Descriptor, "rootfs/test/delete-retained-map",
	)
	require.NoError(t, store.RegisterRootFSGenerationMaterializationBatchObject(ctx, batchID, reference))
	require.NoError(t, store.MarkRootFSGenerationMaterializationBatchObjectUploaded(ctx, batchID, reference.Key))
	require.NoError(t, store.PublishRootFSGenerationMaterializationBatch(ctx,
		&PublishRootFSGenerationMaterializationBatchRequest{
			BatchID: batchID,
			Members: []RootFSGenerationMaterializationPublication{{
				GenerationID: composite.ID, ExpectedLocatorVersion: composite.LocatorVersion,
				ExpectedDescriptor: composite.Descriptor, MaterializedDescriptor: descriptor,
				References: []rootfsblock.ObjectReference{reference},
			}},
		}))
	require.NoError(t, store.MarkSandboxDeleted(ctx, "sandbox-batch-delete", time.Now().UTC()))
	_, err = store.GetRootFSGeneration(ctx, composite.ID)
	require.ErrorIs(t, err, ErrRootFSFilesystemNotFound)
	require.Equal(t, int64(0), rootFSTestCountRows(t, pool, "rootfs_object_deletions"),
		"the terminal journal retains the object through the deletion race window")

	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_materialization_batches
		SET updated_at = NOW() - INTERVAL '2 hours'
		WHERE batch_id = $1
	`, batchID)
	require.NoError(t, err)
	garbage, err := store.ReconcileRootFSGenerationMaterializationGarbage(ctx, time.Minute, time.Minute, 100)
	require.NoError(t, err)
	require.Equal(t, &RootFSGenerationMaterializationGarbageResult{
		PurgedBatches: 1, EnqueuedObjects: 1,
	}, garbage)
	require.Equal(t, int64(1), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))
}

func rootFSMaterializationTestDescriptor(
	t *testing.T,
	basePayload []byte,
	key string,
) ([]byte, rootfsblock.ObjectReference) {
	t.Helper()
	base, err := rootfsblock.DecodeDescriptor(basePayload)
	require.NoError(t, err)
	page, err := rootfsblock.EncodeMappingPage(rootfsblock.MappingPage{
		StartBlock: 0,
		BlockCount: uint64(base.LogicalSizeBytes / rootfsblock.LogicalBlockSize),
	})
	require.NoError(t, err)
	checksum := digest.FromBytes(page).String()
	base.CompositeTail = nil
	base.MappingRoot = rootfsblock.MappingRootLocator{
		Version: rootfsblock.MappingPageVersion, RootDigest: checksum,
		Object: rootfsblock.ObjectRange{Key: key, Length: int64(len(page)), Checksum: checksum},
	}
	payload, err := rootfsblock.EncodeDescriptor(base)
	require.NoError(t, err)
	return payload, rootfsblock.ObjectReference{
		Key: key, Kind: rootfsblock.ObjectKindMappingPage,
		Size: int64(len(page)), Checksum: checksum,
	}
}

func rootFSMaterializationTestCount(t *testing.T, pool *pgxpool.Pool, table string) int64 {
	t.Helper()
	allowed := map[string]struct{}{
		"rootfs_materialization_batches":            {},
		"rootfs_materialization_objects":            {},
		"rootfs_generation_materialization_objects": {},
	}
	_, ok := allowed[table]
	require.True(t, ok, "unexpected materialization test table %s", table)
	var count int64
	require.NoError(t, pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM manager."+table).Scan(&count))
	return count
}

func beginRootFSMaterializationIntegrationBatch(
	t *testing.T,
	ctx context.Context,
	store *PGSandboxStore,
	generation *RootFSGeneration,
	materializedDescriptor []byte,
) *PublishRootFSGenerationMaterializationBatchRequest {
	t.Helper()
	filesystem, err := store.GetRootFSFilesystem(ctx, generation.FilesystemID)
	require.NoError(t, err)
	identity := RootFSGenerationMaterializationIdentity{
		GenerationID: generation.ID, ExpectedLocatorVersion: generation.LocatorVersion,
		ExpectedDescriptor: generation.Descriptor,
	}
	lane := RootFSMaterializationPackLane(filesystem.TeamID, generation.FormatGeneration)
	batchID, err := RootFSMaterializationBatchID(lane, []RootFSGenerationMaterializationIdentity{identity})
	require.NoError(t, err)
	_, err = store.BeginRootFSGenerationMaterializationBatch(ctx,
		&BeginRootFSGenerationMaterializationBatchRequest{
			BatchID: batchID, PackLane: lane, TeamID: filesystem.TeamID,
			FormatGeneration: generation.FormatGeneration,
			Members:          []RootFSGenerationMaterializationIdentity{identity},
		})
	require.NoError(t, err)
	descriptor, err := rootfsblock.DecodeDescriptor(materializedDescriptor)
	require.NoError(t, err)
	require.Zero(t, descriptor.MappingRoot.Object.Offset)
	reference := rootfsblock.ObjectReference{
		Key: descriptor.MappingRoot.Object.Key, Kind: rootfsblock.ObjectKindMappingPage,
		Size: descriptor.MappingRoot.Object.Length, Checksum: descriptor.MappingRoot.Object.Checksum,
	}
	require.NoError(t, store.RegisterRootFSGenerationMaterializationBatchObject(ctx, batchID, reference))
	require.NoError(t, store.MarkRootFSGenerationMaterializationBatchObjectUploaded(
		ctx, batchID, reference.Key,
	))
	return &PublishRootFSGenerationMaterializationBatchRequest{
		BatchID: batchID,
		Members: []RootFSGenerationMaterializationPublication{{
			GenerationID: generation.ID, ExpectedLocatorVersion: generation.LocatorVersion,
			ExpectedDescriptor: generation.Descriptor, MaterializedDescriptor: materializedDescriptor,
			References: []rootfsblock.ObjectReference{reference},
		}},
	}
}

func TestRootFSCompositeBacklogCapacitySerializesPublishersIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	filesystemA, initialA := seedCompositeTestFilesystem(t, ctx, store, "capacity-a")
	filesystemB, initialB := seedCompositeTestFilesystem(t, ctx, store, "capacity-b")
	generationA := compositeTestGeneration(t, initialA, filesystemA.ID, "generation-capacity-a", 1)
	generationB := compositeTestGeneration(t, initialB, filesystemB.ID, "generation-capacity-b", 1)
	require.Equal(t, len(generationA.Descriptor), len(generationB.Descriptor))
	require.NoError(t, store.SetRootFSCompositeBacklogLimit(ctx, int64(len(generationA.Descriptor))))

	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, generation := range []*RootFSGeneration{generationA, generationB} {
		generation := generation
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			tx, err := pool.Begin(ctx)
			if err != nil {
				results <- err
				return
			}
			defer tx.Rollback(ctx)
			if err = ensureRootFSCompositeBacklogCapacity(ctx, tx, generation); err == nil {
				err = insertCompositeTestGeneration(ctx, tx, generation)
			}
			if err == nil {
				err = tx.Commit(ctx)
			}
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	var succeeded, exhausted int
	for err := range results {
		if err == nil {
			succeeded++
		} else if errors.Is(err, ErrRootFSCompositeBacklogExhausted) {
			exhausted++
		} else {
			t.Fatalf("unexpected publisher error: %v", err)
		}
	}
	require.Equal(t, 1, succeeded)
	require.Equal(t, 1, exhausted)
	usage, err := store.GetRootFSCompositeBacklogUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(len(generationA.Descriptor)), usage.UsedDescriptorBytes)
	require.Equal(t, int64(1), usage.GenerationCount)
}

func seedCompositeTestFilesystem(
	t *testing.T,
	ctx context.Context,
	store *PGSandboxStore,
	name string,
) (*RootFSFilesystem, *RootFSGeneration) {
	t.Helper()
	sandboxID := "sandbox-" + name
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord(sandboxID, "team-1")))
	artifactRequest := readyRootFSBaseArtifactTestRequest()
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, artifactRequest)
	require.NoError(t, err)
	filesystem, generation, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: sandboxID, TeamID: "team-1", SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	return filesystem, generation
}

func compositeTestGeneration(
	t *testing.T,
	initial *RootFSGeneration,
	filesystemID, generationID string,
	epoch int64,
) *RootFSGeneration {
	t.Helper()
	descriptor, err := rootfsblock.DecodeDescriptor(initial.Descriptor)
	require.NoError(t, err)
	sealed, payload, err := rootfsblock.BuildCompositeGeneration(descriptor, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{byte(epoch)}, rootfsblock.LogicalBlockSize),
	}})
	require.NoError(t, err)
	return &RootFSGeneration{
		ID: generationID, FilesystemID: filesystemID, ParentGenerationID: initial.ID,
		SourceOCIDigest: initial.SourceOCIDigest, BaseArtifactDigest: initial.BaseArtifactDigest,
		BaseBlockRoot: initial.BaseBlockRoot, CurrentBlockHead: sealed.MappingRoot.RootDigest,
		WriterEpoch: epoch, FormatGeneration: initial.FormatGeneration,
		DurabilityState: RootFSGenerationStateCompositeDurable,
		LocatorVersion:  initial.LocatorVersion + 1, Descriptor: payload,
	}
}

func insertCompositeTestGeneration(
	ctx context.Context,
	db interface {
		Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	},
	generation *RootFSGeneration,
) error {
	_, err := db.Exec(ctx, `
		INSERT INTO manager.rootfs_generations (
			generation_id, filesystem_id, parent_generation_id, source_oci_digest,
			base_artifact_digest, base_block_root, current_block_head, writer_epoch,
			format_generation, durability_state, locator_version, descriptor, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
	`, generation.ID, generation.FilesystemID, generation.ParentGenerationID,
		generation.SourceOCIDigest, generation.BaseArtifactDigest, generation.BaseBlockRoot,
		generation.CurrentBlockHead, generation.WriterEpoch, generation.FormatGeneration,
		generation.DurabilityState, generation.LocatorVersion, generation.Descriptor)
	if err != nil {
		return fmt.Errorf("insert composite test generation: %w", err)
	}
	return nil
}
