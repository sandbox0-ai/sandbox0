package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestInitialRootFSGenerationPersistenceIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-generation", "team-1")))

	artifactRequest := readyRootFSBaseArtifactTestRequest()
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, artifactRequest)
	require.NoError(t, err)
	require.Equal(t, RootFSBaseArtifactStateReady, artifact.State)

	ensureRequest := &EnsureInitialRootFSGenerationRequest{
		SandboxID:          "sandbox-generation",
		TeamID:             "team-1",
		SourceOCIRef:       artifact.SourceOCIRef,
		SourceOCIDigest:    artifact.SourceOCIDigest,
		BaseArtifactDigest: artifact.ArtifactDigest,
	}
	filesystem, generation, err := store.EnsureInitialRootFSGeneration(ctx, ensureRequest)
	require.NoError(t, err)
	require.Equal(t, RootFSStorageFormatBlockCOWV1, filesystem.StorageFormat)
	require.Empty(t, filesystem.HeadLayerID)
	require.Equal(t, generation.ID, filesystem.HeadGenerationID)
	require.Equal(t, artifact.BaseBlockRoot, generation.CurrentBlockHead)
	require.Equal(t, RootFSGenerationStateS3Materialized, generation.DurabilityState)

	retriedFilesystem, retriedGeneration, err := store.EnsureInitialRootFSGeneration(ctx, ensureRequest)
	require.NoError(t, err)
	require.Equal(t, filesystem.ID, retriedFilesystem.ID)
	require.Equal(t, generation.ID, retriedGeneration.ID)

	loaded, err := store.GetRootFSFilesystem(ctx, "sandbox-generation")
	require.NoError(t, err)
	require.Equal(t, generation.ID, loaded.HeadGenerationID)
	require.Equal(t, artifact.ArtifactDigest, loaded.BaseArtifactDigest)

	issue := rootFSWriterGrantTestIssueRequest("sandbox-generation", "grant-generation", "claim-generation", "slot-generation", bytes.Repeat([]byte{0x4a}, 32))
	issue.ExpectedFilesystemID = filesystem.ID
	issue.InitialGenerationID = generation.ID
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	require.Equal(t, generation.ID, issued.Grant.InitialGenerationID)
	require.Equal(t, int64(1), issued.Grant.WriterEpoch)
}

func TestInitialRootFSGenerationRejectsDifferentArtifact(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-conflict", "team-1")))

	first := readyRootFSBaseArtifactTestRequest()
	_, err := store.PutReadyRootFSBaseArtifact(ctx, first)
	require.NoError(t, err)
	_, _, err = store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: "sandbox-conflict", TeamID: "team-1", SourceOCIRef: first.SourceOCIRef,
		SourceOCIDigest: first.SourceOCIDigest, BaseArtifactDigest: first.ArtifactDigest,
	})
	require.NoError(t, err)

	second := readyRootFSBaseArtifactTestRequest()
	second.ArtifactDigest = "sha256:" + strings.Repeat("c", 64)
	second.BaseBlockRoot = digest.FromString("block-root-b").String()
	second.Descriptor, err = rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 1 << 30,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: second.BaseBlockRoot,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/base-b/map.page", Length: 4096,
				Checksum: digest.FromString("base-b-map-page").String(),
			},
		},
	})
	require.NoError(t, err)
	_, err = store.PutReadyRootFSBaseArtifact(ctx, second)
	require.NoError(t, err)
	_, _, err = store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: "sandbox-conflict", TeamID: "team-1", SourceOCIRef: second.SourceOCIRef,
		SourceOCIDigest: second.SourceOCIDigest, BaseArtifactDigest: second.ArtifactDigest,
	})
	require.ErrorIs(t, err, ErrRootFSGenerationConflict)
}

func TestInitialRootFSGenerationConcurrentSandboxesRetrySerialization(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)

	const sandboxes = 12
	for i := range sandboxes {
		require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord(fmt.Sprintf("sandbox-concurrent-%02d", i), "team-1")))
	}
	start := make(chan struct{})
	errorsBySandbox := make([]error, sandboxes)
	var wait sync.WaitGroup
	for i := range sandboxes {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			sandboxID := fmt.Sprintf("sandbox-concurrent-%02d", i)
			filesystem, generation, ensureErr := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
				SandboxID: sandboxID, TeamID: "team-1", SourceOCIRef: artifact.SourceOCIRef,
				SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
			})
			if ensureErr == nil && (filesystem.ID != sandboxID || generation.FilesystemID != sandboxID) {
				ensureErr = fmt.Errorf("generation was bound to the wrong filesystem")
			}
			errorsBySandbox[i] = ensureErr
		}()
	}
	close(start)
	wait.Wait()
	for i, ensureErr := range errorsBySandbox {
		require.NoErrorf(t, ensureErr, "sandbox %d", i)
	}
}

func TestCompleteRootFSWriterRetirePublishesGenerationAndPauseAtomically(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record := rootFSTestSandboxRecord("sandbox-pause", "team-1")
	record.RuntimeGeneration = 7
	record.CurrentPodNamespace = "sandbox0"
	record.CurrentPodName = "sandbox-pause"
	require.NoError(t, store.UpsertSandbox(ctx, record))

	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	filesystem, initial, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: "sandbox-pause", TeamID: "team-1", SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)

	binding := sha256.Sum256([]byte("block-generation-binding"))
	issue := rootFSWriterGrantTestIssueRequest("sandbox-pause", "grant-pause", "claim-pause", "slot-pause", binding[:])
	issue.ExpectedFilesystemID = filesystem.ID
	issue.InitialGenerationID = initial.ID
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ConsumerNodeUID: "node-a",
		ConsumerCtldPodUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: "pause-txn",
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ExpectedOldHeadLayerID: initial.ID,
	})
	require.NoError(t, err)
	require.NoError(t, store.WithSandboxLock(ctx, "sandbox-pause", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: "pause-txn", SandboxID: "sandbox-pause", Kind: SandboxLifecycleKindPause,
			Phase: SandboxLifecyclePhasePublishing, ExpectedHeadLayerID: initial.ID,
		})
	}))

	newBlockHead := digest.FromString("sealed-block-head").String()
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 1 << 30,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: newBlockHead,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/paused/map.page", Length: 4096,
				Checksum: digest.FromString("paused-map-page").String(),
			},
		},
	})
	require.NoError(t, err)
	next := &RootFSGeneration{
		ID: "generation-paused", FilesystemID: filesystem.ID, ParentGenerationID: initial.ID,
		SourceOCIDigest: initial.SourceOCIDigest, BaseArtifactDigest: initial.BaseArtifactDigest,
		BaseBlockRoot: initial.BaseBlockRoot, CurrentBlockHead: newBlockHead,
		WriterEpoch: issued.Grant.WriterEpoch, FormatGeneration: initial.FormatGeneration,
		DurabilityState: RootFSGenerationStateS3Materialized, LocatorVersion: initial.LocatorVersion + 1,
		Descriptor: descriptor,
	}
	proof := sha256.Sum256([]byte("host-detach-seal-proof"))
	publish := &CompleteRootFSWriterRetireAndPublishGenerationRequest{
		LifecycleTxnID: "pause-txn", GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
		OperationID: "pause-txn", BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: binding[:], ProofDigest: proof[:], ExpectedOldGenerationID: initial.ID,
		Generation: next,
	}

	rollback := errors.New("force rollback after generation publish")
	err = store.WithSandboxLock(ctx, "sandbox-pause", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		writerTx, ok := tx.(RootFSWriterGrantTx)
		require.True(t, ok)
		_, publishErr := writerTx.CompleteRootFSWriterRetireAndPublishGeneration(lockCtx, publish)
		if publishErr != nil {
			return publishErr
		}
		if pauseErr := tx.MarkRuntimePaused(lockCtx, "sandbox-pause", 7, time.Now().UTC()); pauseErr != nil {
			return pauseErr
		}
		return rollback
	})
	require.ErrorIs(t, err, rollback)
	assertBlockGenerationPublishState(t, ctx, store, pool, initial.ID, RootFSWriterGrantStateRetiring, SandboxDesiredStateActive, false)

	err = store.WithSandboxLock(ctx, "sandbox-pause", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		writerTx, ok := tx.(RootFSWriterGrantTx)
		require.True(t, ok)
		if _, publishErr := writerTx.CompleteRootFSWriterRetireAndPublishGeneration(lockCtx, publish); publishErr != nil {
			return publishErr
		}
		return tx.MarkRuntimePaused(lockCtx, "sandbox-pause", 7, time.Now().UTC())
	})
	require.NoError(t, err)
	assertBlockGenerationPublishState(t, ctx, store, pool, next.ID, RootFSWriterGrantStateRetired, SandboxDesiredStatePaused, true)
	loaded, err := store.GetRootFSGeneration(ctx, next.ID)
	require.NoError(t, err)
	require.Equal(t, next.Descriptor, loaded.Descriptor)
	require.Equal(t, next.CurrentBlockHead, loaded.CurrentBlockHead)
}

func TestForkRootFSFilesystemSharesBlockGenerationAndPublishesChildWriter(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-fork-source", "team-1")))
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-fork-target", "team-1")))

	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	source, initial, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: "sandbox-fork-source", TeamID: "team-1",
		SourceOCIRef: artifact.SourceOCIRef, SourceOCIDigest: artifact.SourceOCIDigest,
		BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)

	target, err := store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{
		SourceSandboxID: "sandbox-fork-source", TargetSandboxID: "sandbox-fork-target",
	})
	require.NoError(t, err)
	require.Equal(t, source.ID, target.SourceFilesystemID)
	require.Equal(t, initial.ID, target.HeadGenerationID)
	require.Equal(t, RootFSStorageFormatBlockCOWV1, target.StorageFormat)
	require.Equal(t, int64(0), target.WriterEpoch)

	var generationCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.rootfs_generations WHERE generation_id = $1
	`, initial.ID).Scan(&generationCount))
	require.Equal(t, 1, generationCount)

	issue := rootFSWriterGrantTestIssueRequest(
		"sandbox-fork-target", "grant-fork-child", "claim-fork-child", "slot-fork-child", bytes.Repeat([]byte{0x66}, 32),
	)
	issue.ExpectedFilesystemID = target.ID
	issue.InitialGenerationID = initial.ID
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	require.Equal(t, initial.ID, issued.Grant.InitialGenerationID)
	require.Equal(t, int64(1), issued.Grant.WriterEpoch)

	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: issue.BindingDigest,
		ConsumerNodeUID: "node-a", ConsumerCtldPodUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: "pause-fork-child",
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: issue.BindingDigest,
		ExpectedOldHeadLayerID: initial.ID,
	})
	require.NoError(t, err)
	require.NoError(t, store.WithSandboxLock(ctx, "sandbox-fork-target", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: "pause-fork-child", SandboxID: "sandbox-fork-target", Kind: SandboxLifecycleKindPause,
			Phase: SandboxLifecyclePhasePublishing, ExpectedHeadLayerID: initial.ID,
		})
	}))

	childBlockHead := digest.FromString("fork-child-block-head").String()
	childDescriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 1 << 30,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: childBlockHead,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/fork-child/map.page", Length: 4096,
				Checksum: digest.FromString("fork-child-map-page").String(),
			},
		},
	})
	require.NoError(t, err)
	child := &RootFSGeneration{
		ID: "generation-fork-child", FilesystemID: target.ID, ParentGenerationID: initial.ID,
		SourceOCIDigest: initial.SourceOCIDigest, BaseArtifactDigest: initial.BaseArtifactDigest,
		BaseBlockRoot: initial.BaseBlockRoot, CurrentBlockHead: childBlockHead,
		WriterEpoch: issued.Grant.WriterEpoch, FormatGeneration: initial.FormatGeneration,
		DurabilityState: RootFSGenerationStateS3Materialized, LocatorVersion: initial.LocatorVersion + 1,
		Descriptor: childDescriptor,
	}
	proof := sha256.Sum256([]byte("fork-child-detach-seal-proof"))
	require.NoError(t, store.WithSandboxLock(ctx, "sandbox-fork-target", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		writerTx, ok := tx.(RootFSWriterGrantTx)
		require.True(t, ok)
		if _, publishErr := writerTx.CompleteRootFSWriterRetireAndPublishGeneration(lockCtx, &CompleteRootFSWriterRetireAndPublishGenerationRequest{
			LifecycleTxnID: "pause-fork-child", GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
			OperationID: "pause-fork-child", BindingVersion: RootFSWriterBindingVersion,
			BindingDigest: issue.BindingDigest, ProofDigest: proof[:], ExpectedOldGenerationID: initial.ID,
			Generation: child,
		}); publishErr != nil {
			return publishErr
		}
		return tx.MarkRuntimePaused(lockCtx, "sandbox-fork-target", 1, time.Now().UTC())
	}))

	loadedSource, err := store.GetRootFSFilesystem(ctx, "sandbox-fork-source")
	require.NoError(t, err)
	require.Equal(t, initial.ID, loadedSource.HeadGenerationID)
	loadedTarget, err := store.GetRootFSFilesystem(ctx, "sandbox-fork-target")
	require.NoError(t, err)
	require.Equal(t, child.ID, loadedTarget.HeadGenerationID)
	loadedChild, err := store.GetRootFSGeneration(ctx, child.ID)
	require.NoError(t, err)
	require.Equal(t, target.ID, loadedChild.FilesystemID)
	require.Equal(t, initial.ID, loadedChild.ParentGenerationID)
	retired, err := store.GetRootFSWriterGrant(ctx, issue.GrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, retired.State)
}

func TestForkRootFSFilesystemCrashAbandonPreservesSharedGeneration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-crash-source", "team-1")))
	targetRecord := rootFSTestSandboxRecord("sandbox-crash-target", "team-1")
	targetRecord.RuntimeGeneration = 7
	targetRecord.CurrentPodNamespace = "nomad"
	targetRecord.CurrentPodName = "allocation-crashed"
	require.NoError(t, store.UpsertSandbox(ctx, targetRecord))

	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	_, initial, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: "sandbox-crash-source", TeamID: "team-1", SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	target, err := store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{
		SourceSandboxID: "sandbox-crash-source", TargetSandboxID: "sandbox-crash-target",
	})
	require.NoError(t, err)
	require.Equal(t, initial.ID, target.HeadGenerationID)

	binding := sha256.Sum256([]byte("fork-crash-binding"))
	issue := rootFSWriterGrantTestIssueRequest(
		"sandbox-crash-target", "grant-fork-crash", "claim-fork-crash", "slot-fork-crash", binding[:],
	)
	issue.ExpectedFilesystemID = target.ID
	issue.InitialGenerationID = initial.ID
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	_, err = store.ConsumeRootFSWriterGrant(ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ConsumerNodeUID: issue.NodeUID, ConsumerCtldPodUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_writer_grants
		SET lease_expires_at = NOW() - ($2::bigint * INTERVAL '1 millisecond')
		WHERE grant_id = $1
	`, issue.GrantID, RootFSWriterCrashAbandonGrace.Milliseconds()+1000)
	require.NoError(t, err)
	operationID := "fork-crash-abandon"
	require.NoError(t, store.WithSandboxLock(ctx, target.ID, func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: operationID, SandboxID: target.ID, Kind: SandboxLifecycleKindPause,
			Phase: SandboxLifecyclePhasePublishing, Source: SandboxLifecycleSourceCrash,
			FromGeneration: 7, FromPodNamespace: "nomad", FromPodName: "allocation-crashed",
			ExpectedHeadLayerID: initial.ID,
		})
	}))
	_, err = store.BeginRootFSWriterCrashAbandon(ctx, &BeginRootFSWriterCrashAbandonRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: operationID,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		NodeUID: issue.NodeUID, NodeBootID: issue.NodeBootID, ExpectedOldGenerationID: initial.ID,
	})
	require.NoError(t, err)
	proof := sha256.Sum256([]byte("fork-crash-physical-absence-proof"))
	require.NoError(t, store.WithSandboxLock(ctx, target.ID, func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		crashTx, ok := tx.(RootFSWriterCrashAbandonTx)
		require.True(t, ok)
		_, completeErr := crashTx.CompleteRootFSWriterCrashAbandon(lockCtx, &CompleteRootFSWriterCrashAbandonRequest{
			LifecycleTxnID: operationID, GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch,
			OperationID: operationID, BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
			ProofVersion: RootFSWriterCrashAbandonProofVersion, ProofDigest: proof[:],
			NodeUID: issue.NodeUID, NodeBootID: issue.NodeBootID, ExpectedOldGenerationID: initial.ID,
		})
		return completeErr
	}))

	loadedTarget, err := store.GetRootFSFilesystem(ctx, target.ID)
	require.NoError(t, err)
	require.Equal(t, initial.ID, loadedTarget.HeadGenerationID)
	loadedSource, err := store.GetRootFSFilesystem(ctx, "sandbox-crash-source")
	require.NoError(t, err)
	require.Equal(t, initial.ID, loadedSource.HeadGenerationID)
	grant, err := store.GetRootFSWriterGrant(ctx, issue.GrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetired, grant.State)
	require.Equal(t, RootFSWriterRetireKindCrashAbandon, grant.RetireKind)
	record, err := store.GetSandbox(ctx, target.ID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, record.DesiredState)
}

func assertBlockGenerationPublishState(
	t *testing.T,
	ctx context.Context,
	store *PGSandboxStore,
	pool *pgxpool.Pool,
	wantHead, wantGrantState, wantSandboxState string,
	wantGeneration bool,
) {
	t.Helper()
	filesystem, err := store.GetRootFSFilesystem(ctx, "sandbox-pause")
	require.NoError(t, err)
	require.Equal(t, wantHead, filesystem.HeadGenerationID)
	grant, err := store.GetRootFSWriterGrant(ctx, "grant-pause")
	require.NoError(t, err)
	require.Equal(t, wantGrantState, grant.State)
	record, err := store.GetSandbox(ctx, "sandbox-pause")
	require.NoError(t, err)
	require.Equal(t, wantSandboxState, record.DesiredState)
	var exists bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM manager.rootfs_generations WHERE generation_id = 'generation-paused')
	`).Scan(&exists))
	require.Equal(t, wantGeneration, exists)
}
