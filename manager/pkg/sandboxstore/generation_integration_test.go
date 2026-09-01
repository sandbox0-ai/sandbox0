package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
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
	require.Equal(t, artifactRequest.Platform, artifact.Platform)
	selected, err := store.GetReadyRootFSBaseArtifact(
		ctx, artifact.SourceOCIDigest, artifact.Platform, ReadyRootFSArtifactRequirements{
			FormatGeneration: artifact.FormatGeneration,
			LogicalSizeBytes: artifact.LogicalSizeBytes,
			ProcdProtocol:    artifact.ProcdProtocol,
			ProcdDigest:      artifact.ProcdDigest,
		},
	)
	require.NoError(t, err)
	require.Equal(t, artifact.ArtifactDigest, selected.ArtifactDigest)
	for name, requirements := range map[string]ReadyRootFSArtifactRequirements{
		"logical size": {
			FormatGeneration: artifact.FormatGeneration,
			LogicalSizeBytes: 2 << 30,
			ProcdProtocol:    artifact.ProcdProtocol,
			ProcdDigest:      artifact.ProcdDigest,
		},
		"procd digest": {
			FormatGeneration: artifact.FormatGeneration,
			LogicalSizeBytes: artifact.LogicalSizeBytes,
			ProcdProtocol:    artifact.ProcdProtocol,
			ProcdDigest:      "sha256:" + strings.Repeat("d", 64),
		},
	} {
		t.Run("reject mismatched "+name, func(t *testing.T) {
			_, err := store.GetReadyRootFSBaseArtifact(
				ctx, artifact.SourceOCIDigest, artifact.Platform, requirements,
			)
			require.ErrorIs(t, err, ErrRootFSBaseArtifactNotFound)
		})
	}
	selected, err = store.GetReadyRootFSBaseArtifactByDigest(
		ctx, artifact.ArtifactDigest, artifact.Platform, ReadyRootFSArtifactRequirements{
			FormatGeneration: artifact.FormatGeneration,
			LogicalSizeBytes: artifact.LogicalSizeBytes,
			ProcdProtocol:    artifact.ProcdProtocol,
			ProcdDigest:      artifact.ProcdDigest,
		},
	)
	require.NoError(t, err)
	require.Equal(t, artifact.ArtifactDigest, selected.ArtifactDigest)
	_, err = store.GetReadyRootFSBaseArtifact(ctx, artifact.SourceOCIDigest,
		RootFSArtifactPlatform{OS: "linux", Architecture: "arm64"},
		ReadyRootFSArtifactRequirements{
			FormatGeneration: artifact.FormatGeneration,
			LogicalSizeBytes: artifact.LogicalSizeBytes,
			ProcdProtocol:    artifact.ProcdProtocol,
			ProcdDigest:      artifact.ProcdDigest,
		})
	require.ErrorIs(t, err, ErrRootFSBaseArtifactNotFound)

	ensureRequest := &EnsureInitialRootFSGenerationRequest{
		SandboxID:          "sandbox-generation",
		TeamID:             "team-1",
		SourceOCIRef:       artifact.SourceOCIRef,
		SourceOCIDigest:    artifact.SourceOCIDigest,
		BaseArtifactDigest: artifact.ArtifactDigest,
	}
	filesystem, generation, err := store.EnsureInitialRootFSGeneration(ctx, ensureRequest)
	require.NoError(t, err)
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
	record.RuntimeNamespace = "sandbox0"
	record.RuntimeID = "sandbox-pause"
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
		ConsumerAgentUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: "pause-txn",
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:],
		ExpectedOldGenerationID: initial.ID,
	})
	require.NoError(t, err)
	require.NoError(t, store.WithSandboxLock(ctx, "sandbox-pause", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: "pause-txn", SandboxID: "sandbox-pause", Kind: SandboxLifecycleKindPause,
			Phase: SandboxLifecyclePhasePublishing, ExpectedGenerationID: initial.ID,
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
	sourceRecord := rootFSTestSandboxRecord("sandbox-fork-source", "team-1")
	sourceRecord.DesiredState = SandboxDesiredStatePaused
	targetRecord := rootFSTestSandboxRecord("sandbox-fork-target", "team-1")
	targetRecord.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(ctx, sourceRecord))
	require.NoError(t, store.UpsertSandbox(ctx, targetRecord))

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
		ConsumerNodeUID: "node-a", ConsumerAgentUID: "ctld-a", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = store.BeginRootFSWriterRetire(ctx, &BeginRootFSWriterRetireRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: "pause-fork-child",
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: issue.BindingDigest,
		ExpectedOldGenerationID: initial.ID,
	})
	require.NoError(t, err)
	require.NoError(t, store.WithSandboxLock(ctx, "sandbox-fork-target", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: "pause-fork-child", SandboxID: "sandbox-fork-target", Kind: SandboxLifecycleKindPause,
			Phase: SandboxLifecyclePhasePublishing, ExpectedGenerationID: initial.ID,
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

func TestForkRunningRootFSFilesystemKeepsSourceWriterLiveIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "running-fork")
	ctx := fixture.ctx
	store := fixture.store
	sourceRecord, err := store.GetSandbox(ctx, fixture.sandboxID)
	require.NoError(t, err)
	targetRecord := nomadRunningForkTargetRecord(sourceRecord, "sandbox-nomad-running-fork-target")
	operationID := "nomad-running-fork-operation"
	request := &NomadSandboxForkRequest{
		OperationID: operationID, SourceSandboxID: sourceRecord.ID,
		ExpectedTeamID: sourceRecord.TeamID, Target: targetRecord,
	}
	candidate, err := store.RequestNomadSandboxRunningFork(ctx, request)
	require.NoError(t, err)
	require.False(t, candidate.Completed)
	require.Equal(t, fixture.slotID, candidate.Slot.ID)
	require.Equal(t, fixture.issue.GrantID, candidate.SourceWriterGrantID)
	require.Equal(t, fixture.initial.ID, candidate.SourceGenerationID)
	require.Equal(t, NomadSandboxRunningForkGenerationID(operationID, targetRecord.ID), candidate.TargetGenerationID)
	activeFork, err := store.GetActiveLifecycleTxn(ctx, sourceRecord.ID)
	require.NoError(t, err)
	require.Equal(t, targetRecord.ID, activeFork.TargetSandboxID)
	require.Equal(t, candidate.TargetGenerationID, activeFork.TargetGenerationID)
	require.Len(t, activeFork.TargetRecordDigest, sha256.Size)
	require.Empty(t, activeFork.ToRuntimeNamespace)
	require.Empty(t, activeFork.ToRuntimeID,
		"Nomad target identity must not depend on legacy Kubernetes Pod fields")
	unpublishedTarget, err := store.GetRootFSFilesystem(ctx, targetRecord.ID)
	require.NoError(t, err)
	require.Nil(t, unpublishedTarget,
		"durable pre-operation must not expose a target RootFS before publication")

	preoperationRetry, err := store.RequestNomadSandboxRunningFork(ctx, request)
	require.NoError(t, err)
	require.False(t, preoperationRetry.Completed)
	require.Equal(t, candidate.SourceWriterGrantID, preoperationRetry.SourceWriterGrantID)
	require.Equal(t, candidate.TargetGenerationID, preoperationRetry.TargetGenerationID)

	forkRequest, checkpointDescriptor := nomadRunningForkCheckpointRequest(
		t, fixture, sourceRecord, targetRecord, candidate, operationID,
	)
	checkpoint := forkRequest.Generation
	target, err := store.ForkRunningRootFSFilesystem(ctx, forkRequest)
	require.NoError(t, err)
	require.Equal(t, fixture.filesystem.ID, target.SourceFilesystemID)
	require.Equal(t, checkpoint.ID, target.HeadGenerationID)
	require.Equal(t, candidate.SourceWriterEpoch, target.WriterEpoch)

	loadedSource, err := store.GetRootFSFilesystem(ctx, sourceRecord.ID)
	require.NoError(t, err)
	require.Equal(t, fixture.initial.ID, loadedSource.HeadGenerationID, "running fork must not advance the source head")
	require.Equal(t, candidate.SourceWriterEpoch, loadedSource.WriterEpoch)
	grant, err := store.GetRootFSWriterGrant(ctx, candidate.SourceWriterGrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateConsumed, grant.State)
	lifecycle, err := store.GetActiveLifecycleTxn(ctx, sourceRecord.ID)
	require.NoError(t, err)
	require.Nil(t, lifecycle)
	completedCandidate, err := store.RequestNomadSandboxRunningFork(ctx, request)
	require.NoError(t, err)
	require.True(t, completedCandidate.Completed)
	require.Equal(t, targetRecord.ID, completedCandidate.Target.ID)
	aborted, err := store.AbortNomadSandboxRunningFork(
		ctx, operationID, sourceRecord.ID, targetRecord.ID, "late recovery deadline",
	)
	require.NoError(t, err)
	require.False(t, aborted, "published checkpoint must win a concurrent abort")

	materializedDescriptor := checkpointDescriptor
	materializedDescriptor.CompositeTail = nil
	materializedPayload, err := rootfsblock.EncodeDescriptor(materializedDescriptor)
	require.NoError(t, err)
	materialization := beginRootFSMaterializationIntegrationBatch(
		t, ctx, store, checkpoint, materializedPayload,
	)
	require.NoError(t, store.PublishRootFSGenerationMaterializationBatch(ctx, materialization))
	renewed, err := store.RenewRootFSWriterGrant(ctx, &RenewRootFSWriterGrantRequest{
		GrantID: candidate.SourceWriterGrantID, WriterEpoch: candidate.SourceWriterEpoch,
		BindingVersion: candidate.BindingVersion, BindingDigest: candidate.BindingDigest,
		ConsumerNodeUID: candidate.Slot.NodeUID,
	}, RootFSWriterLeaseRenewalPolicy{LeaseTTL: time.Minute, GracePeriod: time.Second})
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateConsumed, renewed.State)
	childBinding := sha256.Sum256([]byte("running-fork-child-binding"))
	childIssue := rootFSWriterGrantTestIssueRequest(
		targetRecord.ID, "grant-running-fork-child", "claim-running-fork-child", "slot-running-fork-child", childBinding[:],
	)
	childIssue.ExpectedFilesystemID = target.ID
	childIssue.InitialGenerationID = checkpoint.ID
	childIssue.ExpectedWriterEpoch = target.WriterEpoch
	child, err := store.IssueRootFSWriterGrant(ctx, childIssue)
	require.NoError(t, err)
	require.Equal(t, candidate.SourceWriterEpoch+1, child.Grant.WriterEpoch)
	loadedSource, err = store.GetRootFSFilesystem(ctx, sourceRecord.ID)
	require.NoError(t, err)
	require.Equal(t, candidate.SourceWriterEpoch, loadedSource.WriterEpoch, "child authority must not fence the source writer")

	retried, err := store.ForkRunningRootFSFilesystem(ctx, forkRequest)
	require.NoError(t, err)
	require.Equal(t, target.ID, retried.ID)
	require.Equal(t, target.HeadGenerationID, retried.HeadGenerationID)

	changed := *forkRequest
	changed.CheckpointProof.CheckpointSequence++
	changedDigest, digestErr := changed.CheckpointProof.Digest()
	require.NoError(t, digestErr)
	changed.CheckpointProofDigest = changedDigest[:]
	_, err = store.ForkRunningRootFSFilesystem(ctx, &changed)
	require.ErrorIs(t, err, ErrRootFSFilesystemConflict)
	_, err = store.CancelRootFSWriterGrant(ctx, &CancelRootFSWriterGrantRequest{
		GrantID: childIssue.GrantID, WriterEpoch: child.Grant.WriterEpoch,
		OperationID: childIssue.OperationID, BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: childBinding[:],
	})
	require.NoError(t, err)
	limit := int64(10)
	resume, err := store.RequestNomadSandboxResume(ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: targetRecord.ID, ExpectedTeamID: targetRecord.TeamID, ActiveSandboxLimit: &limit,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), resume.RuntimeGeneration,
		"a never-run paused fork target must resume into runtime generation one")
	require.Equal(t, checkpoint.ID, resume.SourceGenerationID)
}

func TestRequestSandboxRuntimeClaimCleanupAbortsInboundNomadRunningForkIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "running-fork-target-cleanup")
	source, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	target := nomadRunningForkTargetRecord(source, "sandbox-nomad-running-fork-cleanup-target")
	operationID := "nomad-running-fork-target-cleanup-operation"
	request := &NomadSandboxForkRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: target,
	}
	candidate, err := fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, request)
	require.NoError(t, err)

	cleanup, err := fixture.store.RequestSandboxRuntimeClaimCleanup(
		fixture.ctx, target.ID, "fork target deletion requested",
	)
	require.NoError(t, err)
	require.NotNil(t, cleanup)
	require.False(t, cleanup.PhysicalStateRequired)
	require.Empty(t, cleanup.SlotID)
	var lifecyclePhase, lifecycleError string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT phase, error FROM manager.sandbox_lifecycle_txns WHERE txn_id = $1
	`, operationID).Scan(&lifecyclePhase, &lifecycleError))
	require.Equal(t, SandboxLifecyclePhaseAborted, lifecyclePhase)
	require.Equal(t, "fork target termination requested", lifecycleError)
	active, err := fixture.store.GetActiveLifecycleTxn(fixture.ctx, source.ID)
	require.NoError(t, err)
	require.Nil(t, active)

	forkRequest, _ := nomadRunningForkCheckpointRequest(
		t, fixture, source, target, candidate, operationID,
	)
	_, err = fixture.store.ForkRunningRootFSFilesystem(fixture.ctx, forkRequest)
	require.ErrorIs(t, err, ErrRootFSFilesystemConflict)
	grant, err := fixture.store.GetRootFSWriterGrant(fixture.ctx, candidate.SourceWriterGrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateConsumed, grant.State,
		"deleting an unpublished target must not fence the source writer")
}

func TestRequestSandboxRuntimeClaimCleanupAbortsOutgoingNomadForkAndTargetIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "running-fork-source-cleanup")
	source, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	target := nomadRunningForkTargetRecord(source, "sandbox-nomad-running-fork-source-cleanup-target")
	operationID := "nomad-running-fork-source-cleanup-operation"
	_, err = fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, &NomadSandboxForkRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: target,
	})
	require.NoError(t, err)

	cleanup, err := fixture.store.RequestSandboxRuntimeClaimCleanup(
		fixture.ctx, source.ID, "fork source deletion requested",
	)
	require.NoError(t, err)
	require.True(t, cleanup.PhysicalStateRequired)
	require.Equal(t, fixture.slotID, cleanup.SlotID)
	var lifecyclePhase, targetDesiredState, targetClaimPhase string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT lifecycle.phase, target.desired_state, claim.phase
		FROM manager.sandbox_lifecycle_txns AS lifecycle
		JOIN manager.sandboxes AS target ON target.sandbox_id = lifecycle.target_sandbox_id
		JOIN manager.sandbox_runtime_claims AS claim ON claim.sandbox_id = target.sandbox_id
		WHERE lifecycle.txn_id = $1
	`, operationID).Scan(&lifecyclePhase, &targetDesiredState, &targetClaimPhase))
	require.Equal(t, SandboxLifecyclePhaseAborted, lifecyclePhase)
	require.Equal(t, SandboxDesiredStateTerminating, targetDesiredState)
	require.Equal(t, SandboxRuntimeClaimPhaseCleanupPending, targetClaimPhase)
	targetFilesystem, err := fixture.store.GetRootFSFilesystem(fixture.ctx, target.ID)
	require.NoError(t, err)
	require.Nil(t, targetFilesystem)
}

func TestRequestNomadSandboxRunningForkRejectsMutatedDurableTargetIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "running-fork-target-mutation")
	source, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	target := nomadRunningForkTargetRecord(source, "sandbox-nomad-running-fork-mutated-target")
	operationID := "nomad-running-fork-target-mutation-operation"
	request := &NomadSandboxForkRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: target,
	}
	_, err = fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, request)
	require.NoError(t, err)
	mutated, err := fixture.store.GetSandbox(fixture.ctx, target.ID)
	require.NoError(t, err)
	autoResume := false
	mutated.Config.AutoResume = &autoResume
	require.NoError(t, fixture.store.UpsertSandbox(fixture.ctx, mutated))

	_, err = fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, &NomadSandboxForkRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: mutated,
	})
	require.ErrorIs(t, err, ErrNomadSandboxForkConflict)
	_, err = fixture.store.RequestSandboxRuntimeClaimCleanup(
		fixture.ctx, target.ID, "clean up mutated fork target",
	)
	require.NoError(t, err)
}

func TestNomadRunningForkPublicationAndTargetCleanupShareLockOrderIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "running-fork-cleanup-race")
	source, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	target := nomadRunningForkTargetRecord(source, "sandbox-nomad-running-fork-cleanup-race-target")
	operationID := "nomad-running-fork-cleanup-race-operation"
	candidate, err := fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, &NomadSandboxForkRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: target,
	})
	require.NoError(t, err)
	forkRequest, _ := nomadRunningForkCheckpointRequest(
		t, fixture, source, target, candidate, operationID,
	)
	start := make(chan struct{})
	forkDone := make(chan error, 1)
	cleanupDone := make(chan error, 1)
	go func() {
		<-start
		_, forkErr := fixture.store.ForkRunningRootFSFilesystem(fixture.ctx, forkRequest)
		forkDone <- forkErr
	}()
	go func() {
		<-start
		_, cleanupErr := fixture.store.RequestSandboxRuntimeClaimCleanup(
			fixture.ctx, target.ID, "concurrent fork target deletion",
		)
		cleanupDone <- cleanupErr
	}()
	close(start)
	require.NoError(t, <-cleanupDone)
	forkErr := <-forkDone
	if forkErr != nil {
		require.ErrorIs(t, forkErr, ErrRootFSFilesystemConflict)
	}
	active, err := fixture.store.GetActiveLifecycleTxn(fixture.ctx, source.ID)
	require.NoError(t, err)
	require.Nil(t, active)
	var targetDesiredState, targetClaimPhase string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT sandbox.desired_state, claim.phase
		FROM manager.sandboxes AS sandbox
		JOIN manager.sandbox_runtime_claims AS claim USING (sandbox_id)
		WHERE sandbox.sandbox_id = $1
	`, target.ID).Scan(&targetDesiredState, &targetClaimPhase))
	require.Equal(t, SandboxDesiredStateTerminating, targetDesiredState)
	require.Equal(t, SandboxRuntimeClaimPhaseCleanupPending, targetClaimPhase)
}

func TestAbortNomadSandboxRunningForkQueuesNeverRunTargetCleanupIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "running-fork-stale-abort")
	source, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	target := nomadRunningForkTargetRecord(source, "sandbox-nomad-running-fork-stale-target")
	operationID := "nomad-running-fork-stale-operation"
	request := &NomadSandboxForkRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: target,
	}
	candidate, err := fixture.store.RequestNomadSandboxRunningFork(fixture.ctx, request)
	require.NoError(t, err)

	aborted, err := fixture.store.AbortNomadSandboxRunningFork(
		fixture.ctx, operationID, source.ID, target.ID, "running fork recovery deadline exceeded",
	)
	require.NoError(t, err)
	require.True(t, aborted)
	var lifecyclePhase, desiredState, claimPhase string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT lifecycle.phase, target.desired_state, claim.phase
		FROM manager.sandbox_lifecycle_txns AS lifecycle
		JOIN manager.sandboxes AS target ON target.sandbox_id = lifecycle.target_sandbox_id
		JOIN manager.sandbox_runtime_claims AS claim ON claim.sandbox_id = target.sandbox_id
		WHERE lifecycle.txn_id = $1
	`, operationID).Scan(&lifecyclePhase, &desiredState, &claimPhase))
	require.Equal(t, SandboxLifecyclePhaseAborted, lifecyclePhase)
	require.Equal(t, SandboxDesiredStateTerminating, desiredState)
	require.Equal(t, SandboxRuntimeClaimPhaseCleanupPending, claimPhase)
	aborted, err = fixture.store.AbortNomadSandboxRunningFork(
		fixture.ctx, operationID, source.ID, target.ID, "running fork recovery deadline exceeded",
	)
	require.NoError(t, err)
	require.False(t, aborted)
	grant, err := fixture.store.GetRootFSWriterGrant(fixture.ctx, candidate.SourceWriterGrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateConsumed, grant.State)
}

func TestForkNomadPausedSandboxCommitsLogicalTargetAndRootFSAtomicallyIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "paused-fork")
	pause, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	fixture.publishPlannedPause(t, pause.OperationID)
	source, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, source.DesiredState)
	repository := egressauthstore.NewRepository(fixture.pool,
		egressauthstore.WithDefaultStorageKind(egressauthstore.CredentialSourceStorageKindPlaintextPG))
	_, err = repository.PutSource(fixture.ctx, source.TeamID, &egressauthstore.CredentialSourceWriteRequest{
		Name: "paused-fork-source", ResolverKind: "static_headers",
		StorageKind: egressauthstore.CredentialSourceStorageKindPlaintextPG,
		Spec: egressauthstore.CredentialSourceSecretSpec{StaticHeaders: &egressauthstore.StaticHeadersSourceSpec{
			Values: map[string]string{"token": "fork-token"},
		}},
	})
	require.NoError(t, err)
	binding := egressauthstore.CredentialBinding{
		Ref: "api-auth", SourceRef: "paused-fork-source",
		Projection: egressauthstore.ProjectionSpec{
			Type: egressauthstore.CredentialProjectionTypeHTTPHeaders,
			HTTPHeaders: &egressauthstore.HTTPHeadersProjection{Headers: []egressauthstore.ProjectedHeader{{
				Name: "Authorization", ValueTemplate: "Bearer {{.token}}",
			}}},
		},
	}
	require.NoError(t, fixture.store.WithSandboxLock(fixture.ctx, source.ID, func(
		ctx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		credentialTx, ok := tx.(SandboxCredentialBindingTx)
		if !ok {
			return fmt.Errorf("credential transaction is unavailable")
		}
		_, err := credentialTx.ReplaceNomadSandboxCredentialBindings(
			ctx, source.TeamID, source.ID, []egressauthstore.CredentialBinding{binding},
		)
		return err
	}))
	target := nomadRunningForkTargetRecord(source, "sandbox-nomad-paused-fork-target")
	t.Cleanup(func() {
		_ = repository.DeleteBindings(context.Background(), source.TeamID, source.ID)
		_ = repository.DeleteBindings(context.Background(), source.TeamID, target.ID)
		_ = repository.DeleteSource(context.Background(), source.TeamID, "paused-fork-source")
	})
	request := &NomadSandboxForkRequest{
		OperationID: "nomad-paused-fork-operation", SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: target,
	}

	created, err := fixture.store.ForkNomadPausedSandbox(fixture.ctx, request)
	require.NoError(t, err)
	require.Equal(t, target.ID, created.ID)
	sourceFilesystem, err := fixture.store.GetRootFSFilesystem(fixture.ctx, source.ID)
	require.NoError(t, err)
	targetFilesystem, err := fixture.store.GetRootFSFilesystem(fixture.ctx, target.ID)
	require.NoError(t, err)
	require.Equal(t, sourceFilesystem.ID, targetFilesystem.SourceFilesystemID)
	require.Equal(t, sourceFilesystem.HeadGenerationID, targetFilesystem.HeadGenerationID)
	require.Equal(t, sourceFilesystem.WriterEpoch, targetFilesystem.WriterEpoch)
	var claimOperationID, claimPhase, lifecyclePhase, expectedHead, preparedHead string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT claim.operation_id, claim.phase, lifecycle.phase,
			lifecycle.expected_generation_id, lifecycle.prepared_generation_id
		FROM manager.sandbox_runtime_claims AS claim
		JOIN manager.sandbox_lifecycle_txns AS lifecycle
			ON lifecycle.target_sandbox_id = claim.sandbox_id
		WHERE claim.sandbox_id = $1 AND lifecycle.txn_id = $2
	`, target.ID, request.OperationID).Scan(
		&claimOperationID, &claimPhase, &lifecyclePhase, &expectedHead, &preparedHead,
	))
	require.Equal(t, NomadSandboxForkClaimOperationID(request.OperationID, target.ID), claimOperationID)
	require.Equal(t, SandboxRuntimeClaimPhaseReady, claimPhase)
	require.Equal(t, SandboxLifecyclePhaseCommitted, lifecyclePhase)
	require.Equal(t, sourceFilesystem.HeadGenerationID, expectedHead)
	require.Equal(t, expectedHead, preparedHead)
	targetBindings, err := fixture.store.GetNomadSandboxCredentialBindings(
		fixture.ctx, target.TeamID, target.ID,
	)
	require.NoError(t, err)
	require.Len(t, targetBindings.Bindings, 1)
	require.Equal(t, binding.SourceRef, targetBindings.Bindings[0].SourceRef)

	changedSource := *source
	autoResume := false
	changedSource.Config.AutoResume = &autoResume
	require.NoError(t, fixture.store.UpsertSandbox(fixture.ctx, &changedSource))
	retried, err := fixture.store.ForkNomadPausedSandbox(fixture.ctx, request)
	require.NoError(t, err,
		"completed retry must not depend on mutable source configuration")
	require.Equal(t, target.ID, retried.ID)

	changedRequest := *request
	changedTarget := *target
	changedTarget.ExpiresAt = changedTarget.ExpiresAt.Add(time.Minute)
	changedRequest.Target = &changedTarget
	_, err = fixture.store.ForkNomadPausedSandbox(fixture.ctx, &changedRequest)
	require.ErrorIs(t, err, ErrNomadSandboxForkConflict)
}

func nomadRunningForkTargetRecord(source *SandboxRecord, targetID string) *SandboxRecord {
	target := *source
	target.ID = targetID
	target.DesiredState = SandboxDesiredStatePaused
	target.RuntimeID = ""
	target.RuntimeNamespace = ""
	target.RuntimeGeneration = 0
	target.LifecycleEpoch = 0
	target.HotClaimCompletedAt = time.Time{}
	target.CreatedAt = time.Now().UTC()
	target.UpdatedAt = target.CreatedAt
	return &target
}

func nomadRunningForkCheckpointRequest(
	t *testing.T,
	fixture *nomadPauseStoreFixture,
	source, target *SandboxRecord,
	candidate *NomadSandboxRunningForkCandidate,
	operationID string,
) (*ForkRunningRootFSFilesystemRequest, rootfsblock.Descriptor) {
	t.Helper()
	baseDescriptor, err := rootfsblock.DecodeDescriptor(fixture.initial.Descriptor)
	require.NoError(t, err)
	checkpointDescriptor, checkpointPayload, err := rootfsblock.BuildCompositeGeneration(
		baseDescriptor,
		[]rootfsblock.BlockUpdate{{Block: 3, Data: bytes.Repeat([]byte{0x73}, rootfsblock.LogicalBlockSize)}},
	)
	require.NoError(t, err)
	checkpoint := &RootFSGeneration{
		ID: candidate.TargetGenerationID, FilesystemID: target.ID,
		ParentGenerationID: fixture.initial.ID, SourceOCIDigest: fixture.initial.SourceOCIDigest,
		BaseArtifactDigest: fixture.initial.BaseArtifactDigest, BaseBlockRoot: fixture.initial.BaseBlockRoot,
		CurrentBlockHead: checkpointDescriptor.MappingRoot.RootDigest,
		WriterEpoch:      candidate.SourceWriterEpoch, FormatGeneration: fixture.initial.FormatGeneration,
		DurabilityState: RootFSGenerationStateCompositeDurable,
		LocatorVersion:  fixture.initial.LocatorVersion + 1, Descriptor: checkpointPayload,
	}
	proof := rootfshandoff.RunningForkCheckpointProof{
		Version:     rootfshandoff.RunningForkCheckpointVersion,
		OperationID: operationID, SourceSandboxID: source.ID,
		SourceFilesystemID: candidate.SourceFilesystemID, TargetSandboxID: target.ID,
		SourceWriterGrantID: candidate.SourceWriterGrantID, SourceWriterEpoch: candidate.SourceWriterEpoch,
		BindingVersion: candidate.BindingVersion, BindingDigest: hex.EncodeToString(candidate.BindingDigest),
		ExpectedSourceGenerationID: candidate.SourceGenerationID, CheckpointGenerationID: checkpoint.ID,
		CheckpointSequence: 1, CheckpointDescriptorDigest: digest.FromBytes(checkpointPayload).String(),
	}
	proofDigest, err := proof.Digest()
	require.NoError(t, err)
	return &ForkRunningRootFSFilesystemRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		TargetSandboxID: target.ID, TargetTeamID: target.TeamID,
		SourceGrantID: candidate.SourceWriterGrantID, SourceWriterEpoch: candidate.SourceWriterEpoch,
		BindingVersion: candidate.BindingVersion, BindingDigest: candidate.BindingDigest,
		CheckpointProof: proof, CheckpointProofDigest: proofDigest[:],
		ExpectedSourceGenerationID: candidate.SourceGenerationID,
		Generation:                 checkpoint,
	}, checkpointDescriptor
}

func TestForkRootFSFilesystemCrashAbandonPreservesSharedGeneration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	sourceRecord := rootFSTestSandboxRecord("sandbox-crash-source", "team-1")
	sourceRecord.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(ctx, sourceRecord))
	targetRecord := rootFSTestSandboxRecord("sandbox-crash-target", "team-1")
	targetRecord.DesiredState = SandboxDesiredStatePaused
	targetRecord.RuntimeGeneration = 7
	targetRecord.RuntimeNamespace = "nomad"
	targetRecord.RuntimeID = "allocation-crashed"
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
	targetRecord.DesiredState = SandboxDesiredStateActive
	require.NoError(t, store.UpsertSandbox(ctx, targetRecord))

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
		ConsumerNodeUID: issue.NodeUID, ConsumerAgentUID: "ctld-a", LeaseTTL: time.Minute,
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
			FromGeneration: 7, FromRuntimeNamespace: "nomad", FromRuntimeID: "allocation-crashed",
			ExpectedGenerationID: initial.ID,
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

func TestForkRootFSFilesystemCarriesSharedGenerationEpoch(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	sourceRecord := rootFSTestSandboxRecord("sandbox-fork-epoch-source", "team-1")
	sourceRecord.DesiredState = SandboxDesiredStatePaused
	targetRecord := rootFSTestSandboxRecord("sandbox-fork-epoch-target", "team-1")
	targetRecord.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(ctx, sourceRecord))
	require.NoError(t, store.UpsertSandbox(ctx, targetRecord))
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	filesystem, initial, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: sourceRecord.ID, TeamID: sourceRecord.TeamID, SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	shared := putTestDurableRootFSGeneration(t, ctx, pool, filesystem, initial, "fork-shared-epoch", 7)
	target, err := store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{
		SourceSandboxID: sourceRecord.ID, TargetSandboxID: targetRecord.ID,
	})
	require.NoError(t, err)
	require.Equal(t, shared.ID, target.HeadGenerationID)
	require.Equal(t, shared.WriterEpoch, target.WriterEpoch)
	binding := bytes.Repeat([]byte{0x71}, 32)
	issue := rootFSWriterGrantTestIssueRequest(targetRecord.ID, "grant-fork-epoch", "claim-fork-epoch", "slot-fork-epoch", binding)
	issue.ExpectedFilesystemID = target.ID
	issue.InitialGenerationID = shared.ID
	issue.ExpectedWriterEpoch = shared.WriterEpoch
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	require.Equal(t, shared.WriterEpoch+1, issued.Grant.WriterEpoch)
}

func TestRootFSForkDepthAndFanoutScale(t *testing.T) {
	if os.Getenv("ROOTFS_FORK_SCALE_TEST") != "1" {
		t.Skip("set ROOTFS_FORK_SCALE_TEST=1 to run the 2,000-fork scale gate")
	}
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	root := rootFSTestSandboxRecord("fork-scale-root", "team-scale")
	root.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(ctx, root))
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	_, generation, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: root.ID, TeamID: root.TeamID, SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)

	const depth = 1000
	depthStarted := time.Now()
	parentID := root.ID
	checkpointDurations := make(map[int]time.Duration)
	for level := 1; level <= depth; level++ {
		targetID := fmt.Sprintf("fork-scale-depth-%04d", level)
		target := rootFSTestSandboxRecord(targetID, root.TeamID)
		target.DesiredState = SandboxDesiredStatePaused
		require.NoError(t, store.UpsertSandbox(ctx, target))
		started := time.Now()
		forked, forkErr := store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{
			SourceSandboxID: parentID, TargetSandboxID: targetID,
		})
		require.NoError(t, forkErr)
		require.Equal(t, generation.ID, forked.HeadGenerationID)
		if level == 1 || level == 10 || level == 100 || level == 1000 {
			checkpointDurations[level] = time.Since(started)
		}
		parentID = targetID
	}
	depthDuration := time.Since(depthStarted)

	const fanout = 1000
	fanoutStarted := time.Now()
	for index := 1; index <= fanout; index++ {
		targetID := fmt.Sprintf("fork-scale-fanout-%04d", index)
		target := rootFSTestSandboxRecord(targetID, root.TeamID)
		target.DesiredState = SandboxDesiredStatePaused
		require.NoError(t, store.UpsertSandbox(ctx, target))
		forked, forkErr := store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{
			SourceSandboxID: root.ID, TargetSandboxID: targetID,
		})
		require.NoError(t, forkErr)
		require.Equal(t, generation.ID, forked.HeadGenerationID)
	}
	fanoutDuration := time.Since(fanoutStarted)

	var filesystemCount, generationCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.rootfs_filesystems`).Scan(&filesystemCount))
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.rootfs_generations`).Scan(&generationCount))
	require.Equal(t, 1+depth+fanout, filesystemCount)
	require.Equal(t, 1, generationCount, "metadata forks must not duplicate immutable generations")
	lookupStarted := time.Now()
	for range 1000 {
		deepest, lookupErr := store.GetRootFSFilesystem(ctx, parentID)
		require.NoError(t, lookupErr)
		require.Equal(t, generation.ID, deepest.HeadGenerationID)
	}
	lookupDuration := time.Since(lookupStarted)

	binding := bytes.Repeat([]byte{0x53}, 32)
	issue := rootFSWriterGrantTestIssueRequest(parentID, "grant-fork-depth-1000", "claim-fork-depth-1000", "slot-fork-depth-1000", binding)
	issue.ExpectedFilesystemID = parentID
	issue.InitialGenerationID = generation.ID
	issue.ExpectedWriterEpoch = generation.WriterEpoch
	claimStarted := time.Now()
	issued, err := store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	claimDuration := time.Since(claimStarted)
	require.Equal(t, generation.WriterEpoch+1, issued.Grant.WriterEpoch)

	t.Logf("fork scale: depth=%d total=%s checkpoints=%v fanout=%d total=%s lookup_1000=%s deepest_issue=%s filesystems=%d generations=%d",
		depth, depthDuration, checkpointDurations, fanout, fanoutDuration,
		lookupDuration, claimDuration, filesystemCount, generationCount)
}

func TestBlockRootFSSnapshotRestoreCopyAndRollback(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	sourceRecord := rootFSTestSandboxRecord("sandbox-snapshot-block", "team-1")
	sourceRecord.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(ctx, sourceRecord))
	copyRecord := rootFSTestSandboxRecord("sandbox-snapshot-copy", "team-1")
	copyRecord.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(ctx, copyRecord))
	lateCopyRecord := rootFSTestSandboxRecord("sandbox-snapshot-late-copy", "team-1")
	lateCopyRecord.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(ctx, lateCopyRecord))

	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	filesystem, initial, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: sourceRecord.ID, TeamID: sourceRecord.TeamID,
		SourceOCIRef: artifact.SourceOCIRef, SourceOCIDigest: artifact.SourceOCIDigest,
		BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)

	snapshot, err := store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID: sourceRecord.ID, SnapshotID: "snapshot-block-initial",
	})
	require.NoError(t, err)
	require.Equal(t, initial.ID, snapshot.HeadGenerationID)
	require.Equal(t, artifact.ArtifactDigest, snapshot.BaseArtifactDigest)
	require.Equal(t, artifact.SourceOCIDigest, snapshot.SourceOCIDigest)

	claimRecord := rootFSTestSandboxRecord("sandbox-snapshot-initial-claim", sourceRecord.TeamID)
	_, err = store.ReserveSandboxClaim(ctx, &ReserveSandboxClaimRequest{
		Record: claimRecord, OperationID: "operation-snapshot-initial-claim", LeaseTTL: 15 * time.Second,
	})
	require.NoError(t, err)
	_, err = store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: claimRecord.ID, SnapshotID: snapshot.ID, TeamID: claimRecord.TeamID,
		OperationID: "operation-snapshot-initial-claim/initial-restore",
	})
	require.ErrorIs(t, err, ErrRootFSFilesystemConflict)
	claimedCopy, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: claimRecord.ID, SnapshotID: snapshot.ID, TeamID: claimRecord.TeamID,
		OperationID:             "operation-snapshot-initial-claim/initial-restore",
		InitialClaimOperationID: "operation-snapshot-initial-claim",
	})
	require.NoError(t, err)
	require.NotEqual(t, initial.ID, claimedCopy.HeadGenerationID)
	require.Equal(t, filesystem.ID, claimedCopy.SourceFilesystemID)
	claimedGeneration, err := store.GetRootFSGeneration(ctx, claimedCopy.HeadGenerationID)
	require.NoError(t, err)
	require.Equal(t, claimedCopy.ID, claimedGeneration.FilesystemID)
	require.Equal(t, initial.ID, claimedGeneration.ParentGenerationID)
	require.True(t, rootFSGenerationRestoreContentEqual(initial, claimedGeneration))
	retriedClaimedCopy, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: claimRecord.ID, SnapshotID: snapshot.ID, TeamID: claimRecord.TeamID,
		OperationID:             "operation-snapshot-initial-claim/initial-restore",
		InitialClaimOperationID: "operation-snapshot-initial-claim",
	})
	require.NoError(t, err)
	require.Equal(t, claimedCopy.HeadGenerationID, retriedClaimedCopy.HeadGenerationID)

	second := putTestDurableRootFSGeneration(t, ctx, pool, filesystem, initial, "snapshot-second", 1)
	secondSnapshot, err := store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID: sourceRecord.ID, SnapshotID: "snapshot-block-second",
	})
	require.NoError(t, err)
	require.Equal(t, second.ID, secondSnapshot.HeadGenerationID)
	lateCopy, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: lateCopyRecord.ID, SnapshotID: secondSnapshot.ID, TeamID: lateCopyRecord.TeamID,
	})
	require.NoError(t, err)
	require.NotEqual(t, second.ID, lateCopy.HeadGenerationID)
	require.Equal(t, second.WriterEpoch, lateCopy.WriterEpoch,
		"a new copied filesystem preserves the captured epoch")
	lateGeneration, err := store.GetRootFSGeneration(ctx, lateCopy.HeadGenerationID)
	require.NoError(t, err)
	require.Equal(t, lateCopy.ID, lateGeneration.FilesystemID)
	require.Equal(t, second.ID, lateGeneration.ParentGenerationID)
	require.True(t, rootFSGenerationRestoreContentEqual(second, lateGeneration))
	restored, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: sourceRecord.ID, SnapshotID: snapshot.ID, TeamID: sourceRecord.TeamID,
		OperationID: "restore-block-initial", RollbackExpiresAt: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, initial.ID, restored.HeadGenerationID)
	require.Equal(t, int64(1), restored.WriterEpoch, "head restore must not rewind the writer epoch")

	rolledBack, err := store.RollbackRootFSHead(ctx, &RollbackRootFSHeadRequest{
		SandboxID: sourceRecord.ID, OperationID: "restore-block-initial", TeamID: sourceRecord.TeamID,
	})
	require.NoError(t, err)
	require.Equal(t, second.ID, rolledBack.HeadGenerationID)
	require.Equal(t, int64(1), rolledBack.WriterEpoch)
	_, err = store.RollbackRootFSHead(ctx, &RollbackRootFSHeadRequest{
		SandboxID: sourceRecord.ID, OperationID: "restore-block-initial", TeamID: sourceRecord.TeamID,
	})
	require.ErrorIs(t, err, ErrRootFSHeadConflict)

	copied, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: copyRecord.ID, SnapshotID: snapshot.ID, TeamID: copyRecord.TeamID,
	})
	require.NoError(t, err)
	require.NotEqual(t, initial.ID, copied.HeadGenerationID)
	require.Equal(t, filesystem.ID, copied.SourceFilesystemID)
	copiedGeneration, err := store.GetRootFSGeneration(ctx, copied.HeadGenerationID)
	require.NoError(t, err)
	require.Equal(t, copied.ID, copiedGeneration.FilesystemID)
	require.Equal(t, initial.ID, copiedGeneration.ParentGenerationID)
	require.True(t, rootFSGenerationRestoreContentEqual(initial, copiedGeneration))
	replacedCopy, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: copyRecord.ID, SnapshotID: secondSnapshot.ID, TeamID: copyRecord.TeamID,
		OperationID: "restore-copy-to-second", RollbackExpiresAt: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, copied.WriterEpoch+1, replacedCopy.WriterEpoch)
	require.NotEqual(t, copied.HeadGenerationID, replacedCopy.HeadGenerationID)
	replacedGeneration, err := store.GetRootFSGeneration(ctx, replacedCopy.HeadGenerationID)
	require.NoError(t, err)
	require.Equal(t, replacedCopy.ID, replacedGeneration.FilesystemID)
	require.Equal(t, second.ID, replacedGeneration.ParentGenerationID)
	require.True(t, rootFSGenerationRestoreContentEqual(second, replacedGeneration))
	retriedReplacement, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: copyRecord.ID, SnapshotID: secondSnapshot.ID, TeamID: copyRecord.TeamID,
		OperationID: "restore-copy-to-second", RollbackExpiresAt: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, replacedCopy.HeadGenerationID, retriedReplacement.HeadGenerationID)
	require.Equal(t, replacedCopy.WriterEpoch, retriedReplacement.WriterEpoch)
	rolledBackCopy, err := store.RollbackRootFSHead(ctx, &RollbackRootFSHeadRequest{
		SandboxID: copyRecord.ID, OperationID: "restore-copy-to-second", TeamID: copyRecord.TeamID,
	})
	require.NoError(t, err)
	require.Equal(t, copied.HeadGenerationID, rolledBackCopy.HeadGenerationID)
	require.Equal(t, replacedCopy.WriterEpoch, rolledBackCopy.WriterEpoch,
		"rollback must not rewind the target writer epoch")

	loadedSnapshot, err := store.GetRootFSSnapshot(ctx, snapshot.ID, sourceRecord.TeamID)
	require.NoError(t, err)
	require.Equal(t, initial.ID, loadedSnapshot.HeadGenerationID,
		"moving and rolling back the source head must not mutate the snapshot")
}

func TestBlockRootFSSnapshotRejectsActiveWriter(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record := rootFSTestSandboxRecord("sandbox-snapshot-writer", "team-1")
	record.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(ctx, record))
	artifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	filesystem, initial, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: record.ID, TeamID: record.TeamID, SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	binding := bytes.Repeat([]byte{0x7a}, 32)
	issue := rootFSWriterGrantTestIssueRequest(record.ID, "grant-snapshot-active", "claim-snapshot-active", "slot-snapshot-active", binding)
	issue.ExpectedFilesystemID = filesystem.ID
	issue.InitialGenerationID = initial.ID
	_, err = store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	_, err = store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID: record.ID, SnapshotID: "snapshot-must-fail",
	})
	require.ErrorIs(t, err, ErrRootFSFilesystemNotFound)
}

func TestPublishPausedRootFSRebaseIsCASedAndRollbackable(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record := rootFSTestSandboxRecord("sandbox-rebase", "team-1")
	record.DesiredState = SandboxDesiredStatePaused
	record.ClusterID = "cluster-rebase"
	require.NoError(t, store.UpsertSandbox(ctx, record))
	_, err := pool.Exec(ctx, `
		INSERT INTO manager.sandbox_runtime_claims (
			sandbox_id, operation_id, phase, lease_expires_at
		) VALUES ($1, $2, $3, NULL)
	`, record.ID, "claim-rebase", SandboxRuntimeClaimPhaseReady)
	require.NoError(t, err)
	oldArtifact, err := store.PutReadyRootFSBaseArtifact(ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	filesystem, source, err := store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
		SandboxID: record.ID, TeamID: record.TeamID, SourceOCIRef: oldArtifact.SourceOCIRef,
		SourceOCIDigest: oldArtifact.SourceOCIDigest, BaseArtifactDigest: oldArtifact.ArtifactDigest,
	})
	require.NoError(t, err)
	_, err = store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID: record.ID, SnapshotID: "snapshot-before-rebase",
	})
	require.NoError(t, err)

	newArtifactRequest := readyRootFSBaseArtifactTestRequest()
	newArtifactRequest.ArtifactDigest = digest.FromString("rebase-artifact-v2").String()
	newArtifactRequest.SourceOCIRef = "registry.example/sandbox:v2@" + digest.FromString("rebase-oci-v2").String()
	newArtifactRequest.SourceOCIDigest = digest.FromString("rebase-oci-v2").String()
	newArtifactRequest.BaseBlockRoot = digest.FromString("rebase-base-root-v2").String()
	newArtifactRequest.Descriptor = encodeTestRootFSDescriptor(t, "rebase-base-v2", newArtifactRequest.BaseBlockRoot)
	newArtifact, err := store.PutReadyRootFSBaseArtifact(ctx, newArtifactRequest)
	require.NoError(t, err)
	rollbackExpiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	preoperation, err := store.RequestNomadPausedRebase(ctx, &NomadPausedRebaseRequest{
		OperationID: "rebase-v2", SandboxID: record.ID, ExpectedTeamID: record.TeamID,
		TargetBaseArtifactDigest: newArtifact.ArtifactDigest,
		RollbackExpiresAt:        rollbackExpiresAt,
		WorkerClusterID:          record.ClusterID, WorkerNodeID: "rebase-node", WorkerNodeUID: "rebase-node-uid",
	})
	require.NoError(t, err)
	require.False(t, preoperation.Completed)
	require.Equal(t, source.ID, preoperation.SourceGeneration.ID)
	require.Equal(t, oldArtifact.ArtifactDigest, preoperation.SourceBaseArtifact.ArtifactDigest)
	require.Equal(t, newArtifact.ArtifactDigest, preoperation.TargetBaseArtifact.ArtifactDigest)
	retryPreoperation, err := store.RequestNomadPausedRebase(ctx, &NomadPausedRebaseRequest{
		OperationID: "rebase-v2", SandboxID: record.ID, ExpectedTeamID: record.TeamID,
		TargetBaseArtifactDigest: newArtifact.ArtifactDigest,
		RollbackExpiresAt:        rollbackExpiresAt,
		WorkerClusterID:          record.ClusterID, WorkerNodeID: "rebase-node", WorkerNodeUID: "rebase-node-uid",
	})
	require.NoError(t, err)
	require.Equal(t, preoperation.TargetGenerationID, retryPreoperation.TargetGenerationID)
	require.Equal(t, preoperation.TargetWriterEpoch, retryPreoperation.TargetWriterEpoch)
	targetHead := digest.FromString("rebase-target-head").String()
	target := &RootFSGeneration{
		ID: preoperation.TargetGenerationID, FilesystemID: filesystem.ID,
		ParentGenerationID: source.ID, SourceOCIDigest: newArtifact.SourceOCIDigest,
		BaseArtifactDigest: newArtifact.ArtifactDigest, BaseBlockRoot: newArtifact.BaseBlockRoot,
		CurrentBlockHead: targetHead, WriterEpoch: preoperation.TargetWriterEpoch,
		FormatGeneration: newArtifact.FormatGeneration,
		DurabilityState:  RootFSGenerationStateS3Materialized,
		LocatorVersion:   source.LocatorVersion + 1,
		Descriptor:       encodeTestRootFSDescriptor(t, "rebase-target-v2", targetHead),
	}
	health := sha256.Sum256([]byte("rebase-worker-health-proof"))
	workerProof := sha256.Sum256([]byte("rebase-worker-output-proof"))
	rebaseRequest := &PublishPausedRootFSRebaseRequest{
		SandboxID: record.ID, TeamID: record.TeamID, OperationID: "rebase-v2",
		ExpectedSourceGenerationID: source.ID,
		ExpectedBaseArtifactDigest: oldArtifact.ArtifactDigest,
		Generation:                 target, HealthCheckDigest: health[:],
		RollbackExpiresAt: rollbackExpiresAt,
		WorkerClusterID:   record.ClusterID, WorkerNodeID: "rebase-node", WorkerNodeUID: "rebase-node-uid",
		WorkerProofDigest: workerProof[:],
	}
	published, err := store.PublishPausedRootFSRebase(ctx, rebaseRequest)
	require.NoError(t, err)
	require.Equal(t, target.ID, published.HeadGenerationID)
	require.Equal(t, newArtifact.ArtifactDigest, published.BaseArtifactDigest)
	require.Equal(t, target.WriterEpoch, published.WriterEpoch)
	var storedHealth []byte
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT health_check_digest
		FROM manager.rootfs_head_rollbacks
		WHERE operation_id = $1
	`, rebaseRequest.OperationID).Scan(&storedHealth))
	require.Equal(t, health[:], storedHealth)
	retried, err := store.PublishPausedRootFSRebase(ctx, rebaseRequest)
	require.NoError(t, err)
	require.Equal(t, published.HeadGenerationID, retried.HeadGenerationID)
	completedPreoperation, err := store.RequestNomadPausedRebase(ctx, &NomadPausedRebaseRequest{
		OperationID: "rebase-v2", SandboxID: record.ID, ExpectedTeamID: record.TeamID,
		TargetBaseArtifactDigest: newArtifact.ArtifactDigest,
		RollbackExpiresAt:        rollbackExpiresAt,
		WorkerClusterID:          record.ClusterID, WorkerNodeID: "rebase-node", WorkerNodeUID: "rebase-node-uid",
	})
	require.NoError(t, err)
	require.True(t, completedPreoperation.Completed)
	require.Equal(t, SandboxLifecyclePhaseCommitted, completedPreoperation.LifecyclePhase)
	require.Equal(t, workerProof[:], completedPreoperation.WorkerProofDigest)
	require.True(t, completedPreoperation.WorkerAcknowledgedAt.IsZero())
	pending, err := store.ListPendingNomadPausedRebases(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, rebaseRequest.OperationID, pending[0].ID)
	wrongProof := sha256.Sum256([]byte("wrong-rebase-worker-output-proof"))
	err = store.AcknowledgeNomadPausedRebaseWorker(
		ctx, rebaseRequest.OperationID, record.ID, record.ClusterID,
		"rebase-node", "rebase-node-uid", wrongProof[:],
	)
	require.ErrorIs(t, err, ErrNomadSandboxRebaseConflict)
	require.NoError(t, store.AcknowledgeNomadPausedRebaseWorker(
		ctx, rebaseRequest.OperationID, record.ID, record.ClusterID,
		"rebase-node", "rebase-node-uid", workerProof[:],
	))
	require.NoError(t, store.AcknowledgeNomadPausedRebaseWorker(
		ctx, rebaseRequest.OperationID, record.ID, record.ClusterID,
		"rebase-node", "rebase-node-uid", workerProof[:],
	), "exact acknowledgement retries must remain idempotent")
	pending, err = store.ListPendingNomadPausedRebases(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, pending)
	acknowledged, err := store.RequestNomadPausedRebase(ctx, &NomadPausedRebaseRequest{
		OperationID: "rebase-v2", SandboxID: record.ID, ExpectedTeamID: record.TeamID,
		TargetBaseArtifactDigest: newArtifact.ArtifactDigest,
		RollbackExpiresAt:        rollbackExpiresAt,
		WorkerClusterID:          record.ClusterID, WorkerNodeID: "rebase-node", WorkerNodeUID: "rebase-node-uid",
	})
	require.NoError(t, err)
	require.False(t, acknowledged.WorkerAcknowledgedAt.IsZero())

	rolledBack, err := store.RollbackRootFSHead(ctx, &RollbackRootFSHeadRequest{
		SandboxID: record.ID, TeamID: record.TeamID, OperationID: rebaseRequest.OperationID,
	})
	require.NoError(t, err)
	require.Equal(t, source.ID, rolledBack.HeadGenerationID)
	require.Equal(t, oldArtifact.ArtifactDigest, rolledBack.BaseArtifactDigest)
	require.Equal(t, target.WriterEpoch, rolledBack.WriterEpoch,
		"rollback must not make a consumed rebase epoch reusable")
	snapshot, err := store.GetRootFSSnapshot(ctx, "snapshot-before-rebase", record.TeamID)
	require.NoError(t, err)
	require.Equal(t, source.ID, snapshot.HeadGenerationID)

	binding := bytes.Repeat([]byte{0x6b}, 32)
	issue := rootFSWriterGrantTestIssueRequest(record.ID, "grant-rebase-active", "claim-rebase-active", "slot-rebase-active", binding)
	issue.ExpectedFilesystemID = filesystem.ID
	issue.InitialGenerationID = source.ID
	issue.ExpectedWriterEpoch = rolledBack.WriterEpoch
	_, err = store.IssueRootFSWriterGrant(ctx, issue)
	require.NoError(t, err)
	_, err = store.RequestNomadPausedRebase(ctx, &NomadPausedRebaseRequest{
		OperationID: "rebase-active-writer", SandboxID: record.ID, ExpectedTeamID: record.TeamID,
		TargetBaseArtifactDigest: newArtifact.ArtifactDigest,
		RollbackExpiresAt:        time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond),
		WorkerClusterID:          record.ClusterID, WorkerNodeID: "rebase-node", WorkerNodeUID: "rebase-node-uid",
	})
	require.ErrorIs(t, err, ErrNomadSandboxRebaseNotReady)
}

func encodeTestRootFSDescriptor(t *testing.T, suffix, blockHead string) []byte {
	t.Helper()
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 1 << 30,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: blockHead,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/" + suffix + "/map.page", Length: 4096,
				Checksum: digest.FromString("map-page-" + suffix).String(),
			},
		},
	})
	require.NoError(t, err)
	return descriptor
}

func putTestDurableRootFSGeneration(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	filesystem *RootFSFilesystem,
	parent *RootFSGeneration,
	suffix string,
	writerEpoch int64,
) *RootFSGeneration {
	t.Helper()
	blockHead := digest.FromString("block-head-" + suffix).String()
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 1 << 30,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: blockHead,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/" + suffix + "/map.page", Length: 4096,
				Checksum: digest.FromString("map-page-" + suffix).String(),
			},
		},
	})
	require.NoError(t, err)
	generation := &RootFSGeneration{
		ID: "generation-" + suffix, FilesystemID: filesystem.ID,
		ParentGenerationID: parent.ID, SourceOCIDigest: parent.SourceOCIDigest,
		BaseArtifactDigest: parent.BaseArtifactDigest, BaseBlockRoot: parent.BaseBlockRoot,
		CurrentBlockHead: blockHead, WriterEpoch: writerEpoch,
		FormatGeneration: parent.FormatGeneration,
		DurabilityState:  RootFSGenerationStateS3Materialized,
		LocatorVersion:   parent.LocatorVersion + 1, Descriptor: descriptor,
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_generations (
			generation_id, filesystem_id, parent_generation_id, source_oci_digest,
			base_artifact_digest, base_block_root, current_block_head, writer_epoch,
			format_generation, durability_state, locator_version, descriptor, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
	`, generation.ID, generation.FilesystemID, generation.ParentGenerationID,
		generation.SourceOCIDigest, generation.BaseArtifactDigest, generation.BaseBlockRoot,
		generation.CurrentBlockHead, generation.WriterEpoch, generation.FormatGeneration,
		generation.DurabilityState, generation.LocatorVersion, generation.Descriptor)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_filesystems
		SET head_generation_id = $1, writer_epoch = $2, updated_at = NOW()
		WHERE filesystem_id = $3
	`, generation.ID, generation.WriterEpoch, generation.FilesystemID)
	require.NoError(t, err)
	return generation
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
