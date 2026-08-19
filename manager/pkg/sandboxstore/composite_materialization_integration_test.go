package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
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

	request := &RootFSGenerationMaterialization{
		GenerationID: composite.ID, ExpectedLocatorVersion: composite.LocatorVersion,
		ExpectedDescriptor: composite.Descriptor, MaterializedDescriptor: initial.Descriptor,
	}
	require.NoError(t, store.PublishRootFSGenerationMaterialization(ctx, request))
	require.NoError(t, store.PublishRootFSGenerationMaterialization(ctx, request), "exact response-loss retry")
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
	changed.MaterializedDescriptor = append([]byte(nil), initial.Descriptor...)
	changed.MaterializedDescriptor[len(changed.MaterializedDescriptor)-1] ^= 1
	require.Error(t, store.PublishRootFSGenerationMaterialization(ctx, &changed))
	require.Error(t, store.SetRootFSCompositeBacklogLimit(ctx, 0))
	_, err = store.ListCompositeRootFSGenerations(ctx, 0)
	require.Error(t, err)
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
