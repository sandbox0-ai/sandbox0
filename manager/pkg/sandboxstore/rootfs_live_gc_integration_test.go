package sandboxstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRootFSFilesystemGCReleasesInternalLiveDependenciesIntegration(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprintf("external_reader_%t", external), func(t *testing.T) {
			f, objects, slot, cut := fencedProviderFixture(t)
			ctx := t.Context()
			_, err := f.store.CompleteRuntimeSlotProviderAbandon(ctx, providerCompletionRequest(t, f, slot, confirmedProviderProof(t, f, slot)))
			require.NoError(t, err)
			for {
				completed, err := f.store.InventoryRootFSGeneration(ctx, objects, 100)
				require.NoError(t, err)
				if !completed {
					break
				}
			}
			reader := rootFSTestSandboxRecord("external-reader", "team-a")
			require.NoError(t, f.store.UpsertSandbox(ctx, reader))
			_, dependent, err := f.store.EnsureInitialRootFSGeneration(ctx, &EnsureInitialRootFSGenerationRequest{
				SandboxID: reader.ID, TeamID: reader.TeamID, SourceOCIRef: f.filesystem.BaseImageRef,
				SourceOCIDigest: f.initial.SourceOCIDigest, BaseArtifactDigest: f.initial.BaseArtifactDigest,
			})
			require.NoError(t, err)
			_, err = f.pool.Exec(ctx, `INSERT INTO manager.rootfs_generation_live_dependencies(generation_id,base_generation_id)
				VALUES($1,$2),($3,$1) ON CONFLICT DO NOTHING`, cut.Generation.ID, f.initial.ID, dependent.ID)
			require.NoError(t, err)
			_, err = f.store.RequestSandboxRuntimeClaimCleanup(ctx, f.sandboxID, "delete source retained by independent reader")
			require.NoError(t, err)
			require.NoError(t, f.store.MarkSandboxDeleted(ctx, f.sandboxID, time.Now()))
			var before int
			require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM manager.rootfs_generations WHERE filesystem_id=$1`, f.filesystem.ID).Scan(&before))
			require.Positive(t, before, "direct deletion must preserve the reader's base")
			if !external {
				require.NoError(t, f.store.MarkSandboxDeleted(ctx, reader.ID, time.Now()))
			}
			deleted, err := f.store.DeleteUnreferencedRootFSFilesystems(ctx, "team-a", 100)
			require.NoError(t, err, "background GC must handle live dependencies after direct deletion deferred cleanup")
			var remaining, dependencies int
			require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM manager.rootfs_generations WHERE filesystem_id=$1`, f.filesystem.ID).Scan(&remaining))
			if external {
				require.Zero(t, deleted)
				require.Equal(t, before, remaining)
				require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM manager.rootfs_generation_live_dependencies WHERE generation_id=$1 AND base_generation_id=$2`, dependent.ID, cut.Generation.ID).Scan(&dependencies))
				require.Equal(t, 1, dependencies, "a live independent reader must retain its exact base")
			} else {
				require.Equal(t, 1, deleted)
				require.Zero(t, remaining)
				require.NoError(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM manager.rootfs_generation_live_dependencies`).Scan(&dependencies))
				require.Zero(t, dependencies)
			}
		})
	}
}
