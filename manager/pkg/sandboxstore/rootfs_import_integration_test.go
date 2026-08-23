package sandboxstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"

	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

func TestRootFSImportMigrationDownAndUpIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPoolThrough(t, "00043")
	assertRootFSImportSchema(t, ctx, pool, false)
	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
	assertRootFSImportSchema(t, ctx, pool, true)
	require.NoError(t, migrate.Down(ctx, pool, ".",
		migrate.WithBaseFS(storemigrations.FS),
		migrate.WithLogger(noopSandboxStoreMigrateLogger{}),
		migrate.WithSchema(sandboxStoreSchemaName),
	))
	assertRootFSImportSchema(t, ctx, pool, false)
	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
	assertRootFSImportSchema(t, ctx, pool, true)
}

func TestRootFSImportOperationLeaseJournalAndReadyCAS(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	begin, result, reference := rootFSImportTestFixture(t, "ready-cas")

	operation, err := store.BeginRootFSImport(ctx, begin)
	require.NoError(t, err)
	require.Equal(t, RootFSImportStatePending, operation.State)
	require.Equal(t, rootfsblock.DefaultPackBytes, operation.Spec.BlockOptions.PackBytes)
	retry, err := store.BeginRootFSImport(ctx, begin)
	require.NoError(t, err)
	require.Equal(t, operation.ID, retry.ID)
	changed := *begin
	changed.Spec = begin.Spec
	changed.Spec.ProcdProtocol = "sandbox0.procd.changed"
	_, err = store.BeginRootFSImport(ctx, &changed)
	require.ErrorIs(t, err, ErrRootFSImportConflict)

	leased, err := store.LeaseNextRootFSImport(ctx, "manager-a", time.Minute)
	require.NoError(t, err)
	require.Equal(t, operation.ID, leased.ID)
	require.Equal(t, 1, leased.AttemptCount)
	lease, err := leased.Lease()
	require.NoError(t, err)
	journal, err := NewRootFSImportPublicationJournal(store, lease)
	require.NoError(t, err)
	require.NoError(t, journal.PrepareObject(ctx, operation.ID, reference))
	require.NoError(t, journal.MarkObjectPublished(ctx, operation.ID, reference))

	artifact, err := store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{
		Lease: lease, Result: result,
	})
	require.NoError(t, err)
	require.NotEmpty(t, artifact.Attestation)
	require.Equal(t, result.ProcdDigest.String(), artifact.ProcdDigest)
	require.Equal(t, begin.Spec.ProcdProtocol, artifact.ProcdProtocol)
	require.Equal(t, result.DescriptorDigest.String(), artifact.DescriptorDigest)
	require.Equal(t, result.LogicalSizeBytes, artifact.LogicalSizeBytes)

	// Commit-response loss uses the original lease identity and exact result.
	replayed, err := store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{
		Lease: lease, Result: result,
	})
	require.NoError(t, err)
	require.Equal(t, artifact.ArtifactDigest, replayed.ArtifactDigest)
	operation, err = store.GetRootFSImportOperation(ctx, operation.ID)
	require.NoError(t, err)
	require.Equal(t, RootFSImportStateReady, operation.State)
	require.Equal(t, artifact.ArtifactDigest, operation.ArtifactDigest)

	result.ManifestDigest = result.ConfigDigest
	_, err = store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{
		Lease: lease, Result: result,
	})
	require.ErrorIs(t, err, ErrRootFSImportConflict)
}

func TestRootFSImportExpiredLeaseSelfFencesAndRecovers(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	begin, _, reference := rootFSImportTestFixture(t, "lease-recovery")
	require.NoError(t, func() error {
		_, err := store.BeginRootFSImport(ctx, begin)
		return err
	}())
	first, err := store.LeaseNextRootFSImport(ctx, "manager-old", time.Minute)
	require.NoError(t, err)
	firstLease, err := first.Lease()
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_import_operations
		SET lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE operation_id = $1
	`, first.ID)
	require.NoError(t, err)
	err = store.PrepareRootFSImportObject(ctx, firstLease, reference)
	require.ErrorIs(t, err, ErrRootFSImportLeaseLost)

	second, err := store.LeaseNextRootFSImport(ctx, "manager-new", time.Minute)
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)
	require.Equal(t, 2, second.AttemptCount)
	require.NotEqual(t, first.LeaseToken, second.LeaseToken)
	secondLease, err := second.Lease()
	require.NoError(t, err)
	require.NoError(t, store.PrepareRootFSImportObject(ctx, secondLease, reference))
	require.NoError(t, store.MarkRootFSImportObjectPublished(ctx, secondLease, reference))
	require.NoError(t, store.ReleaseRootFSImportLease(ctx, secondLease))

	listed, err := store.ListRootFSImportOperations(ctx, []string{RootFSImportStatePending}, 10)
	require.NoError(t, err)
	require.Len(t, listed, 1)
}

func TestRootFSImportAbandonAndTerminalGarbageReleaseObjects(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	begin, _, reference := rootFSImportTestFixture(t, "abandon-gc")
	_, err := store.BeginRootFSImport(ctx, begin)
	require.NoError(t, err)
	operation, err := store.LeaseNextRootFSImport(ctx, "manager-gc", time.Minute)
	require.NoError(t, err)
	lease, err := operation.Lease()
	require.NoError(t, err)
	require.NoError(t, store.PrepareRootFSImportObject(ctx, lease, reference))
	require.NoError(t, store.MarkRootFSImportObjectPublished(ctx, lease, reference))
	require.NoError(t, store.AbandonRootFSImport(ctx, lease, "permanent OCI policy rejection"))
	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_import_operations
		SET updated_at = NOW() - INTERVAL '1 hour'
		WHERE operation_id = $1
	`, operation.ID)
	require.NoError(t, err)

	garbage, err := store.ReconcileRootFSImportGarbage(ctx, time.Minute, 10)
	require.NoError(t, err)
	require.Equal(t, 1, garbage.PurgedAbandoned)
	require.Equal(t, 1, garbage.EnqueuedObjects)
	var queued bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM manager.rootfs_object_deletions WHERE object_key = $1
		)
	`, reference.Key).Scan(&queued))
	require.True(t, queued)
	_, err = store.GetRootFSImportOperation(ctx, operation.ID)
	require.ErrorIs(t, err, ErrRootFSImportNotFound)
}

func TestRootFSImportReadyGarbageRetainsArtifactObjects(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	begin, result, reference := rootFSImportTestFixture(t, "ready-gc")
	_, err := store.BeginRootFSImport(ctx, begin)
	require.NoError(t, err)
	operation, err := store.LeaseNextRootFSImport(ctx, "manager-ready-gc", time.Minute)
	require.NoError(t, err)
	lease, err := operation.Lease()
	require.NoError(t, err)
	require.NoError(t, store.PrepareRootFSImportObject(ctx, lease, reference))
	require.NoError(t, store.MarkRootFSImportObjectPublished(ctx, lease, reference))
	extraPayload := []byte("published by a crashed non-deterministic XFS build")
	extraDigest := digest.FromBytes(extraPayload)
	extraReference := rootfsblock.ObjectReference{
		Key:  begin.Spec.BlockOptions.ObjectPrefix + "/packs/sha256/" + extraDigest.Encoded(),
		Kind: rootfsblock.ObjectKindDataPack, Size: int64(len(extraPayload)), Checksum: extraDigest.String(),
	}
	require.NoError(t, store.PrepareRootFSImportObject(ctx, lease, extraReference))
	require.NoError(t, store.MarkRootFSImportObjectPublished(ctx, lease, extraReference))
	artifact, err := store.PublishReadyRootFSImport(ctx, &PublishReadyRootFSImportRequest{Lease: lease, Result: result})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_import_operations
		SET updated_at = NOW() - INTERVAL '1 hour'
		WHERE operation_id = $1
	`, operation.ID)
	require.NoError(t, err)

	garbage, err := store.ReconcileRootFSImportGarbage(ctx, time.Minute, 10)
	require.NoError(t, err)
	require.Equal(t, 1, garbage.PurgedReady)
	require.Equal(t, 1, garbage.EnqueuedObjects)
	var objectExists, edgeExists, extraQueued bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM manager.rootfs_materialization_objects WHERE object_key = $1),
			EXISTS (
				SELECT 1 FROM manager.rootfs_base_artifact_objects
				WHERE artifact_digest = $2 AND object_key = $1
			),
			EXISTS (SELECT 1 FROM manager.rootfs_object_deletions WHERE object_key = $3)
	`, reference.Key, artifact.ArtifactDigest, extraReference.Key).Scan(&objectExists, &edgeExists, &extraQueued))
	require.True(t, objectExists)
	require.True(t, edgeExists)
	require.True(t, extraQueued)
}

func TestRootFSImportJournalRejectsOperationMismatch(t *testing.T) {
	journal := &RootFSImportPublicationJournal{
		lease: RootFSImportLease{OperationID: "rootfs-import-one", Token: strings.Repeat("a", 64)},
	}
	err := journal.PrepareObject(context.Background(), "rootfs-import-two", rootfsblock.ObjectReference{})
	require.ErrorContains(t, err, "does not match")
}

func assertRootFSImportSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want bool) {
	t.Helper()
	var operationTable, attestationColumn bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT to_regclass('manager.rootfs_import_operations') IS NOT NULL,
			EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'manager' AND table_name = 'rootfs_base_artifacts'
					AND column_name = 'attestation'
			)
	`).Scan(&operationTable, &attestationColumn))
	require.Equal(t, want, operationTable)
	require.Equal(t, want, attestationColumn)
}
