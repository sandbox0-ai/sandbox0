package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type imageGCCommitLoss struct {
	*PGSandboxStore
	fail bool
}

func (s *imageGCCommitLoss) CompleteNomadMigrationImageGC(ctx context.Context, b runtimecheckpoint.Binding) error {
	if s.fail {
		s.fail = false
		return errors.New("injected completion reply loss")
	}
	return s.PGSandboxStore.CompleteNomadMigrationImageGC(ctx, b)
}

func TestNomadMigrationImageGCRequiresPhysicalCustodyAndRetriesIntegration(t *testing.T) {
	f, adoption, adopted := migrationFinalizationStoreFixture(t, "image-gc")
	id := adoption.OperationID
	blocked := func() {
		t.Helper()
		b, err := f.store.AuthorizeNomadMigrationImageGC(f.ctx, id)
		require.NoError(t, err)
		require.Nil(t, b)
		_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET image_gc_binding_digest=publication_receipt->'reference'->>'binding_digest' WHERE operation_id=$1`, id)
		require.Error(t, err)
	}
	blocked()
	require.NoError(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, adoption, adopted))
	blocked()
	command, err := f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, id)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFinalization(f.ctx, *command, migrationFinalizationStoreProof(t, *command)))
	blocked()
	source, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotAllocationMissing(f.ctx, &MarkRuntimeSlotAllocationMissingRequest{SlotID: source.ID, AllocationID: source.AllocationID, NodeUID: source.NodeUID, NodeBootID: source.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0x85}, 32)})
	require.NoError(t, err)
	_, err = f.store.CompleteNomadSandboxMigrationSource(f.ctx, id)
	require.NoError(t, err)
	blocked()
	for range 2 {
		r, err := f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, id)
		require.NoError(t, err)
		require.NotNil(t, r)
		d, err := r.Digest()
		require.NoError(t, err)
		require.NoError(t, f.store.CommitNomadSandboxMigrationStagingRelease(f.ctx, *r, protocol.MigrationStagingReleased{RequestDigest: d}))
	}
	var wg sync.WaitGroup
	bindings := make([]*runtimecheckpoint.Binding, 4)
	errs := make([]error, 4)
	for i := range bindings {
		wg.Go(func() { bindings[i], errs[i] = NewPGSandboxStore(f.pool).AuthorizeNomadMigrationImageGC(f.ctx, id) })
	}
	wg.Wait()
	for i := range bindings {
		require.NoError(t, errs[i])
		require.NotNil(t, bindings[i])
		require.Equal(t, bindings[0], bindings[i])
	}
	b := *bindings[0]
	d, err := b.Digest()
	require.NoError(t, err)
	wrong := b
	wrong.TeamID = "another-team"
	require.ErrorIs(t, f.store.CompleteNomadMigrationImageGC(f.ctx, wrong), ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET image_gc_binding_digest=NULL WHERE operation_id=$1`, id)
	require.Error(t, err)
	objects := objectstore.NewMemoryStore("")
	key := "runtime-checkpoints/v1/" + strings.TrimPrefix(d, "sha256:") + "/manifest.json"
	require.NoError(t, objects.Put(key, strings.NewReader("test image")))
	collector, err := runtimecheckpoint.NewCollector(objects)
	require.NoError(t, err)
	store := &imageGCCommitLoss{PGSandboxStore: f.store, fail: true}
	worker, err := nomadmigration.NewImageGC(store, collector)
	require.NoError(t, err)
	_, err = worker.RunOnce(f.ctx)
	require.NoError(t, err)
	_, err = worker.RunOnce(f.ctx)
	require.ErrorContains(t, err, "injected")
	worker, err = nomadmigration.NewImageGC(NewPGSandboxStore(f.pool), collector)
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
	require.NoError(t, f.store.CompleteNomadMigrationImageGC(f.ctx, b))
	ids, err := f.store.ListNomadMigrationImageGC(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET image_gc_completed_at=NULL WHERE operation_id=$1`, id)
	require.Error(t, err)
	target, err := f.store.GetRuntimeSlot(f.ctx, adoption.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeResourceLeaseActive, target.ResourceLeaseState)
}
