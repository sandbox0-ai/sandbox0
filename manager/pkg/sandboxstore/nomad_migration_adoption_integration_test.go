package sandboxstore

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadMigrationAdoptionRequiresAtomicGenerationAndRetainsLeasesIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "adoption-commit")
	handover, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationHandover(f.ctx, *handover, migrationHandoverResponse(t, *handover)))
	var pending []byte
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT adoption_request FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, ready.OperationID).Scan(&pending))
	require.Empty(t, pending)
	command, err := f.store.GetNomadSandboxMigrationAdoptionForSlot(f.ctx, ready.SlotID)
	require.NoError(t, err)
	require.Nil(t, command, "handover alone cannot authorize adoption")

	premature := protocol.MigrationAdoptionRequest{Target: restored.Request.Image.Target, OperationID: ready.OperationID, ClaimID: ready.ClaimID,
		SandboxID: f.sandboxID, RuntimeGeneration: restored.Request.Image.Publication.Assignment.Target.RuntimeGeneration,
		ProcdInstanceID: ready.ProcdInstanceID, RestoreDigest: restored.RequestDigest, CommandReadyDigest: hex.EncodeToString(ready.CommandReadyDigest)}
	require.NoError(t, premature.ValidateFor(restored))
	prematureDigest, err := premature.Digest()
	require.NoError(t, err)
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, premature, protocol.MigrationAdoptionProof{RequestDigest: prematureDigest, ImageAbsent: true}), ErrNomadSandboxMigrationConflict)
	slot, err := f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.NoError(t, err)
	require.NotNil(t, slot.MigrationAdoption)
	request := *slot.MigrationAdoption
	command, err = f.store.GetNomadSandboxMigrationAdoptionForSlot(f.ctx, ready.SlotID)
	require.NoError(t, err)
	require.Equal(t, &request, command)
	sourceCommand, err := f.store.GetNomadSandboxMigrationAdoptionForSlot(f.ctx, restored.Request.Image.Publication.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, sourceCommand, "source slot cannot read destination authority")

	require.Equal(t, premature, request)
	require.NoError(t, request.ValidateFor(restored))
	digest, err := request.Digest()
	require.NoError(t, err)
	proof := protocol.MigrationAdoptionProof{RequestDigest: digest, ImageAbsent: true}
	changed := request
	changed.CommandReadyDigest = strings.Repeat("c", 64)
	changedDigest, err := changed.Digest()
	require.NoError(t, err)
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, changed, protocol.MigrationAdoptionProof{RequestDigest: changedDigest, ImageAbsent: true}), ErrNomadSandboxMigrationConflict)
	invalid := proof
	invalid.ImageAbsent = false
	require.ErrorIs(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, request, invalid), ErrNomadSandboxMigrationConflict)
	// Cleanup facts can arrive after TTL or desired-state changes. They must
	// remain recordable without granting new execution or releasing capacity.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='terminating',hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	command, err = f.store.GetNomadSandboxMigrationAdoptionForSlot(f.ctx, ready.SlotID)
	require.NoError(t, err)
	require.Equal(t, &request, command, "historical command survives desired-state and TTL changes")
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Go(func() { errs[i] = NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationAdoption(f.ctx, request, proof) })
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	var payload []byte
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT adoption_receipt FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, request.OperationID).Scan(&payload))
	var actual protocol.MigrationAdoptionProof
	require.NoError(t, json.Unmarshal(payload, &actual))
	require.Equal(t, proof, actual)
	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&count))
	require.Equal(t, 2, count)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET adoption_receipt=NULL WHERE operation_id=$1`, request.OperationID)
	require.Error(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET adoption_request=NULL,adoption_digest=NULL WHERE operation_id=$1`, request.OperationID)
	require.Error(t, err)
}

func TestNomadMigrationAdoptionFailureRollsBackRoutingIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "adoption-rollback")
	handover, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationHandover(f.ctx, *handover, migrationHandoverResponse(t, *handover)))
	_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_adoption() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.adoption_request IS NOT NULL THEN RAISE EXCEPTION 'injected adoption failure'; END IF; RETURN NEW; END; $$;
        CREATE TRIGGER reject_test_adoption BEFORE UPDATE ON manager.sandbox_runtime_migrations FOR EACH ROW EXECUTE FUNCTION manager.reject_test_adoption()`)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.ErrorContains(t, err, "injected adoption failure")
	slot, err := f.store.GetRuntimeSlot(f.ctx, ready.SlotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateStarting, slot.State)
	visible, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, handover.Assignment.SourceGeneration, visible.RuntimeGeneration)
	var uncommitted bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT generation_committed_at IS NULL AND adoption_request IS NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, ready.OperationID).Scan(&uncommitted))
	require.True(t, uncommitted)
	command, err := f.store.GetNomadSandboxMigrationAdoptionForSlot(f.ctx, ready.SlotID)
	require.NoError(t, err)
	require.Nil(t, command, "rolled-back generation CAS cannot leak adoption authority")

}
