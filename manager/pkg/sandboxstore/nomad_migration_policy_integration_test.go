package sandboxstore

import (
	"strings"
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadMigrationPreparationAtomicallyRetainsExactSourcePolicyIntegration(t *testing.T) {
	f, a := migrationStoreFixture(t, "policy-snapshot")
	migrationReadyTarget(t, f, "policy-snapshot", "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	policy := migrationSourcePolicy(f.sandboxID, a.Target.TeamID)
	for _, bad := range []string{"", `{}`, " " + policy, strings.ReplaceAll(policy, "allow-all", "block-all"), migrationSourcePolicy("foreign", a.Target.TeamID), migrationSourcePolicy(f.sandboxID, "foreign"), strings.Repeat("x", protocol.MaxNetworkPolicyBytes+1)} {
		_, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, bad)
		require.Error(t, err)
	}
	var untouched bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT preparation_request IS NULL AND source_network_policy IS NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, a.OperationID).Scan(&untouched))
	require.True(t, untouched)
	retainMigrationEligibilityFixture(t, f, a)
	first, err := f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
	require.NoError(t, err)
	var stored, digest string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT source_network_policy,source_network_policy_digest FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, a.OperationID).Scan(&stored, &digest))
	require.Equal(t, policy, stored)
	require.Equal(t, protocol.NetworkPolicyDigest(policy), digest)
	again, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
	require.NoError(t, err)
	require.Equal(t, first, again)
	_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy+" ")
	require.Error(t, err, "semantic equivalence cannot rewrite exact applied bytes")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET source_network_policy=NULL,source_network_policy_digest=NULL WHERE operation_id=$1`, a.OperationID)
	require.Error(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_network_policy_digest=$2 WHERE slot_id=$1`, f.slotID, protocol.NetworkPolicyDigest(policy+" "))
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationCapture(f.ctx, *first, preparedMigrationResponse(t, *first))
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "changed source policy cannot enter capture")
}

func TestNomadMigrationSourceFenceRejectsPolicyDriftIntegration(t *testing.T) {
	f, _, request, _ := migrationFenceStoreFixture(t, "policy-fence")
	_, err := f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET claim_network_policy_digest=$2 WHERE slot_id=$1`, f.slotID, protocol.NetworkPolicyDigest("changed"))
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, request)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	grant, err := f.store.GetRootFSWriterGrant(f.ctx, f.issue.GrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateConsumed, grant.State)
}
