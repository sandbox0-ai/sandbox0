package sandboxstore

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadSandboxNetworkMutationPublishesOnlyAfterExactAckIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "network-ack")
	slot, err := fixture.store.GetRuntimeSlot(fixture.ctx, fixture.slotID)
	require.NoError(t, err)
	desiredPolicy := `{"version":"v1","sandboxId":"` + fixture.sandboxID +
		`","teamId":"team-slot","mode":"block-all"}`
	request := nomadNetworkMutationRequest(fixture, slot, "network-operation-ack", desiredPolicy)

	mutation, err := fixture.store.BeginNomadSandboxNetworkMutation(fixture.ctx, request)
	require.NoError(t, err)
	require.Equal(t, NomadSandboxNetworkMutationPhasePending, mutation.Phase)
	require.Equal(t, slot.Revision, mutation.SlotRevision)
	before, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Nil(t, before.Config.Network, "desired config was published before node acknowledgement")

	retry, err := fixture.store.BeginNomadSandboxNetworkMutation(fixture.ctx, request)
	require.NoError(t, err)
	require.Equal(t, mutation.OperationID, retry.OperationID)
	changed := *request
	changed.OperationID = "network-operation-other"
	_, err = fixture.store.BeginNomadSandboxNetworkMutation(fixture.ctx, &changed)
	require.ErrorIs(t, err, ErrNomadSandboxNetworkMutationConflict)

	invalidToken := nomadNetworkMutationToken(slot, mutation)
	invalidToken.PolicyDigest = protocol.NetworkPolicyDigest(`{"different":true}`)
	_, err = fixture.store.CommitNomadSandboxNetworkMutation(
		fixture.ctx, fixture.sandboxID, mutation.OperationID, invalidToken,
	)
	require.ErrorIs(t, err, ErrNomadSandboxNetworkMutationConflict)
	pending, err := fixture.store.GetNomadSandboxNetworkMutation(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, NomadSandboxNetworkMutationPhasePending, pending.Phase)

	token := nomadNetworkMutationToken(slot, mutation)
	applied, err := fixture.store.CommitNomadSandboxNetworkMutation(
		fixture.ctx, fixture.sandboxID, mutation.OperationID, token,
	)
	require.NoError(t, err)
	require.Equal(t, NomadSandboxNetworkMutationPhaseApplied, applied.Phase)
	require.NotNil(t, applied.AppliedPolicyToken)
	require.NotEmpty(t, applied.AppliedTokenDigest)
	after, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.NotNil(t, after.Config.Network)
	require.Equal(t, v1alpha1.NetworkModeBlockAll, after.Config.Network.Mode)
	updatedSlot, err := fixture.store.GetRuntimeSlot(fixture.ctx, fixture.slotID)
	require.NoError(t, err)
	require.Equal(t, mutation.DesiredPolicyDigest, updatedSlot.ClaimNetworkPolicyDigest)
	require.Equal(t, slot.Revision+1, updatedSlot.Revision)

	replayed, err := fixture.store.CommitNomadSandboxNetworkMutation(
		fixture.ctx, fixture.sandboxID, mutation.OperationID, token,
	)
	require.NoError(t, err)
	require.Equal(t, applied.AppliedTokenDigest, replayed.AppliedTokenDigest)
	updatedSlotAgain, err := fixture.store.GetRuntimeSlot(fixture.ctx, fixture.slotID)
	require.NoError(t, err)
	require.Equal(t, updatedSlot.Revision, updatedSlotAgain.Revision)
}

func TestNomadSandboxNetworkMutationLifecyclePreemptsPendingApplyIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "network-preempt")
	slot, err := fixture.store.GetRuntimeSlot(fixture.ctx, fixture.slotID)
	require.NoError(t, err)
	desiredPolicy := `{"version":"v1","sandboxId":"` + fixture.sandboxID +
		`","teamId":"team-slot","mode":"allow-all"}`
	mutation, err := fixture.store.BeginNomadSandboxNetworkMutation(
		fixture.ctx,
		nomadNetworkMutationRequest(fixture, slot, "network-operation-preempt", desiredPolicy),
	)
	require.NoError(t, err)

	_, err = fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	canceled, err := fixture.store.PrepareNomadSandboxNetworkMutation(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, mutation.OperationID, canceled.OperationID)
	require.Equal(t, NomadSandboxNetworkMutationPhaseCanceled, canceled.Phase)
	require.Contains(t, canceled.CancellationReason, "lifecycle")

	_, err = fixture.store.CommitNomadSandboxNetworkMutation(
		fixture.ctx, fixture.sandboxID, mutation.OperationID, nomadNetworkMutationToken(slot, mutation),
	)
	require.ErrorIs(t, err, ErrNomadSandboxNetworkMutationConflict)
	record, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Nil(t, record.Config.Network)
	pending, err := fixture.store.ListPendingNomadSandboxNetworkMutations(fixture.ctx, 10)
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestNomadSandboxNetworkMutationCancelsStaleSlotRevisionIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "network-stale")
	slot, err := fixture.store.GetRuntimeSlot(fixture.ctx, fixture.slotID)
	require.NoError(t, err)
	desiredPolicy := `{"version":"v1","sandboxId":"` + fixture.sandboxID +
		`","teamId":"team-slot","mode":"allow-all"}`
	mutation, err := fixture.store.BeginNomadSandboxNetworkMutation(
		fixture.ctx,
		nomadNetworkMutationRequest(fixture, slot, "network-operation-stale", desiredPolicy),
	)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.runtime_slots SET revision = revision + 1 WHERE slot_id = $1
	`, fixture.slotID)
	require.NoError(t, err)

	canceled, err := fixture.store.PrepareNomadSandboxNetworkMutation(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, mutation.OperationID, canceled.OperationID)
	require.Equal(t, NomadSandboxNetworkMutationPhaseCanceled, canceled.Phase)
	require.Contains(t, canceled.CancellationReason, "authority changed")
}

func TestNomadSandboxNetworkMutationHardDeletePreemptsPendingApplyIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "network-hard-delete")
	slot, err := fixture.store.GetRuntimeSlot(fixture.ctx, fixture.slotID)
	require.NoError(t, err)
	desiredPolicy := `{"version":"v1","sandboxId":"` + fixture.sandboxID +
		`","teamId":"team-slot","mode":"block-all"}`
	mutation, err := fixture.store.BeginNomadSandboxNetworkMutation(
		fixture.ctx,
		nomadNetworkMutationRequest(fixture, slot, "network-operation-hard-delete", desiredPolicy),
	)
	require.NoError(t, err)
	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.sandboxes
		SET hard_expires_at = NOW() - INTERVAL '1 second'
		WHERE sandbox_id = $1
	`, fixture.sandboxID)
	require.NoError(t, err)

	_, err = fixture.store.RequestHardExpiredSandboxRuntimeClaimCleanup(
		fixture.ctx, fixture.sandboxID, "sandbox hard TTL expired",
	)
	require.NoError(t, err)
	canceled, err := fixture.store.GetNomadSandboxNetworkMutation(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, mutation.OperationID, canceled.OperationID)
	require.Equal(t, NomadSandboxNetworkMutationPhaseCanceled, canceled.Phase)
	require.Contains(t, canceled.CancellationReason, "termination")
	record, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateTerminating, record.DesiredState)
	require.Nil(t, record.Config.Network)
}

func TestNomadSandboxNetworkMutationValidationIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "network-validation")
	slot, err := fixture.store.GetRuntimeSlot(fixture.ctx, fixture.slotID)
	require.NoError(t, err)
	request := nomadNetworkMutationRequest(
		fixture, slot, "network-operation-validation",
		`{"version":"v1","sandboxId":"`+fixture.sandboxID+`","teamId":"team-slot","mode":"allow-all"}`,
	)
	wrongDigest := *request
	wrongDigest.DesiredPolicyDigest = protocol.NetworkPolicyDigest(`{"different":true}`)
	_, err = fixture.store.BeginNomadSandboxNetworkMutation(context.Background(), &wrongDigest)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrNomadSandboxNetworkMutationConflict))
	_, err = fixture.store.ListPendingNomadSandboxNetworkMutations(fixture.ctx, 0)
	require.Error(t, err)
}

func nomadNetworkMutationRequest(
	fixture *nomadPauseStoreFixture,
	slot *RuntimeSlot,
	operationID, desiredPolicy string,
) *BeginNomadSandboxNetworkMutationRequest {
	return &BeginNomadSandboxNetworkMutationRequest{
		SandboxID: fixture.sandboxID, OperationID: operationID, ExpectedTeamID: "team-slot",
		ExpectedCurrentPolicyDigest: slot.ClaimNetworkPolicyDigest,
		DesiredPolicy:               desiredPolicy, DesiredPolicyDigest: protocol.NetworkPolicyDigest(desiredPolicy),
		RequestPolicy: &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeBlockAll},
	}
}

func nomadNetworkMutationToken(
	slot *RuntimeSlot,
	mutation *NomadSandboxNetworkMutation,
) rootfshandoff.NetworkPolicyToken {
	return rootfshandoff.NetworkPolicyToken{
		PodUID: slot.AllocationID,
		PodSandboxID: protocol.RuntimeSlotNetworkIncarnationID(protocol.NodeNetworkPrepareControlRequest{
			SlotID: slot.ID, ClusterID: slot.ClusterID, AllocationID: slot.AllocationID,
			NodeID: slot.NodeID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
			NetNSIdentity: slot.NetNSIdentity,
		}),
		ClaimID: mutation.ClaimID, NetworkEpoch: 2,
		PolicyDigest: mutation.DesiredPolicyDigest, PodIP: "192.0.2.2",
		CtldGeneration: "ctld-generation-2", NetNSIdentity: slot.NetNSIdentity,
	}
}
