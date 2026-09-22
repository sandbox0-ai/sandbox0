package sandboxstore

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestRuntimeSlotClaimInputsRetainExactLaunchIntegration(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained", true: "legacy"}[legacy], func(t *testing.T) {
			ctx := context.Background()
			pool := newSandboxStoreIntegrationPool(t)
			store := NewPGSandboxStore(pool)
			fs, generation := runtimeSlotTestGeneration(t, store, "sandbox-slot", "claim-inputs")
			registration := runtimeSlotTestRegistration("slot-inputs", "allocation-inputs")
			_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
			require.NoError(t, err)
			proof := bytes.Repeat([]byte{0x91}, 32)
			_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{SlotID: registration.SlotID,
				AllocationID: registration.AllocationID, NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
				RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof, HeartbeatTTL: time.Minute})
			require.NoError(t, err)
			a := runtimecontrol.Assignment{SandboxID: "sandbox-slot", TeamID: "team-slot", RuntimeGeneration: 1, SecurityClass: "standard",
				EnvVars:         map[string]string{"MODE": "original", runtimecontrol.EnvSandboxID: "sandbox-slot"},
				EphemeralMounts: []runtimecontrol.EphemeralMount{{MountPath: "/work-cache", SizeBytes: 8 << 20}},
				Webhook:         &runtimecontrol.WebhookConfig{URL: "https://example.test/events", Secret: "test-fixture-only"}, ResetCopiedSessionState: true}
			payload, err := json.Marshal(a)
			require.NoError(t, err)
			revision, err := a.Revision()
			require.NoError(t, err)
			policy := migrationSourcePolicy(a.SandboxID, a.TeamID)
			r := &AcquireRuntimeSlotRequest{OperationID: "claim-inputs", ClaimID: "claim-inputs", SandboxID: a.SandboxID,
				FilesystemID: fs.ID, SourceGenerationID: generation.ID, ClusterID: registration.ClusterID,
				CompatibilityDigest: registration.CompatibilityDigest, RuntimeAssignmentRevision: revision,
				NetworkPolicyDigest: protocol.NetworkPolicyDigest(policy), ClaimTTL: time.Minute, Resources: runtimeSlotTestResources()}
			if !legacy {
				r.RuntimeAssignmentPayload, r.NetworkPolicy = string(payload), policy
			}
			slot, err := store.AcquireRuntimeSlot(ctx, r)
			require.NoError(t, err)
			require.Equal(t, registration.SlotID, slot.ID)
			_, err = NewPGSandboxStore(pool).AcquireRuntimeSlot(ctx, r)
			require.NoError(t, err)
			inputs, err := store.GetRuntimeSlotClaimInputs(ctx, slot.ID)
			require.NoError(t, err)
			if legacy {
				require.Nil(t, inputs)
				r.RuntimeAssignmentPayload, r.NetworkPolicy = string(payload), policy
				_, err = store.AcquireRuntimeSlot(ctx, r)
				require.ErrorIs(t, err, ErrRuntimeSlotConflict, "retry cannot backfill a launch payload")
				_, err = pool.Exec(ctx, `UPDATE manager.runtime_slots SET claim_runtime_assignment=$2 WHERE slot_id=$1`, slot.ID, string(payload))
				require.Error(t, err)
				return
			}
			require.Equal(t, a, inputs.Runtime)
			require.Equal(t, policy, inputs.NetworkPolicy)
			_, err = pool.Exec(ctx, `UPDATE manager.sandboxes SET config='{"envVars":{"MODE":"changed"}}'::jsonb WHERE sandbox_id=$1`, a.SandboxID)
			require.NoError(t, err)
			reloaded, err := NewPGSandboxStore(pool).GetRuntimeSlotClaimInputs(ctx, slot.ID)
			require.NoError(t, err)
			require.Equal(t, inputs, reloaded, "migration must retain the original launch config")
			a.EnvVars["MODE"] = "changed"
			changed, err := json.Marshal(a)
			require.NoError(t, err)
			changedRevision, err := a.Revision()
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `UPDATE manager.runtime_slots SET claim_runtime_assignment=$2,claim_runtime_assignment_revision=$3 WHERE slot_id=$1`, slot.ID, string(changed), changedRevision)
			require.Error(t, err, "even matching hashes cannot rewrite launch history")
			_, err = pool.Exec(ctx, `UPDATE manager.runtime_slots SET claim_runtime_assignment=NULL WHERE slot_id=$1`, slot.ID)
			require.Error(t, err)
			r.RuntimeAssignmentPayload = string(changed)
			_, err = store.AcquireRuntimeSlot(ctx, r)
			require.ErrorIs(t, err, ErrRuntimeSlotConflict)
			// A byte-different, semantically identical policy is not an ack.
			_, err = pool.Exec(ctx, `UPDATE manager.runtime_slots SET claim_network_policy=$2,claim_network_policy_digest=$3 WHERE slot_id=$1`, slot.ID, policy+" ", protocol.NetworkPolicyDigest(policy+" "))
			require.Error(t, err)
		})
	}
}
