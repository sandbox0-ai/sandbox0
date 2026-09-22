package runtimeslot

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrationAdoptionChannelBindsReadyProofAndTarget(t *testing.T) {
	ready := CommandReadyControlRequest{Proof: testNodeChannelCommandProof()}
	digest, err := ready.Proof.Digest()
	require.NoError(t, err)
	request := MigrationAdoptionRequest{Target: testNodeChannelTarget(true), OperationID: ready.Proof.OperationID, ClaimID: ready.Proof.ClaimID,
		SandboxID: "sandbox-1", RuntimeGeneration: 2, ProcdInstanceID: ready.Proof.ProcdInstanceID, RestoreDigest: strings.Repeat("a", 64), CommandReadyDigest: digest}
	command, err := NewNodeChannelCommandReadyCommand(request.Target, ready)
	require.NoError(t, err)
	for name, mutate := range map[string]func(*MigrationAdoptionRequest){
		"matching": func(*MigrationAdoptionRequest) {},
		"boot":     func(r *MigrationAdoptionRequest) { r.Target.NodeBootID = "other-boot" },
		"probe":    func(r *MigrationAdoptionRequest) { r.CommandReadyDigest = strings.Repeat("c", 64) },
		"claim":    func(r *MigrationAdoptionRequest) { r.ClaimID = "other-claim" },
		"process":  func(r *MigrationAdoptionRequest) { r.ProcdInstanceID = "other-procd" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			digest, err := changed.Digest()
			require.NoError(t, err)
			response := NodeControlResponse{Phase: string(StateActive), MigrationAdoption: &MigrationAdoptionReceipt{Request: changed, Proof: MigrationAdoptionProof{RequestDigest: digest, ImageAbsent: true}}}
			result := NodeChannelResult{Version: NodeChannelVersion, RequestID: command.RequestID, Kind: command.Kind, ControlResponse: &response}
			if name == "matching" {
				require.NoError(t, result.ValidateFor(command))
			} else {
				require.Error(t, result.ValidateFor(command))
			}
			require.Error(t, response.ValidateClaimResult(testNodeChannelClaim()), "claim cannot carry adoption authority")
			response.MigrationAdoption.Proof.ImageAbsent = false
			require.Error(t, result.ValidateFor(command))
		})
	}
}
