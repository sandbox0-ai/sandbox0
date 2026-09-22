package runtimeslot

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func migrationProtocolRequest() MigrationCaptureRequest {
	return MigrationCaptureRequest{Target: NodeChannelTarget{SlotID: "slot", ClusterID: "cluster", AllocationID: "allocation", NodeID: "node", NodeUID: "uid", NodeBootID: "boot", ControlEndpoint: "unix:///private/ctld/control.sock"},
		OperationID: "migration", SandboxID: "sandbox", LifecycleEpoch: 2, SourceGeneration: 1,
		AssignmentRevision: strings.Repeat("ab", 32), BindingDigest: strings.Repeat("cd", 32), ResourceLeaseDigest: strings.Repeat("ef", 32), ProcdInstanceID: "instance"}
}

func TestMigrationCaptureIdentityBindsEverySourceAuthorityField(t *testing.T) {
	request := migrationProtocolRequest()
	digest, err := request.Digest()
	require.NoError(t, err)
	for _, mutate := range []func(*MigrationCaptureRequest){
		func(r *MigrationCaptureRequest) { r.Target.NodeBootID = "rebooted" },
		func(r *MigrationCaptureRequest) { r.Target.AllocationID = "replacement" },
		func(r *MigrationCaptureRequest) { r.LifecycleEpoch++ },
		func(r *MigrationCaptureRequest) { r.SourceGeneration++ },
		func(r *MigrationCaptureRequest) { r.ProcdInstanceID = "restarted" },
		func(r *MigrationCaptureRequest) { r.ResourceLeaseDigest = strings.Repeat("aa", 32) },
	} {
		changed := request
		mutate(&changed)
		other, err := changed.Digest()
		require.NoError(t, err)
		require.NotEqual(t, digest, other)
		require.Error(t, (MigrationCapture{Request: changed, RequestDigest: digest, State: MigrationCaptureComplete}).Validate())
	}
	for _, mutate := range []func(*MigrationCaptureRequest){
		func(r *MigrationCaptureRequest) { r.Target.ControlEndpoint = "http://remote/unsafe" },
		func(r *MigrationCaptureRequest) { r.BindingDigest = "../image" },
		func(r *MigrationCaptureRequest) { r.LifecycleEpoch = 0 },
		func(r *MigrationCaptureRequest) { r.SourceGeneration = 0 },
	} {
		changed := request
		mutate(&changed)
		require.Error(t, changed.Validate())
	}
}

func TestMigrationCaptureCannotSatisfyOrdinaryCommandReadiness(t *testing.T) {
	request := migrationProtocolRequest()
	digest, err := request.Digest()
	require.NoError(t, err)
	capture := &MigrationCapture{Request: request, RequestDigest: digest, State: MigrationCaptureComplete}
	require.NoError(t, capture.Validate())
	require.Error(t, (NodeControlResponse{Phase: string(StateActive), Migration: capture}).Validate())
	require.Error(t, (NodeControlResponse{Phase: "migrating", Migration: capture}).Validate())
}

func TestMigrationChannelRejectsMixedAuthorityAndCrossOperationReply(t *testing.T) {
	request := migrationProtocolRequest()
	command, err := NewNodeChannelMigrationCaptureCommand(request)
	require.NoError(t, err)
	digest, err := request.Digest()
	require.NoError(t, err)
	result := NodeChannelResult{Version: NodeChannelVersion, RequestID: command.RequestID, Kind: command.Kind,
		MigrationCapture: &MigrationCapture{Request: request, RequestDigest: digest, State: MigrationCaptureComplete}}
	require.NoError(t, result.ValidateFor(command))
	changed := request
	changed.LifecycleEpoch++
	changedDigest, err := changed.Digest()
	require.NoError(t, err)
	result.MigrationCapture = &MigrationCapture{Request: changed, RequestDigest: changedDigest, State: MigrationCaptureComplete}
	require.Error(t, result.ValidateFor(command))
	command.Cleanup = &NodeCleanupControlRequest{}
	_, err = sealNodeChannelCommand(command)
	require.Error(t, err)
	command.Cleanup = nil
	command.Target.NodeBootID = "another-boot"
	_, err = sealNodeChannelCommand(command)
	require.Error(t, err)
}

func TestMigrationRecoveryHasDistinctCommandAuthority(t *testing.T) {
	request := migrationProtocolRequest()
	recover, err := NewNodeChannelMigrationRecoverCommand(request)
	require.NoError(t, err)
	capture, err := NewNodeChannelMigrationCaptureCommand(request)
	require.NoError(t, err)
	require.NotEqual(t, capture.RequestID, recover.RequestID)
	digest, err := request.Digest()
	require.NoError(t, err)
	result := NodeChannelResult{Version: NodeChannelVersion, RequestID: recover.RequestID, Kind: recover.Kind,
		MigrationCapture: &MigrationCapture{Request: request, RequestDigest: digest, State: MigrationCaptureIntent}}
	require.NoError(t, result.ValidateFor(recover))
	require.Error(t, result.ValidateFor(capture), "recovery reply cannot acknowledge first capture")
	recover.Target.NodeBootID = "different-boot"
	_, err = sealNodeChannelCommand(recover)
	require.Error(t, err)
}
