package runtimeslot

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrationStagingBindsPlacementResourcesAndCapacity(t *testing.T) {
	source := migrationProtocolRequest()
	r := MigrationStagingRequest{Target: source.Target, Source: source,
		Destination: NodeChannelTarget{SlotID: "target-slot", ClusterID: source.Target.ClusterID, AllocationID: "target-allocation",
			NodeID: "target-node", NodeUID: "target-uid", NodeBootID: "target-boot", ControlEndpoint: "unix:///private/target.sock"},
		DestinationResourceLeaseDigest: strings.Repeat("12", 32), Bytes: 8 << 20, Inodes: 64}
	want, err := r.Digest()
	require.NoError(t, err)
	receipt := MigrationStagingReserved{RequestDigest: want}
	require.NoError(t, receipt.ValidateFor(r))
	reserve, err := NewNodeChannelMigrationStagingReserveCommand(r)
	require.NoError(t, err)
	release, err := NewNodeChannelMigrationStagingReleaseCommand(r)
	require.NoError(t, err)
	require.NotEqual(t, reserve.RequestID, release.RequestID)
	result := NodeChannelResult{Version: NodeChannelVersion, RequestID: reserve.RequestID, Kind: reserve.Kind, MigrationStagingReserved: &receipt}
	require.NoError(t, result.ValidateFor(reserve))
	require.Error(t, result.ValidateFor(release))
	result.RequestID, result.Kind = release.RequestID, release.Kind
	require.Error(t, result.ValidateFor(release), "a reservation receipt cannot prove release")
	result.MigrationStagingReleased = &MigrationStagingReleased{RequestDigest: want}
	require.Error(t, result.ValidateFor(release), "mixed reservation/release receipt must be rejected")
	result.MigrationStagingReserved = nil
	require.NoError(t, result.ValidateFor(release))
	reserve.MigrationCapture = &source
	_, err = sealNodeChannelCommand(reserve)
	require.Error(t, err, "staging cannot smuggle capture authority")
	for _, mutate := range []func(*MigrationStagingRequest){
		func(r *MigrationStagingRequest) { r.Target = r.Destination },
		func(r *MigrationStagingRequest) { r.Bytes += 4096 },
		func(r *MigrationStagingRequest) { r.Inodes++ },
		func(r *MigrationStagingRequest) { r.Source.LifecycleEpoch++ },
		func(r *MigrationStagingRequest) { r.Source.ProcdInstanceID = "new-procd" },
		func(r *MigrationStagingRequest) { r.Destination.NodeBootID = "rebooted" },
		func(r *MigrationStagingRequest) { r.DestinationResourceLeaseDigest = strings.Repeat("34", 32) },
	} {
		changed := r
		mutate(&changed)
		require.NoError(t, changed.Validate())
		require.Error(t, receipt.ValidateFor(changed))
	}
	for _, mutate := range []func(*MigrationStagingRequest){
		func(r *MigrationStagingRequest) { r.Target.NodeUID = "another-uid" },
		func(r *MigrationStagingRequest) { r.Destination.NodeID = r.Source.Target.NodeID },
		func(r *MigrationStagingRequest) { r.Destination.AllocationID = r.Source.Target.AllocationID },
		func(r *MigrationStagingRequest) { r.Destination.ClusterID = "another-cluster" },
		func(r *MigrationStagingRequest) { r.DestinationResourceLeaseDigest = "bad" },
		func(r *MigrationStagingRequest) { r.Bytes = 1<<50 + 4096 },
		func(r *MigrationStagingRequest) { r.Bytes = 0 },
		func(r *MigrationStagingRequest) { r.Bytes++ },
		func(r *MigrationStagingRequest) { r.Inodes = 0 },
		func(r *MigrationStagingRequest) { r.Inodes = 16385 },
	} {
		changed := r
		mutate(&changed)
		require.Error(t, changed.Validate())
	}
}
