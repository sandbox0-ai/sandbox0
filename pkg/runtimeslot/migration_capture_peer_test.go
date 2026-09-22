package runtimeslot

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/stretchr/testify/require"
)

func migrationCapturePeerFixture(t *testing.T) MigrationCapturePeerRequest {
	t.Helper()
	source := migrationProtocolRequest()
	r := MigrationStagingRequest{Target: source.Target, Source: source,
		Destination: NodeChannelTarget{SlotID: "destination", ClusterID: source.Target.ClusterID, AllocationID: "destination-alloc",
			NodeID: "destination-node", NodeUID: "destination-uid", NodeBootID: "destination-boot", ControlEndpoint: "unix:///private/destination.sock"},
		DestinationResourceLeaseDigest: strings.Repeat("ab", 32), Bytes: 576 << 20, Inodes: 2048}
	var err error
	r.CaptureUpload, err = NewMigrationCaptureUpload(source, "team", digest.FromString("compatibility").String(), digest.FromString("cpu").String(), r.Bytes)
	require.NoError(t, err)
	makeReceipt := func(request MigrationStagingRequest, address string) MigrationStagingReserved {
		identity, err := runtimecheckpoint.NewPeerIdentity()
		require.NoError(t, err)
		endpoint, err := runtimecheckpoint.NewPeerEndpoint(address, identity)
		require.NoError(t, err)
		want, err := request.Digest()
		require.NoError(t, err)
		return MigrationStagingReserved{RequestDigest: want, PeerCertificateSHA256: runtimecheckpoint.PeerCertificateDigest(identity), Peer: endpoint}
	}
	dest := r
	dest.Target = dest.Destination
	return MigrationCapturePeerRequest{Staging: r, Source: makeReceipt(r, "10.1.1.1:18992"), Destination: makeReceipt(dest, "10.1.1.2:18992")}
}

func TestCapturePeerGrantBindsBothReservedPlacementsAndKeys(t *testing.T) {
	r := migrationCapturePeerFixture(t)
	require.NoError(t, r.Validate())
	want, err := r.Digest()
	require.NoError(t, err)
	receipt := MigrationCapturePeerPrepared{RequestDigest: want}
	require.NoError(t, receipt.ValidateFor(r))
	command, err := NewNodeChannelMigrationCapturePeerCommand(r)
	require.NoError(t, err)
	result := NodeChannelResult{Version: NodeChannelVersion, RequestID: command.RequestID, Kind: command.Kind, MigrationCapturePeer: &receipt}
	require.NoError(t, result.ValidateFor(command))
	result.MigrationStagingReserved = &r.Source
	require.Error(t, result.ValidateFor(command), "staging and early-peer receipts cannot be mixed")
	result.MigrationStagingReserved = nil
	command.MigrationCapture = &r.Staging.Source
	_, err = sealNodeChannelCommand(command)
	require.Error(t, err, "peer preparation cannot carry capture authority")
	command.MigrationCapture = nil
	command.Target = r.Staging.Destination
	_, err = sealNodeChannelCommand(command)
	require.Error(t, err, "node dispatch must retain the exact grant recipient")
	require.Equal(t, r.Source, r.LocalReceipt())
	destination := r
	destination.Staging.Target = destination.Staging.Destination
	require.NoError(t, destination.Validate())
	require.Equal(t, destination.Destination, destination.LocalReceipt())
	require.Error(t, receipt.ValidateFor(destination), "source and destination acknowledgements are not interchangeable")
	for _, mutate := range []func(*MigrationCapturePeerRequest){
		func(r *MigrationCapturePeerRequest) { r.Staging.CaptureUpload = MigrationCaptureUpload{} },
		func(r *MigrationCapturePeerRequest) {
			r.Staging.Source.Target.NodeBootID = "rebooted"
			r.Staging.Target = r.Staging.Source.Target
		},
		func(r *MigrationCapturePeerRequest) { r.Staging.Destination.NodeBootID = "rebooted" },
		func(r *MigrationCapturePeerRequest) {
			r.Staging.DestinationResourceLeaseDigest = strings.Repeat("cd", 32)
		},
		func(r *MigrationCapturePeerRequest) { r.Staging.Bytes += 4096 },
		func(r *MigrationCapturePeerRequest) { r.Source.RequestDigest = r.Destination.RequestDigest },
		func(r *MigrationCapturePeerRequest) { r.Source.Peer = runtimecheckpoint.PeerEndpoint{} },
		func(r *MigrationCapturePeerRequest) { r.Destination.Peer.Certificate = r.Source.Peer.Certificate },
		func(r *MigrationCapturePeerRequest) { r.Destination.Peer.Address = "https://public.example:18992" },
		func(r *MigrationCapturePeerRequest) { r.Destination.Peer.Address = r.Source.Peer.Address },
		func(r *MigrationCapturePeerRequest) {
			r.Destination.PeerCertificateSHA256 = r.Source.PeerCertificateSHA256
			r.Destination.Peer.Certificate = r.Source.Peer.Certificate
		},
	} {
		changed := r
		mutate(&changed)
		require.Error(t, changed.Validate())
	}
	changed := r
	changed.Destination.Peer.Address = "https://10.1.1.3:18992"
	require.NoError(t, changed.Validate())
	require.Error(t, receipt.ValidateFor(changed), "a well-formed endpoint change still changes the grant")
}

func TestStagingReceiptPreservesLegacyEncodingAndRejectsUnpinnedEndpoint(t *testing.T) {
	r := migrationCapturePeerFixture(t)
	legacy := MigrationStagingReserved{RequestDigest: r.Source.RequestDigest, PeerCertificateSHA256: r.Source.PeerCertificateSHA256}
	payload, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NotContains(t, string(payload), `"peer":`)
	require.NoError(t, legacy.ValidateFor(r.Staging))
	unpinned := r.Source
	unpinned.PeerCertificateSHA256 = ""
	require.Error(t, unpinned.ValidateFor(r.Staging))
}
