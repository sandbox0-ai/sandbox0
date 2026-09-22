package nomadmigration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type capturePeerDecisionStore struct {
	StagingStore
	request *protocol.MigrationCapturePeerRequest
	receipt *protocol.MigrationCapturePeerPrepared
	commits int
	err     error
}

func (s *capturePeerDecisionStore) AuthorizeNomadSandboxMigrationCapturePeer(context.Context, runtimecontrol.MigrationAssignment) (*protocol.MigrationCapturePeerRequest, error) {
	return s.request, nil
}
func (s *capturePeerDecisionStore) CommitNomadSandboxMigrationCapturePeer(_ context.Context, _ runtimecontrol.MigrationAssignment, _ protocol.MigrationCapturePeerRequest, receipt *protocol.MigrationCapturePeerPrepared) error {
	s.commits++
	s.receipt = receipt
	return s.err
}

type capturePeerDecisionNode struct {
	StagingNode
	mode string
}

func (n *capturePeerDecisionNode) PrepareMigrationCapturePeer(ctx context.Context, request protocol.MigrationCapturePeerRequest) (*protocol.MigrationCapturePeerPrepared, error) {
	if n.mode == "timeout" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	want, err := request.Digest()
	r := &protocol.MigrationCapturePeerPrepared{RequestDigest: want}
	switch n.mode {
	case "error":
		return r, errors.New("reply lost after acceptance")
	case "invalid":
		r.RequestDigest = strings.Repeat("f", 64)
	}
	return r, err
}

func capturePeerDecisionFixture(t *testing.T) (runtimecontrol.MigrationAssignment, protocol.MigrationCapturePeerRequest) {
	t.Helper()
	runtime := runtimecontrol.Assignment{SandboxID: "sandbox", TeamID: "team", RuntimeGeneration: 1, SecurityClass: "standard"}
	revision, err := runtime.Revision()
	require.NoError(t, err)
	runtime.RuntimeGeneration++
	a := runtimecontrol.MigrationAssignment{OperationID: "migration", SourceGeneration: 1, SourceRevision: revision, Target: runtime}
	source := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{SlotID: "source", ClusterID: "cluster", NodeID: "node-a", NodeUID: "uid-a",
		NodeBootID: "boot-a", AllocationID: "allocation-a", ControlEndpoint: "unix:///private/source.sock"}, OperationID: a.OperationID,
		SandboxID: runtime.SandboxID, SourceGeneration: 1, LifecycleEpoch: 2, ProcdInstanceID: "procd", AssignmentRevision: revision,
		BindingDigest: strings.Repeat("a", 64), ResourceLeaseDigest: strings.Repeat("b", 64)}
	staging := protocol.MigrationStagingRequest{Target: source.Target, Source: source, Destination: protocol.NodeChannelTarget{
		SlotID: "destination", ClusterID: "cluster", NodeID: "node-b", NodeUID: "uid-b", NodeBootID: "boot-b", AllocationID: "allocation-b", ControlEndpoint: "unix:///private/destination.sock"},
		DestinationResourceLeaseDigest: strings.Repeat("c", 64), Bytes: 16 << 20, Inodes: 2048}
	staging.CaptureUpload, err = protocol.NewMigrationCaptureUpload(source, runtime.TeamID, digest.FromString("compat").String(), digest.FromString("cpu").String(), staging.Bytes)
	require.NoError(t, err)
	r := protocol.MigrationCapturePeerRequest{Staging: staging}
	for i, receipt := range []*protocol.MigrationStagingReserved{&r.Source, &r.Destination} {
		request, address := staging, "10.1.1.1:18992"
		if i == 1 {
			request.Target, address = staging.Destination, "10.1.1.2:18992"
		}
		receipt.RequestDigest, err = request.Digest()
		require.NoError(t, err)
		cert, err := runtimecheckpoint.NewPeerIdentity()
		require.NoError(t, err)
		receipt.PeerCertificateSHA256 = runtimecheckpoint.PeerCertificateDigest(cert)
		receipt.Peer, err = runtimecheckpoint.NewPeerEndpoint(address, cert)
		require.NoError(t, err)
	}
	require.NoError(t, a.Validate())
	require.NoError(t, r.Validate())
	return a, r
}

func TestCapturePeerDecisionSeparatesFallbackFromAuthorityErrors(t *testing.T) {
	for _, mode := range []string{"accepted", "unsupported", "error", "timeout", "invalid", "canceled", "commit-error"} {
		t.Run(mode, func(t *testing.T) {
			a, request := capturePeerDecisionFixture(t)
			store := &capturePeerDecisionStore{request: &request}
			var node StagingNode = &capturePeerDecisionNode{mode: mode}
			if mode == "unsupported" {
				node = struct{ StagingNode }{}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			if mode == "commit-error" {
				store.err = errors.New("regional commit unavailable")
			}
			progress, err := prepareCapturePeer(ctx, store, node, a)
			if mode == "invalid" || mode == "canceled" {
				require.Error(t, err)
				require.False(t, progress)
				require.Zero(t, store.commits)
				return
			}
			require.Equal(t, 1, store.commits)
			if mode == "commit-error" {
				require.ErrorIs(t, err, store.err)
				require.False(t, progress)
				return
			}
			require.NoError(t, err)
			require.True(t, progress)
			if mode == "accepted" {
				require.NoError(t, store.receipt.ValidateFor(request))
			} else {
				require.Nil(t, store.receipt, "fallback cannot retain an acknowledgement from an unsuccessful call")
			}
		})
	}
}
