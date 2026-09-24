package runtimeslotnode

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationStagingChannelExecutor struct {
	reserves, releases atomic.Int32
	peers              atomic.Int32
	fail               atomic.Bool
}

func (e *migrationStagingChannelExecutor) PrepareMigrationCapturePeer(_ context.Context, r protocol.MigrationCapturePeerRequest) (*protocol.MigrationCapturePeerPrepared, error) {
	e.peers.Add(1)
	want, err := r.Digest()
	receipt := &protocol.MigrationCapturePeerPrepared{RequestDigest: want}
	if e.fail.Load() {
		return receipt, errdefs.ErrFailedPrecondition
	}
	return receipt, err
}

func (e *migrationStagingChannelExecutor) PrefetchMigrationImage(context.Context, protocol.MigrationImagePrefetchRequest) (*protocol.MigrationImagePrefetched, error) {
	return nil, errdefs.ErrUnavailable
}

func (e *migrationStagingChannelExecutor) ReserveMigrationStaging(_ context.Context, r protocol.MigrationStagingRequest) (*protocol.MigrationStagingReserved, error) {
	e.reserves.Add(1)
	digest, err := r.Digest()
	if e.fail.Load() {
		// Even a nonnil result must be discarded when the node rejects work.
		return &protocol.MigrationStagingReserved{RequestDigest: digest}, errdefs.ErrResourceExhausted
	}
	return &protocol.MigrationStagingReserved{RequestDigest: digest}, err
}

func (e *migrationStagingChannelExecutor) ReleaseMigrationStaging(_ context.Context, r protocol.MigrationStagingRequest) error {
	e.releases.Add(1)
	if e.fail.Load() {
		return errdefs.ErrFailedPrecondition
	}
	return r.Validate()
}

func TestMigrationStagingUsesDistinctAuthenticatedCommands(t *testing.T) {
	for _, supported := range []bool{false, true} {
		t.Run(map[bool]string{false: "unsupported", true: "supported"}[supported], func(t *testing.T) {
			hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
			require.NoError(t, err)
			server, files := newNodeChannelTLSServer(t, hub)
			executor := &migrationStagingChannelExecutor{}
			config := protocol.NodeChannelAgentConfig{BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
				ClientKeyFile: files.clientKey, TokenFile: files.token, PeerURISAN: testNodeChannelServerURI,
				NodeUID: "node-uid-1", NodeBootIDFile: files.boot, ClusterID: "cluster-1", NodeID: "node-1",
				Executor: &channelTestExecutor{}, Capacity: channelTestCapacity(), AgentInstanceID: "agent-1",
				ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond}
			if supported {
				config.MigrationStagingExecutor = executor
				config.MigrationCapturePeerExecutor = executor
				// Real ctld advertises both features. Their canonical hello order
				// must remain valid when early-peer support is added.
				config.MigrationImagePrefetchExecutor = executor
			}
			agent, err := protocol.NewNodeChannelAgent(config)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- agent.Run(ctx) }()
			t.Cleanup(func() { cancel(); <-done; server.Close(); hub.Close() })
			waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-1")
			source := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
				NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1", ControlEndpoint: "unix:///private/source.sock"},
				OperationID: "migration-1", LifecycleEpoch: 2, SandboxID: "sandbox-1", SourceGeneration: 1, ProcdInstanceID: "procd-1",
				AssignmentRevision: strings.Repeat("a", 64), BindingDigest: strings.Repeat("b", 64), ResourceLeaseDigest: strings.Repeat("c", 64)}
			request := protocol.MigrationStagingRequest{Target: source.Target, Source: source,
				Destination: protocol.NodeChannelTarget{SlotID: "slot-2", ClusterID: "cluster-1", AllocationID: "allocation-2",
					NodeID: "node-2", NodeUID: "node-uid-2", NodeBootID: "boot-2", ControlEndpoint: "unix:///private/target.sock"},
				DestinationResourceLeaseDigest: strings.Repeat("d", 64), Bytes: 8 << 20, Inodes: 64}
			receipt, err := hub.ReserveMigrationStaging(ctx, request)
			peerRequest := channelCapturePeerRequest(t, request)
			peerReceipt, peerErr := hub.PrepareMigrationCapturePeer(ctx, peerRequest)
			if !supported {
				require.Error(t, err)
				require.Nil(t, receipt)
				require.Error(t, hub.ReleaseMigrationStaging(ctx, request))
				require.Zero(t, executor.reserves.Load())
				require.Zero(t, executor.releases.Load())
				require.Error(t, peerErr)
				require.Nil(t, peerReceipt)
				require.Zero(t, executor.peers.Load())
				return
			}
			require.NoError(t, err)
			require.NoError(t, peerErr)
			require.NoError(t, peerReceipt.ValidateFor(peerRequest))
			require.EqualValues(t, 1, executor.peers.Load())
			require.NoError(t, receipt.ValidateFor(request))
			require.NoError(t, hub.ReleaseMigrationStaging(ctx, request))
			require.EqualValues(t, 1, executor.reserves.Load())
			require.EqualValues(t, 1, executor.releases.Load())
			for _, boot := range []bool{false, true} {
				changed := request
				if boot {
					changed.Source.Target.NodeBootID = "old-boot"
				} else {
					changed.Source.Target.NodeUID = "another-uid"
				}
				changed.Target = changed.Source.Target
				attempt, stop := context.WithTimeout(ctx, 100*time.Millisecond)
				_, err = hub.ReserveMigrationStaging(attempt, changed)
				require.Error(t, err)
				require.Error(t, hub.ReleaseMigrationStaging(attempt, changed))
				stop()
			}
			require.EqualValues(t, 1, executor.reserves.Load())
			require.EqualValues(t, 1, executor.releases.Load())
			executor.fail.Store(true)
			peerReceipt, peerErr = hub.PrepareMigrationCapturePeer(ctx, peerRequest)
			require.Error(t, peerErr)
			require.Nil(t, peerReceipt, "an error cannot carry an accepted grant receipt")
			receipt, err = hub.ReserveMigrationStaging(ctx, request)
			require.Error(t, err)
			require.Nil(t, receipt)
			require.Error(t, hub.ReleaseMigrationStaging(ctx, request))
		})
	}
}

func TestMigrationStagingReleaseUsesAuthenticatedSuccessorBoot(t *testing.T) {
	hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
	require.NoError(t, err)
	defer hub.Close()
	server, files := newNodeChannelTLSServer(t, hub)
	defer server.Close()
	require.NoError(t, os.WriteFile(files.boot, []byte("boot-2\n"), 0o600))
	executor := &migrationStagingChannelExecutor{}
	agent, err := protocol.NewNodeChannelAgent(protocol.NodeChannelAgentConfig{
		BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
		ClientKeyFile: files.clientKey, TokenFile: files.token, PeerURISAN: testNodeChannelServerURI,
		NodeUID: "node-uid-1", NodeBootIDFile: files.boot, ClusterID: "cluster-1", NodeID: "node-1",
		Executor: &channelTestExecutor{}, Capacity: channelTestCapacity(), AgentInstanceID: "agent-boot-2",
		ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond, MigrationStagingExecutor: executor,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()
	defer func() { cancel(); <-done }()
	waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-2")
	source := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1", ControlEndpoint: "unix:///private/source.sock"},
		OperationID: "migration-1", LifecycleEpoch: 2, SandboxID: "sandbox-1", SourceGeneration: 1, ProcdInstanceID: "procd-1",
		AssignmentRevision: strings.Repeat("a", 64), BindingDigest: strings.Repeat("b", 64), ResourceLeaseDigest: strings.Repeat("c", 64)}
	request := protocol.MigrationStagingRequest{Target: source.Target, Source: source,
		Destination: protocol.NodeChannelTarget{SlotID: "slot-2", ClusterID: "cluster-1", AllocationID: "allocation-2",
			NodeID: "node-2", NodeUID: "node-uid-2", NodeBootID: "boot-3", ControlEndpoint: "unix:///private/target.sock"},
		DestinationResourceLeaseDigest: strings.Repeat("d", 64), Bytes: 8 << 20, Inodes: 64}
	require.NoError(t, hub.ReleaseMigrationStaging(ctx, request))
	require.EqualValues(t, 1, executor.releases.Load())
	attempt, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	_, err = hub.ReserveMigrationStaging(attempt, request)
	require.Error(t, err, "successor cannot create staging for an old boot")
	require.Zero(t, executor.reserves.Load())
}

func channelCapturePeerRequest(t *testing.T, staging protocol.MigrationStagingRequest) protocol.MigrationCapturePeerRequest {
	t.Helper()
	var err error
	staging.CaptureUpload, err = protocol.NewMigrationCaptureUpload(staging.Source, "team", digest.FromString("compat").String(), digest.FromString("cpu").String(), staging.Bytes)
	require.NoError(t, err)
	r := protocol.MigrationCapturePeerRequest{Staging: staging}
	for i, receipt := range []*protocol.MigrationStagingReserved{&r.Source, &r.Destination} {
		request := staging
		address := "10.1.1.1:18992"
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
	require.NoError(t, r.Validate())
	return r
}
