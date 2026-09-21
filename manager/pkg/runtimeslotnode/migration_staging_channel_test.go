package runtimeslotnode

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationStagingChannelExecutor struct {
	reserves, releases atomic.Int32
	fail               atomic.Bool
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
			if !supported {
				require.Error(t, err)
				require.Nil(t, receipt)
				require.Error(t, hub.ReleaseMigrationStaging(ctx, request))
				require.Zero(t, executor.reserves.Load())
				require.Zero(t, executor.releases.Load())
				return
			}
			require.NoError(t, err)
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
			receipt, err = hub.ReserveMigrationStaging(ctx, request)
			require.Error(t, err)
			require.Nil(t, receipt)
			require.Error(t, hub.ReleaseMigrationStaging(ctx, request))
		})
	}
}
