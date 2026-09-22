package runtimeslotnode

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationRecoveryChannelExecutor struct{ calls atomic.Int32 }

func (e *migrationRecoveryChannelExecutor) RecoverMigrationCapture(_ context.Context, r protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	e.calls.Add(1)
	digest, err := r.Digest()
	return &protocol.MigrationCapture{Request: r, RequestDigest: digest, State: protocol.MigrationCaptureIntent}, err
}

func TestMigrationRecoveryRequiresItsOwnAuthenticatedCapability(t *testing.T) {
	for _, supported := range []bool{false, true} {
		hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
		require.NoError(t, err)
		server, files := newNodeChannelTLSServer(t, hub)
		recovery := &migrationRecoveryChannelExecutor{}
		capture := &migrationChannelExecutor{}
		config := protocol.NodeChannelAgentConfig{BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
			ClientKeyFile: files.clientKey, TokenFile: files.token, PeerURISAN: testNodeChannelServerURI,
			NodeUID: "node-uid-1", NodeBootIDFile: files.boot, ClusterID: "cluster-1", NodeID: "node-1",
			Executor: &channelTestExecutor{}, Capacity: channelTestCapacity(), AgentInstanceID: "agent-1",
			ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond, MigrationCaptureExecutor: capture}
		if supported {
			config.MigrationRecoveryExecutor = recovery
		}
		agent, err := protocol.NewNodeChannelAgent(config)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- agent.Run(ctx) }()
		t.Cleanup(func() { cancel(); <-done; server.Close(); hub.Close() })
		waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-1")
		request := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
			NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1", ControlEndpoint: "unix:///private/source.sock"},
			OperationID: "migration-1", LifecycleEpoch: 2, SandboxID: "sandbox-1", SourceGeneration: 1, ProcdInstanceID: "procd-1",
			AssignmentRevision: strings.Repeat("a", 64), BindingDigest: strings.Repeat("b", 64), ResourceLeaseDigest: strings.Repeat("c", 64)}
		result, err := hub.RecoverMigrationCapture(ctx, request)
		if supported {
			require.NoError(t, err)
			require.NoError(t, result.Validate())
			require.EqualValues(t, 1, recovery.calls.Load())
		} else {
			require.Error(t, err)
			require.Nil(t, result)
			require.Zero(t, recovery.calls.Load())
		}
		calls := recovery.calls.Load()
		for _, boot := range []bool{false, true} {
			changed := request
			if boot {
				changed.Target.NodeBootID = "stale-boot"
			} else {
				changed.Target.NodeUID = "another-node"
			}
			attempt, stop := context.WithTimeout(ctx, 100*time.Millisecond)
			_, err = hub.RecoverMigrationCapture(attempt, changed)
			stop()
			require.Error(t, err)
			require.Equal(t, calls, recovery.calls.Load())
		}
		capture.mu.Lock()
		captureCalls := capture.calls
		capture.mu.Unlock()
		require.Zero(t, captureCalls, "no fallback to the initial capture capability")
	}
}
