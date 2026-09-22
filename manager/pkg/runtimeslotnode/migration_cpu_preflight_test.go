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

type migrationCPUChannelExecutor struct{ calls atomic.Int32 }

func (e *migrationCPUChannelExecutor) PreflightMigrationCPU(_ context.Context, r protocol.MigrationCPUPreflightRequest) (*protocol.MigrationCPUPreflight, error) {
	e.calls.Add(1)
	launch := protocol.MigrationCPULaunch{Version: protocol.MigrationCPULaunchVersion, Target: r.Source.Target,
		ExecutableDigest: "sha256:" + strings.Repeat("a", 64), SandboxID: r.Source.SandboxID,
		RuntimeGeneration: r.Source.SourceGeneration, LaunchAttempt: "launch-1", BindingDigest: r.Source.BindingDigest,
		ResourceLeaseDigest: r.Source.ResourceLeaseDigest, Resources: r.SourceResources, AssignmentRevision: r.Source.AssignmentRevision,
		Observation: protocol.MigrationCPUObservation{CPUSet: "0-3", Profile: protocol.MigrationCPUProfile{
			Version: protocol.MigrationCPUProfileVersion, Architecture: "arm64", RunscVersion: "runsc version release-20260817.0", Features: []string{"aes"}}}}
	digest, err := r.Digest()
	return &protocol.MigrationCPUPreflight{RequestDigest: digest, Launch: launch, Observation: launch.Observation}, err
}

// Exercise the mutually authenticated production channel, including capability
// negotiation and exact physical-node/boot routing before executor dispatch.
func TestMigrationCPUPreflightRequiresAuthenticatedNodeAndCapability(t *testing.T) {
	for _, supported := range []bool{false, true} {
		hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
		require.NoError(t, err)
		server, files := newNodeChannelTLSServer(t, hub)
		executor := &migrationCPUChannelExecutor{}
		config := protocol.NodeChannelAgentConfig{BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert,
			ClientKeyFile: files.clientKey, TokenFile: files.token, PeerURISAN: testNodeChannelServerURI,
			NodeUID: "node-uid-1", NodeBootIDFile: files.boot, ClusterID: "cluster-1", NodeID: "node-1",
			Executor: &channelTestExecutor{}, Capacity: channelTestCapacity(), AgentInstanceID: "agent-1",
			ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond}
		if supported {
			config.MigrationCPUPreflightExecutor = executor
		}
		agent, err := protocol.NewNodeChannelAgent(config)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- agent.Run(ctx) }()
		// Register before assertions so a failure cannot leave a channel goroutine.
		t.Cleanup(func() { cancel(); <-done; server.Close(); hub.Close() })
		waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-1")
		source := protocol.NodeChannelTarget{SlotID: "slot-1", AllocationID: "allocation-1", ClusterID: "cluster-1",
			NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1", ControlEndpoint: "unix:///private/source.sock"}
		target := protocol.NodeChannelTarget{SlotID: "slot-2", AllocationID: "allocation-2", ClusterID: "cluster-1",
			NodeID: "node-2", NodeUID: "node-uid-2", NodeBootID: "boot-2", ControlEndpoint: "unix:///private/target.sock"}
		makeResources := func(p protocol.NodeChannelTarget, operation string) protocol.RuntimeResourceLease {
			lease, err := protocol.NewRuntimeResourceLease(operation, "claim-"+p.SlotID, p.SlotID, p.ClusterID, p.NodeID, p.NodeUID, p.NodeBootID,
				protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 1 << 30, PIDsLimit: protocol.DefaultRuntimePIDsLimit}, "0-3", "0")
			require.NoError(t, err)
			return lease
		}
		sourceResources := makeResources(source, "ordinary-claim")
		leaseDigest, _ := sourceResources.Digest()
		request := protocol.MigrationCPUPreflightRequest{Target: source, SourceResources: sourceResources,
			Destination: target, DestinationResources: makeResources(target, "migration-1"),
			Source: protocol.MigrationCaptureRequest{Target: source, OperationID: "migration-1", LifecycleEpoch: 2,
				SandboxID: "sandbox-1", SourceGeneration: 1, ProcdInstanceID: "procd-1", AssignmentRevision: strings.Repeat("b", 64),
				BindingDigest: strings.Repeat("c", 64), ResourceLeaseDigest: strings.TrimPrefix(leaseDigest, "sha256:")}}
		result, err := hub.PreflightMigrationCPU(ctx, request)
		if supported {
			require.NoError(t, err)
			require.NoError(t, result.ValidateFor(request))
			require.EqualValues(t, 1, executor.calls.Load())
		} else {
			require.Error(t, err)
			require.Nil(t, result)
			require.Zero(t, executor.calls.Load())
		}
		calls := executor.calls.Load()
		for _, boot := range []bool{false, true} {
			changed := request
			if boot {
				changed.Target.NodeBootID, changed.Source.Target.NodeBootID, changed.SourceResources.NodeBootID = "old-boot", "old-boot", "old-boot"
			} else {
				changed.Target.NodeUID, changed.Source.Target.NodeUID, changed.SourceResources.NodeUID = "another-uid", "another-uid", "another-uid"
			}
			leaseDigest, _ := changed.SourceResources.Digest()
			changed.Source.ResourceLeaseDigest = strings.TrimPrefix(leaseDigest, "sha256:")
			require.NoError(t, changed.Validate())
			probe, stop := context.WithTimeout(ctx, 100*time.Millisecond)
			result, err = hub.PreflightMigrationCPU(probe, changed)
			stop()
			require.Error(t, err)
			require.Nil(t, result)
			require.Equal(t, calls, executor.calls.Load())
		}
	}
}
