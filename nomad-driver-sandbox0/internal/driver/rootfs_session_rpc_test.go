package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestRootFSSessionRPCDelegatesLifecycleOverPrivateUnixSocket(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "sessiond.sock")
	runtime := &fakeRootFSRuntime{source: t.TempDir()}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	cleaner := &fakeRuntimeSlotCleaner{}
	go func() { done <- serveRootFSSessionRuntime(ctx, socket, runtime, nil, nil, cleaner) }()
	require.Eventually(t, func() bool {
		info, err := os.Stat(socket)
		return err == nil && info.Mode().Perm() == 0o600
	}, time.Second, 10*time.Millisecond)

	clientRuntime, err := newRootFSSessionClient(socket)
	require.NoError(t, err)
	require.NoError(t, clientRuntime.Ping(t.Context()))
	stage := rootfshandoff.StageRequest{Parent: "parent", Identity: rootfshandoff.Identity{
		RootFSID: "rootfs", WriterEpoch: 1,
	}}
	mount, err := clientRuntime.Ensure(t.Context(), stage, nil)
	require.NoError(t, err)
	require.Equal(t, runtime.source, mount.Source)
	lease, err := clientRuntime.RegisterConsumer(t.Context(), stage, RootFSConsumerRequest{
		ActiveKey: "task", ContainerID: "container", StableMount: "/tmp/task/rootfs",
		HostMountNamespace: "mnt:[1]",
	})
	require.NoError(t, err)
	require.Equal(t, "fake-consumer", lease.LeaseID)
	_, err = clientRuntime.RenewConsumer(t.Context(), stage, lease)
	require.NoError(t, err)
	checkpoint, err := clientRuntime.CaptureRunningFork(t.Context(), stage, rootfshandoff.RunningForkCheckpointRequest{
		OperationID: "fork", SourceSandboxID: "source", TargetSandboxID: "target", TargetGenerationID: "generation",
	})
	require.NoError(t, err)
	require.Equal(t, "fork", checkpoint.Proof.OperationID)
	retired, err := clientRuntime.Retire(t.Context(), stage, "retire")
	require.NoError(t, err)
	require.Equal(t, "retire", retired.OperationID)
	crashed, err := clientRuntime.CrashFence(t.Context(), stage, "crash", crashTaskObservation{})
	require.NoError(t, err)
	require.Equal(t, "crash", crashed.OperationID)
	cleanupRequest := testNodeCleanupRequest()
	cleanupProof, err := clientRuntime.CleanupRuntimeSlot(t.Context(), cleanupRequest)
	require.NoError(t, err)
	require.Equal(t, cleanupRequest.OperationID, cleanupProof.OperationID)
	require.Equal(t, cleanupRequest, cleaner.request)

	cancel()
	require.NoError(t, <-done)
}

type fakeRuntimeSlotCleaner struct {
	request protocol.NodeCleanupControlRequest
}

func (c *fakeRuntimeSlotCleaner) CleanupRuntimeSlot(
	_ context.Context,
	request protocol.NodeCleanupControlRequest,
) (protocol.NodeCleanupControlProof, error) {
	c.request = request
	proof := protocol.NodeCleanupControlProof{
		Version: protocol.NodeCleanupProofVersion, OperationID: request.OperationID,
		WriterOperationID: request.WriterOperationID, SlotID: request.SlotID,
		ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		WriterGrantID: request.WriterGrantID, WriterFenceDigest: request.WriterFenceDigest,
		RootFSCrashOperationID: request.WriterOperationID, RootFSCrashProofDigest: strings.Repeat("cd", sha256.Size),
		RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
	}
	proof.ProofDigest, _ = proof.Digest()
	return proof, nil
}

func TestRootFSSessionDaemonClientModeNeedsNoStorageCredentialsInPlugin(t *testing.T) {
	config := &PluginConfig{
		RootFSEnabled: true, RootFSSessiondSocket: "/run/sandbox0/rootfs-sessiond.sock",
		RootFSMountRoot: "/run/sandbox0/rootfs", RootFSConsumerMountRoot: "/opt/nomad",
	}
	require.NoError(t, validateRootFSConfig(config))
	config.RootFSMaxDirtyTailBytes = -1
	require.ErrorContains(t, validateRootFSConfig(config), "rootfs_max_dirty_tail_bytes")
	config.RootFSMaxDirtyTailBytes = 0
	config.RootFSConsumerNetNSRoot = "/"
	require.ErrorContains(t, validateRootFSConfig(config), "rootfs_consumer_netns_root")
	config.RootFSConsumerNetNSRoot = "/var/run/netns"
	config.RootFSConsumerMountRoot = ""
	require.ErrorContains(t, validateRootFSConfig(config), "rootfs_consumer_mount_root")
}

func TestPluginFingerprintRequiresHealthyRootFSSessionDaemon(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "sessiond.sock")
	runtime := &fakeRootFSRuntime{source: t.TempDir()}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serveRootFSSessionRuntime(ctx, socket, runtime, nil, nil, nil) }()
	require.Eventually(t, func() bool {
		_, err := os.Stat(socket)
		return err == nil
	}, time.Second, 10*time.Millisecond)

	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return newFakeRunsc() }).(*Plugin)
	plugin.config.RootFSEnabled = true
	plugin.config.RootFSSessiondSocket = socket
	fingerprint := plugin.buildFingerprint()
	require.Equal(t, drivers.HealthStateHealthy, fingerprint.Health)

	cancel()
	require.NoError(t, <-done)
	fingerprint = plugin.buildFingerprint()
	require.Equal(t, drivers.HealthStateUndetected, fingerprint.Health)
}

func TestRootFSSessionRPCPreservesErrorClass(t *testing.T) {
	runtime := &fakeRootFSRuntime{
		source: t.TempDir(), ensureErr: errors.Join(errors.New("binding mismatch"), errdefs.ErrFailedPrecondition),
	}
	server := rootFSSessionRPCHandler(runtime, nil, nil, nil)
	request := rootFSSessionRPCRequest{Stage: rootfshandoff.StageRequest{Parent: "parent"}}
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/v1/sessions/ensure", bytes.NewReader(payload)))
	require.Equal(t, http.StatusPreconditionFailed, recorder.Code)
	var response rootFSSessionRPCResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.ErrorIs(t, remoteRootFSError(response.Error, response.ErrorClass), errdefs.ErrFailedPrecondition)
}

func TestRootFSSessionReconcileSelectionUsesDurableConsumerLease(t *testing.T) {
	now := time.Now()
	base := rootfssession.RecoverySession{
		Kind: rootfssession.RecoveryCrashAbandon, CreatedAt: now.Add(-time.Minute), Live: true,
	}
	require.False(t, rootFSSessionNeedsReconciliation(base, now, false))
	base.CreatedAt = now.Add(-rootFSSessionAttachGrace)
	require.True(t, rootFSSessionNeedsReconciliation(base, now, false))

	base.Consumer = &rootfssession.ConsumerRegistration{
		LeaseID: "lease", ActiveKey: "task", ContainerID: "container", StableMount: "/tmp/task/rootfs",
		HostMountNamespace: "mnt:[1]", LeaseExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano),
	}
	require.False(t, rootFSSessionNeedsReconciliation(base, now, false))
	base.Live = false
	require.True(t, rootFSSessionNeedsReconciliation(base, now, false))
	base.Live = true
	base.Consumer.LeaseExpiresAt = now.Add(-time.Second).Format(time.RFC3339Nano)
	require.True(t, rootFSSessionNeedsReconciliation(base, now, false))
	base.Kind = rootfssession.RecoveryPlannedRetire
	base.Consumer.LeaseExpiresAt = now.Add(time.Minute).Format(time.RFC3339Nano)
	require.True(t, rootFSSessionNeedsReconciliation(base, now, false))
}

func TestRootFSSessionDaemonFencesRegisteredRunscAndStableMount(t *testing.T) {
	consumerRoot := t.TempDir()
	stableMount := filepath.Join(consumerRoot, "alloc", "rootfs")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	mounter := &fakeMounter{}
	hostMountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	daemon := &rootFSSessionDaemon{
		runner: runner, mounter: mounter, config: PluginConfig{RootFSConsumerMountRoot: consumerRoot},
	}
	observation, err := daemon.fenceHostRuntime(t.Context(), rootfssession.RecoverySession{
		Stage: rootfshandoff.StageRequest{Identity: rootfshandoff.Identity{ClaimID: "claim"}},
		Consumer: &rootfssession.ConsumerRegistration{
			LeaseID: "lease", ActiveKey: "task", ContainerID: "container", StableMount: stableMount,
			HostMountNamespace: hostMountNamespace, LeaseExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano),
		},
	})
	require.NoError(t, err)
	require.Equal(t, "task", observation.ActiveKey)
	require.Equal(t, "container", observation.ContainerID)
	require.True(t, observation.ContainerAbsent)
	require.Equal(t, []string{"kill:KILL", "delete:force", "state"}, runner.callsSnapshot())
	require.Equal(t, []string{stableMount}, mounter.unmounts)
}

func TestRootFSSessionDaemonCleansExactRuntimeSlotWithoutCompletingWriter(t *testing.T) {
	consumerRoot := t.TempDir()
	stableMount := filepath.Join(consumerRoot, "alloc", "rootfs")
	netnsPath := filepath.Join(consumerRoot, "alloc", "network.ns")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	require.NoError(t, os.WriteFile(netnsPath, []byte("netns"), 0o600))
	netnsIdentity, err := networkNamespaceIdentity(netnsPath)
	require.NoError(t, err)
	hostMountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	request := testNodeCleanupRequest()
	request.NetNSIdentity = netnsIdentity
	stage := rootfshandoff.StageRequest{
		BindingVersion: rootfshandoff.WriterBindingVersion, Parent: "parent-1",
		InitialGeneration:   "generation-1",
		Generation:          &rootfshandoff.GenerationDescriptor{CurrentBlockHead: "sha256:" + strings.Repeat("a", 64)},
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{NetNSIdentity: netnsIdentity},
		Identity: rootfshandoff.Identity{
			NodeUID: request.NodeUID, BootID: request.NodeBootID, RuntimeGeneration: "7",
			PodUID: request.AllocationID, PodSandboxID: "allocation-network-1", ContainerName: protocol.NomadTaskName,
			SlotNonce: request.SlotID, ClaimID: "claim-1", RootFSID: "filesystem-1",
			WriterEpoch: 9, WriterGrantID: request.WriterGrantID,
		},
	}
	consumer := &rootfssession.ConsumerRegistration{
		LeaseID: "lease-1", ActiveKey: request.SlotID, ContainerID: request.RunscContainerID,
		StableMount: stableMount, HostMountNamespace: hostMountNamespace,
		NetNSPath: netnsPath, NetNSIdentity: netnsIdentity,
		NetworkChain:   networkChainName(request.RunscContainerID),
		LeaseExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano),
	}
	localProof := testLocalCrashProof(stage, *consumer, request.OperationID, hostMountNamespace)
	runtime := &cleanupRootFSRuntime{
		fakeRootFSRuntime: &fakeRootFSRuntime{},
		recovery: []rootfssession.RecoverySession{{
			Stage: stage, Kind: rootfssession.RecoveryCrashAbandon, Consumer: consumer,
		}},
		proof: localProof,
	}
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	network := &fakeNetworkRuntime{}
	daemon := &rootFSSessionDaemon{
		runtime: runtime, runner: runner, mounter: &fakeMounter{}, network: network,
		config: PluginConfig{
			RootFSConsumerMountRoot: consumerRoot, RootFSConsumerNetNSRoot: consumerRoot,
		},
		clusterID: request.ClusterID, nodeID: request.NodeID,
	}

	first, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, first.Validate())
	require.Equal(t, request.WriterOperationID, first.RootFSCrashOperationID)
	second, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(first, second), "retry changed proof: %#v != %#v", first, second)
	require.Equal(t, 2, runtime.localCalls)
	_, cleanups := network.snapshot()
	require.Equal(t, 2, cleanups)

	changed := request
	changed.NodeBootID = "another-boot"
	_, err = daemon.CleanupRuntimeSlot(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

type cleanupRootFSRuntime struct {
	*fakeRootFSRuntime
	recovery   []rootfssession.RecoverySession
	proof      rootfshandoff.CrashFenceProof
	localCalls int
}

func (r *cleanupRootFSRuntime) RecoverySessions() ([]rootfssession.RecoverySession, error) {
	return append([]rootfssession.RecoverySession(nil), r.recovery...), nil
}

func (r *cleanupRootFSRuntime) FenceLocalRootFSWriter(
	_ context.Context,
	_ rootfshandoff.StageRequest,
	operationID string,
	_ crashTaskObservation,
) (rootfshandoff.CrashFenceProof, error) {
	r.localCalls++
	for index := range r.recovery {
		if r.recovery[index].Stage.Parent == r.proof.Parent {
			r.recovery[index].CrashOperationID = operationID
			r.recovery[index].ExternalCrash = true
		}
	}
	proof := r.proof
	proof.OperationID = operationID
	proof.Session.OperationID = operationID
	return proof, nil
}

func TestRootFSSessionDaemonRejectsNetworkNamespaceSymlinkEscapeBeforeCleanup(t *testing.T) {
	consumerRoot := t.TempDir()
	netnsRoot := filepath.Join(consumerRoot, "netns")
	outsideRoot := t.TempDir()
	require.NoError(t, os.MkdirAll(netnsRoot, 0o755))
	outsidePath := filepath.Join(outsideRoot, "outside.ns")
	require.NoError(t, os.WriteFile(outsidePath, []byte("outside"), 0o600))
	symlinkPath := filepath.Join(netnsRoot, "slot.ns")
	require.NoError(t, os.Symlink(outsidePath, symlinkPath))
	identity, err := networkNamespaceIdentity(symlinkPath)
	require.NoError(t, err)
	request := testNodeCleanupRequest()
	request.NetNSIdentity = identity
	stage := rootfshandoff.StageRequest{
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{NetNSIdentity: identity},
		Identity: rootfshandoff.Identity{
			SlotNonce: request.SlotID, PodUID: request.AllocationID, NodeUID: request.NodeUID,
			BootID: request.NodeBootID, WriterGrantID: request.WriterGrantID,
			ContainerName: protocol.NomadTaskName,
		},
	}
	consumer := &rootfssession.ConsumerRegistration{
		ActiveKey: request.SlotID, ContainerID: request.RunscContainerID,
		NetNSPath: symlinkPath, NetNSIdentity: identity, NetworkChain: networkChainName(request.RunscContainerID),
	}
	runtime := &cleanupRootFSRuntime{
		fakeRootFSRuntime: &fakeRootFSRuntime{},
		recovery: []rootfssession.RecoverySession{{
			Stage: stage, Kind: rootfssession.RecoveryCrashAbandon, Consumer: consumer,
		}},
	}
	runner := newFakeRunsc()
	daemon := &rootFSSessionDaemon{
		runtime: runtime, runner: runner, mounter: &fakeMounter{}, network: &fakeNetworkRuntime{},
		config:    PluginConfig{RootFSConsumerMountRoot: consumerRoot, RootFSConsumerNetNSRoot: netnsRoot},
		clusterID: request.ClusterID, nodeID: request.NodeID,
	}

	_, err = daemon.CleanupRuntimeSlot(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Empty(t, runner.callsSnapshot())
	require.Zero(t, runtime.localCalls)
}

func testNodeCleanupRequest() protocol.NodeCleanupControlRequest {
	return protocol.NodeCleanupControlRequest{
		OperationID: "cleanup-1", WriterOperationID: "writer-1", SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
		RunscContainerID: "runsc-1", WriterGrantID: "grant-1",
		WriterFenceDigest: strings.Repeat("ab", sha256.Size),
	}
}

func testLocalCrashProof(
	stage rootfshandoff.StageRequest,
	consumer rootfssession.ConsumerRegistration,
	operationID, hostMountNamespace string,
) rootfshandoff.CrashFenceProof {
	binding := strings.Repeat("cd", sha256.Size)
	observedAt := time.Unix(1_700_000_000, 0).UTC().Format(time.RFC3339Nano)
	session := rootfshandoff.CrashFenceSessionObservation{
		Parent: stage.Parent, RootFSID: stage.Identity.RootFSID, WriterEpoch: stage.Identity.WriterEpoch,
		OperationID: operationID, BindingDigest: binding, SessionState: rootfshandoff.StateTombstoned,
		BranchPath: "/var/lib/sandbox0/branch-1", NBDPoolAbsent: true,
		LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: observedAt,
	}
	return rootfshandoff.CrashFenceProof{
		Version: rootfshandoff.CrashFenceProofVersion, OperationID: operationID,
		Parent: stage.Parent, ClaimID: stage.Identity.ClaimID, WriterGrantID: stage.Identity.WriterGrantID,
		WriterEpoch: stage.Identity.WriterEpoch, BindingVersion: rootfshandoff.WriterBindingVersion,
		BindingDigest: binding, RootFSID: stage.Identity.RootFSID,
		InitialGeneration: stage.InitialGeneration, InitialBlockHead: stage.Generation.CurrentBlockHead,
		HeadAction: rootfshandoff.CrashFenceHeadKeepInitial, NodeUID: stage.Identity.NodeUID,
		BootID: stage.Identity.BootID, RuntimeGeneration: stage.Identity.RuntimeGeneration,
		HostMountNamespaceID: hostMountNamespace, PodUID: stage.Identity.PodUID,
		PodSandboxID: stage.Identity.PodSandboxID, ContainerName: stage.Identity.ContainerName,
		SlotNonce: stage.Identity.SlotNonce, ActiveKey: consumer.ActiveKey,
		ConsumerBound: true, ContainerID: consumer.ContainerID,
		ContainerAbsent: true, TaskAbsent: true, FrontendSnapshotAbsent: true, StableMountAbsent: true,
		SnapshotterState: rootfshandoff.StateTombstoned, Session: session, ObservedAt: observedAt,
	}
}

func TestRootFSSessionDaemonConvergesDurablePlannedAndCrashIntents(t *testing.T) {
	runtime := &fakeRootFSRuntime{}
	daemon := &rootFSSessionDaemon{runtime: runtime}
	stage := rootfshandoff.StageRequest{
		Parent: "parent", Identity: rootfshandoff.Identity{
			ClaimID: "claim", WriterGrantID: "grant", WriterEpoch: 7,
		},
	}
	require.NoError(t, daemon.reconcile(t.Context(), rootfssession.RecoverySession{
		Stage: stage, Kind: rootfssession.RecoveryPlannedRetire, RetireOperationID: "retire-operation",
	}))
	ensureCalls, retireCalls, parent, operation := runtime.snapshot()
	require.Zero(t, ensureCalls)
	require.Equal(t, 1, retireCalls)
	require.Equal(t, stage.Parent, parent)
	require.Equal(t, "retire-operation", operation)

	require.NoError(t, daemon.reconcile(t.Context(), rootfssession.RecoverySession{
		Stage: stage, Kind: rootfssession.RecoveryCrashAbandon,
	}))
	runtime.mu.Lock()
	require.Equal(t, 1, runtime.crashCalls)
	require.Equal(t, crashOperationID(stage), runtime.lastOperation)
	runtime.mu.Unlock()

	require.NoError(t, daemon.reconcile(t.Context(), rootfssession.RecoverySession{
		Stage: stage, Kind: rootfssession.RecoveryCrashAbandon, CrashOperationID: "regional-operation",
		ExternalCrash: true,
	}))
	runtime.mu.Lock()
	require.Equal(t, 1, runtime.crashCalls)
	require.Equal(t, 1, runtime.externalReclaims)
	runtime.mu.Unlock()
}

func TestClassifyRunscNotFound(t *testing.T) {
	err := classifyRunscError("state", errors.New("exit status 1"), "container does not exist")
	require.ErrorIs(t, err, errdefs.ErrNotFound)
}

func TestNomadAllocationSourceUsesAbsenceAsThePurgeFence(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "nomad.token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("secret-token\n"), 0o600))
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/v1/node/node-a/allocations", request.URL.Path)
		require.Equal(t, "secret-token", request.Header.Get("X-Nomad-Token"))
		_, _ = writer.Write([]byte(`[
            {"ID":"running","DesiredStatus":"run","ClientStatus":"running"},
            {"ID":"pending","DesiredStatus":"run","ClientStatus":"pending"},
            {"ID":"stopping","DesiredStatus":"stop","ClientStatus":"running"},
            {"ID":"complete","DesiredStatus":"run","ClientStatus":"complete"}
        ]`))
	}))
	defer server.Close()
	source, err := newNomadAllocationSource(NomadAllocationConfig{
		Address: server.URL, NodeID: "node-a", TokenFile: tokenFile,
	})
	require.NoError(t, err)
	active, err := source.ActiveAllocations(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		"running": true, "pending": true, "stopping": true, "complete": true,
	}, active)
	require.False(t, active["purged"])
}
