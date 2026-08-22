package nomadruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestNodeRuntimeRPCDelegatesLifecycleOverPrivateUnixSocket(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "ctld Nomad runtime.sock")
	runtime := &fakeRootFSRuntime{source: t.TempDir()}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	cleaner := &fakeRuntimeSlotCleaner{}
	go func() { done <- serveNodeRuntime(ctx, socket, runtime, nil, nil, cleaner) }()
	require.Eventually(t, func() bool {
		info, err := os.Stat(socket)
		return err == nil && info.Mode().Perm() == 0o600
	}, time.Second, 10*time.Millisecond)

	clientRuntime, err := NewClient(socket)
	require.NoError(t, err)
	require.NoError(t, clientRuntime.Ping(t.Context()))
	info, err := clientRuntime.RuntimeInfo(t.Context())
	require.NoError(t, err)
	require.Equal(t, RuntimeInfoVersion, info.Version)
	require.Equal(t, "/run/sandbox0/rootfs", info.MountRoot)
	stage := rootfshandoff.StageRequest{Parent: "parent", Identity: rootfshandoff.Identity{
		RootFSID: "rootfs", WriterEpoch: 1,
	}}
	mount, err := clientRuntime.Ensure(t.Context(), stage, nil)
	require.NoError(t, err)
	require.Equal(t, runtime.source, mount.Source)
	lease, err := clientRuntime.RegisterConsumer(t.Context(), stage, ConsumerRequest{
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
	crashed, err := clientRuntime.CrashFence(t.Context(), stage, "crash", CrashTaskObservation{})
	require.NoError(t, err)
	require.Equal(t, "crash", crashed.OperationID)
	cleanupRequest := testNodeCleanupRequest()
	cleanupProof, err := clientRuntime.CleanupRuntimeSlot(t.Context(), cleanupRequest)
	require.NoError(t, err)
	require.Equal(t, cleanupRequest.OperationID, cleanupProof.OperationID)
	require.Equal(t, cleanupRequest, cleaner.request)
	registration := testRuntimeSlotJournalRegistration(t, "slot-rpc")
	require.NoError(t, clientRuntime.RegisterRuntimeSlot(t.Context(), registration))
	require.Equal(t, registration, cleaner.registration)

	cancel()
	require.NoError(t, <-done)
}

type fakeRuntimeSlotCleaner struct {
	request      protocol.NodeCleanupControlRequest
	registration RuntimeSlotRegistration
}

func (c *fakeRuntimeSlotCleaner) runtimeSlotNetworkPrepareRequest(
	request protocol.NodeNetworkPrepareControlRequest,
) (protocol.RuntimeSlotNetworkPrepareRequest, error) {
	return protocol.RuntimeSlotNetworkPrepareRequest{Request: request, NetNSRelativePath: "allocation-1"}, nil
}

func (c *fakeRuntimeSlotCleaner) RegisterRuntimeSlot(
	_ context.Context,
	registration RuntimeSlotRegistration,
) error {
	c.registration = registration
	return nil
}

func (c *fakeRuntimeSlotCleaner) CleanupRuntimeSlot(
	_ context.Context,
	request protocol.NodeCleanupControlRequest,
) (protocol.NodeCleanupControlProof, error) {
	c.request = request
	proof := protocol.NodeCleanupControlProof{
		Version: protocol.NodeCleanupProofVersion, OperationID: request.OperationID,
		WriterOperationID: request.WriterOperationID, WriterRetireKind: request.WriterRetireKind,
		SlotID:    request.SlotID,
		ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		WriterGrantID: request.WriterGrantID, WriterAuthorityDigest: request.WriterAuthorityDigest,
		RootFSOperationID: request.WriterOperationID, RootFSProofDigest: strings.Repeat("cd", sha256.Size),
		RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
	}
	proof.ProofDigest, _ = proof.Digest()
	return proof, nil
}

func TestNodeRuntimeRPCPreservesErrorClass(t *testing.T) {
	runtime := &fakeRootFSRuntime{
		source: t.TempDir(), ensureErr: errors.Join(errors.New("binding mismatch"), errdefs.ErrFailedPrecondition),
	}
	server := nodeRuntimeRPCHandler(runtime, nil, nil, nil)
	request := nodeRuntimeRPCRequest{Stage: rootfshandoff.StageRequest{Parent: "parent"}}
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/v1/sessions/ensure", bytes.NewReader(payload)))
	require.Equal(t, http.StatusPreconditionFailed, recorder.Code)
	var response nodeRuntimeRPCResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.ErrorIs(t, remoteRootFSError(response.Error, response.ErrorClass), errdefs.ErrFailedPrecondition)
}

func TestNodeRuntimeRPCPreservesConsumedWriterAttachFailure(t *testing.T) {
	runtime := &fakeRootFSRuntime{
		ensureErr: &ConsumedAttachError{Err: errors.New("NBD attach failed")},
	}
	server := httptest.NewServer(nodeRuntimeRPCHandler(runtime, nil, nil, nil))
	defer server.Close()
	client := &Client{http: server.Client()}
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(server.URL, "http://"))
	}
	client.http = &http.Client{Transport: transport}

	_, err := client.Ensure(t.Context(), rootfshandoff.StageRequest{Parent: "parent"}, nil)
	var consumed *ConsumedAttachError
	require.ErrorAs(t, err, &consumed)
	require.ErrorContains(t, consumed, "NBD attach failed")
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

	terminal := rootfssession.RecoverySession{
		Kind: rootfssession.RecoveryCrashAbandon, ExternalCrash: true, BranchRemoved: true,
		CrashRequestedAt: now.Add(-runtimeSlotProofRetention),
	}
	require.False(t, rootFSSessionNeedsReconciliation(terminal, now, true),
		"a purged allocation must not hot-poll a retained terminal proof")
	terminal.CrashRequestedAt = now.Add(-rootfssession.ExternalTerminalProofRetention)
	require.True(t, rootFSSessionNeedsReconciliation(terminal, now, false),
		"an expired terminal proof must be verified and forgotten")
}

func TestNodeRuntimeThrottlesCompletedProofPruning(t *testing.T) {
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	putExpired := func(slotID string) RuntimeSlotRegistration {
		registration := testRuntimeSlotJournalRegistration(t, slotID)
		require.NoError(t, journal.Register(registration))
		request := testRuntimeSlotJournalCleanup(registration)
		_, err := journal.BeginCleanup(request)
		require.NoError(t, err)
		require.NoError(t, journal.CompleteCleanup(request, testRuntimeSlotJournalProof(t, request)))
		record, err := journal.Get(slotID)
		require.NoError(t, err)
		record.CompletedAt = time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
		record.UpdatedAt = record.CompletedAt
		require.NoError(t, journal.db.Update(func(tx *bolt.Tx) error {
			return putRuntimeSlotJournalRecord(tx.Bucket(runtimeSlotJournalBucket), record)
		}))
		return registration
	}

	first := putExpired("slot-prune-first")
	daemon := &nodeRuntime{runtime: &fakeRootFSRuntime{}, journal: journal}
	daemon.scan(t.Context(), "")
	_, err = journal.Get(first.SlotID)
	require.ErrorIs(t, err, errdefs.ErrNotFound)

	second := putExpired("slot-prune-second")
	daemon.scan(t.Context(), "")
	_, err = journal.Get(second.SlotID)
	require.NoError(t, err, "a hot reconciliation trigger must not rescan terminal history")

	daemon.lastJournalPrune = time.Now().Add(-runtimeSlotJournalPruneInterval)
	daemon.scan(t.Context(), "")
	_, err = journal.Get(second.SlotID)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
}

func TestNodeRuntimeFencesRegisteredRunscAndStableMount(t *testing.T) {
	consumerRoot := t.TempDir()
	stableMount := filepath.Join(consumerRoot, "alloc", "rootfs")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	mounter := &fakeMounter{}
	hostMountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	daemon := &nodeRuntime{
		runner: runner, mounter: mounter, config: Config{RootFSConsumerMountRoot: consumerRoot},
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

func TestNodeRuntimeCleansExactRuntimeSlotWithoutCompletingWriter(t *testing.T) {
	testNodeRuntimeCleansExactRuntimeSlot(t, false)
}

func TestNodeRuntimeExternalCleanupPreemptsLocalReconciliation(t *testing.T) {
	daemon := &nodeRuntime{}
	localContext, localCancel := context.WithCancel(context.Background())
	require.True(t, daemon.beginReconciliation("slot-preempt", localCancel))
	localExited := make(chan struct{})
	go func() {
		<-localContext.Done()
		daemon.endReconciliation("slot-preempt")
		close(localExited)
	}()

	cleanupContext, cleanupCancel := context.WithTimeout(t.Context(), time.Second)
	defer cleanupCancel()
	require.NoError(t, daemon.beginExternalReconciliation(cleanupContext, "slot-preempt"))
	select {
	case <-localExited:
	case <-time.After(time.Second):
		t.Fatal("local reconciliation was not canceled")
	}
	require.True(t, daemon.reconciliationInFlight("slot-preempt"))
	require.False(t, daemon.beginReconciliation("slot-preempt", func() {}))
	daemon.endReconciliation("slot-preempt")
	require.False(t, daemon.reconciliationInFlight("slot-preempt"))
}

func TestNodeRuntimeScanYieldsToDurableRegionalCleanup(t *testing.T) {
	registration := testRuntimeSlotJournalRegistration(t, "slot-regional-cleanup")
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	require.NoError(t, journal.Register(registration))
	cleanup := testRuntimeSlotJournalCleanup(registration)
	cleanup.WriterOperationID = "regional-crash-operation"
	cleanup.WriterRetireKind = protocol.WriterRetireKindCrashAbandon
	cleanup.WriterGrantID = "grant-regional-cleanup"
	cleanup.WriterAuthorityDigest = strings.Repeat("ab", sha256.Size)
	_, err = journal.BeginCleanup(cleanup)
	require.NoError(t, err)

	stage := rootfshandoff.StageRequest{
		Parent: "parent-regional-cleanup",
		Identity: rootfshandoff.Identity{
			SlotNonce: registration.SlotID, WriterGrantID: cleanup.WriterGrantID, WriterEpoch: 7,
		},
	}
	runtime := &fakeRootFSRuntime{recoverySessions: []rootfssession.RecoverySession{{
		Stage: stage, Kind: rootfssession.RecoveryPlannedRetire, RetireOperationID: "old-local-planned-operation",
	}}}
	daemon := &nodeRuntime{runtime: runtime, journal: journal}
	daemon.scan(t.Context(), stage.Parent)
	daemon.wg.Wait()
	_, retireCalls, _, _ := runtime.snapshot()
	require.Zero(t, retireCalls)
}

func TestNodeRuntimeReclaimsMatchingInternalCrashTerminal(t *testing.T) {
	testNodeRuntimeCleansExactRuntimeSlot(t, true)
}

func testNodeRuntimeCleansExactRuntimeSlot(t *testing.T, internalTerminal bool) {
	t.Helper()
	consumerRoot := t.TempDir()
	stableMount := filepath.Join(consumerRoot, "alloc", "rootfs")
	netnsPath := filepath.Join(consumerRoot, "alloc", "network.ns")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	require.NoError(t, os.WriteFile(netnsPath, []byte("netns"), 0o600))
	netnsIdentity, err := networkNamespaceIdentity(netnsPath)
	require.NoError(t, err)
	hostMountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	stableMountID, err := stableMountIdentity(stableMount)
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
	localProof := testLocalCrashProof(t, stage, *consumer, request.WriterOperationID, hostMountNamespace)
	observation := CrashTaskObservation{
		ActiveKey: consumer.ActiveKey, ContainerID: consumer.ContainerID,
		HostMountNamespaceID: hostMountNamespace, ContainerAbsent: true, TaskAbsent: true,
		FrontendSnapshotAbsent: true, StableMountAbsent: true,
	}
	require.NoError(t, validateCrashCleanupProof(localProof, stage, observation, request))
	wrongProof := testLocalCrashProof(t, stage, *consumer, "another-writer-operation", hostMountNamespace)
	require.ErrorIs(t, validateCrashCleanupProof(wrongProof, stage, observation, request), errdefs.ErrFailedPrecondition)
	recovery := rootfssession.RecoverySession{
		Stage: stage, Kind: rootfssession.RecoveryCrashAbandon, Consumer: consumer,
	}
	if internalTerminal {
		recovery.CrashOperationID = request.WriterOperationID
	}
	runtime := &cleanupRootFSRuntime{
		fakeRootFSRuntime: &fakeRootFSRuntime{},
		recovery:          []rootfssession.RecoverySession{recovery},
		proof:             localProof,
	}
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	network := newFakeCtldNetwork(t)
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	daemon := &nodeRuntime{
		runtime: runtime, runner: runner, mounter: &fakeMounter{}, runtimeSlotNetwork: network.client, journal: journal,
		config: Config{
			RootFSConsumerMountRoot: consumerRoot, RootFSConsumerNetNSRoot: consumerRoot,
		},
		clusterID: request.ClusterID, nodeID: request.NodeID, nodeUID: request.NodeUID,
	}
	require.NoError(t, daemon.RegisterRuntimeSlot(t.Context(), RuntimeSlotRegistration{
		Version: RuntimeSlotJournalVersion, SlotID: request.SlotID, ClusterID: request.ClusterID,
		AllocationID: request.AllocationID, NodeID: request.NodeID, NodeBootID: request.NodeBootID,
		NetNSPath: netnsPath, NetNSIdentity: netnsIdentity, NetworkChain: consumer.NetworkChain,
		RunscContainerID: request.RunscContainerID, StableMount: stableMount, StableMountID: stableMountID,
		MountNamespaceID: hostMountNamespace,
	}))

	first, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, first.Validate())
	require.Equal(t, request.WriterOperationID, first.RootFSOperationID)
	runtime.recovery = nil
	second, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(first, second), "retry changed proof: %#v != %#v", first, second)
	if internalTerminal {
		require.Equal(t, 1, runtime.reclaimCalls)
		require.Zero(t, runtime.localCalls)
		require.Equal(t, request.WriterAuthorityDigest, first.RootFSProofDigest)
	} else {
		require.Equal(t, 1, runtime.localCalls)
		require.Zero(t, runtime.reclaimCalls)
	}
	cleanups := network.cleanupCount()
	require.Equal(t, 1, cleanups)

	changed := request
	changed.NodeBootID = "another-boot"
	_, err = daemon.CleanupRuntimeSlot(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestNodeRuntimeCleansCanceledUnconsumedWriterFromJournal(t *testing.T) {
	consumerRoot := t.TempDir()
	stableMount := filepath.Join(consumerRoot, "alloc", "rootfs")
	netnsPath := filepath.Join(consumerRoot, "alloc", "network.ns")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	require.NoError(t, os.WriteFile(netnsPath, []byte("netns"), 0o600))
	netnsIdentity, err := networkNamespaceIdentity(netnsPath)
	require.NoError(t, err)
	hostMountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	stableMountID, err := stableMountIdentity(stableMount)
	require.NoError(t, err)
	request := testNodeCleanupRequest()
	request.WriterRetireKind = protocol.WriterRetireKindCanceled
	request.NetNSIdentity = netnsIdentity
	stage := rootfshandoff.StageRequest{
		BindingVersion: rootfshandoff.WriterBindingVersion, Parent: "parent-unconsumed",
		InitialGeneration:   "generation-1",
		Generation:          &rootfshandoff.GenerationDescriptor{CurrentBlockHead: "sha256:" + strings.Repeat("a", 64)},
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{NetNSIdentity: netnsIdentity},
		Identity: rootfshandoff.Identity{
			NodeUID: request.NodeUID, BootID: request.NodeBootID, RuntimeGeneration: "7",
			PodUID: request.AllocationID, PodSandboxID: "allocation-network-1", ContainerName: protocol.NomadTaskName,
			SlotNonce: request.SlotID, ClaimID: "claim-unconsumed", RootFSID: "filesystem-1",
			WriterEpoch: 9, WriterGrantID: request.WriterGrantID,
		},
	}
	localProof := testUnboundLocalCrashProof(t, stage, request.WriterOperationID, hostMountNamespace)
	runtime := &cleanupRootFSRuntime{
		fakeRootFSRuntime: &fakeRootFSRuntime{},
		recovery: []rootfssession.RecoverySession{{
			Stage: stage, Kind: rootfssession.RecoveryCrashAbandon,
		}},
		proof: localProof,
	}
	boundCanceled := runtime.recovery[0]
	boundCanceled.Consumer = &rootfssession.ConsumerRegistration{}
	require.ErrorIs(t, validateRuntimeSlotCleanupSession(boundCanceled, request), errdefs.ErrFailedPrecondition)
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	network := newFakeCtldNetwork(t)
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	daemon := &nodeRuntime{
		runtime: runtime, runner: runner, mounter: &fakeMounter{}, runtimeSlotNetwork: network.client, journal: journal,
		config: Config{
			RootFSConsumerMountRoot: consumerRoot, RootFSConsumerNetNSRoot: consumerRoot,
		},
		clusterID: request.ClusterID, nodeID: request.NodeID, nodeUID: request.NodeUID,
	}
	require.NoError(t, daemon.RegisterRuntimeSlot(t.Context(), RuntimeSlotRegistration{
		Version: RuntimeSlotJournalVersion, SlotID: request.SlotID, ClusterID: request.ClusterID,
		AllocationID: request.AllocationID, NodeID: request.NodeID, NodeBootID: request.NodeBootID,
		NetNSPath: netnsPath, NetNSIdentity: netnsIdentity,
		NetworkChain: networkChainName(request.RunscContainerID), RunscContainerID: request.RunscContainerID,
		StableMount: stableMount, StableMountID: stableMountID, MountNamespaceID: hostMountNamespace,
	}))

	proof, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, proof.Validate())
	require.Equal(t, 1, runtime.localCalls)
	require.Equal(t, request.WriterOperationID, proof.RootFSOperationID)
	require.NotEqual(t, request.WriterAuthorityDigest, proof.RootFSProofDigest)
	require.Equal(t, []string{"kill:KILL", "delete:force", "state"}, runner.callsSnapshot())
	cleanups := network.cleanupCount()
	require.Equal(t, 1, cleanups)
}

func TestNodeRuntimeCleansPlannedRetiredWriterAfterSessionForgotten(t *testing.T) {
	consumerRoot := t.TempDir()
	netnsRoot := filepath.Join(consumerRoot, "netns")
	stableMount := filepath.Join(consumerRoot, "alloc", "rootfs")
	netnsPath := filepath.Join(netnsRoot, "allocation.ns")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	require.NoError(t, os.MkdirAll(netnsRoot, 0o755))
	require.NoError(t, os.WriteFile(netnsPath, []byte("netns"), 0o600))
	netnsIdentity, err := networkNamespaceIdentity(netnsPath)
	require.NoError(t, err)
	stableMountID, err := stableMountIdentity(stableMount)
	require.NoError(t, err)
	mountNamespaceID, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	request := testNodeCleanupRequest()
	request.WriterOperationID = "planned-retire-1"
	request.WriterRetireKind = protocol.WriterRetireKindPlannedPublish
	request.WriterAuthorityDigest = strings.Repeat("ef", sha256.Size)
	request.NetNSIdentity = netnsIdentity
	registration := RuntimeSlotRegistration{
		Version: RuntimeSlotJournalVersion, SlotID: request.SlotID, ClusterID: request.ClusterID,
		AllocationID: request.AllocationID, NodeID: request.NodeID, NodeBootID: request.NodeBootID,
		NetNSPath: netnsPath, NetNSIdentity: netnsIdentity,
		NetworkChain: networkChainName(request.RunscContainerID), RunscContainerID: request.RunscContainerID,
		StableMount: stableMount, StableMountID: stableMountID, MountNamespaceID: mountNamespaceID,
	}
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	network := newFakeCtldNetwork(t)
	runtime := &cleanupRootFSRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}}
	daemon := &nodeRuntime{
		runtime: runtime, runner: runner, mounter: &fakeMounter{}, runtimeSlotNetwork: network.client, journal: journal,
		config:    Config{RootFSConsumerMountRoot: consumerRoot, RootFSConsumerNetNSRoot: netnsRoot},
		clusterID: request.ClusterID, nodeID: request.NodeID, nodeUID: request.NodeUID,
	}
	require.NoError(t, daemon.RegisterRuntimeSlot(t.Context(), registration))

	first, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, first.Validate())
	require.Equal(t, request.WriterOperationID, first.RootFSOperationID)
	require.NotEmpty(t, first.RootFSProofDigest)
	require.Zero(t, runtime.localCalls)
	require.Equal(t, []string{"kill:KILL", "delete:force", "state"}, runner.callsSnapshot())
	cleanups := network.cleanupCount()
	require.Equal(t, 1, cleanups)

	// The journaled proof remains byte-stable after all destructive adapters and
	// the RootFS session record are unavailable.
	daemon.runtime = nil
	daemon.runner = nil
	daemon.mounter = nil
	daemon.runtimeSlotNetwork = nil
	second, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, first, second)

	changed := request
	changed.WriterAuthorityDigest = strings.Repeat("ab", sha256.Size)
	_, err = daemon.CleanupRuntimeSlot(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
}

func TestNodeRuntimeFinishesMatchingPlannedSessionBeforeProof(t *testing.T) {
	consumerRoot := t.TempDir()
	stableMount := filepath.Join(consumerRoot, "alloc", "rootfs")
	netnsPath := filepath.Join(consumerRoot, "alloc", "network.ns")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	require.NoError(t, os.WriteFile(netnsPath, []byte("netns"), 0o600))
	netnsIdentity, err := networkNamespaceIdentity(netnsPath)
	require.NoError(t, err)
	hostMountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	stableMountID, err := stableMountIdentity(stableMount)
	require.NoError(t, err)
	request := testNodeCleanupRequest()
	request.WriterOperationID = "planned-retire-live"
	request.WriterRetireKind = protocol.WriterRetireKindPlannedPublish
	detachProof := bytes.Repeat([]byte{0xab}, sha256.Size)
	request.WriterAuthorityDigest = strings.Repeat("ab", sha256.Size)
	request.NetNSIdentity = netnsIdentity
	stage := rootfshandoff.StageRequest{
		BindingVersion: rootfshandoff.WriterBindingVersion, Parent: "parent-planned",
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
		LeaseID: "lease-planned", ActiveKey: request.SlotID, ContainerID: request.RunscContainerID,
		StableMount: stableMount, HostMountNamespace: hostMountNamespace,
		NetNSPath: netnsPath, NetNSIdentity: netnsIdentity,
		NetworkChain:   networkChainName(request.RunscContainerID),
		LeaseExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano),
	}
	runtime := &cleanupRootFSRuntime{
		fakeRootFSRuntime: &fakeRootFSRuntime{},
		recovery: []rootfssession.RecoverySession{{
			Stage: stage, Kind: rootfssession.RecoveryPlannedRetire,
			RetireOperationID: request.WriterOperationID, Consumer: consumer,
		}},
		retireResult: &rootfssession.RetireResult{
			Parent: stage.Parent, RootFSID: stage.Identity.RootFSID,
			WriterEpoch: stage.Identity.WriterEpoch, OperationID: request.WriterOperationID,
			DetachProof: detachProof,
		},
	}
	require.NoError(t, validatePlannedCleanupResult(*runtime.retireResult, stage, request))
	wrongResult := *runtime.retireResult
	wrongResult.OperationID = "another-planned-operation"
	require.ErrorIs(t, validatePlannedCleanupResult(wrongResult, stage, request), errdefs.ErrFailedPrecondition)
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	daemon := &nodeRuntime{
		runtime: runtime, runner: runner, mounter: &fakeMounter{}, runtimeSlotNetwork: newFakeCtldNetwork(t).client, journal: journal,
		config:    Config{RootFSConsumerMountRoot: consumerRoot, RootFSConsumerNetNSRoot: consumerRoot},
		clusterID: request.ClusterID, nodeID: request.NodeID, nodeUID: request.NodeUID,
	}
	require.NoError(t, daemon.RegisterRuntimeSlot(t.Context(), RuntimeSlotRegistration{
		Version: RuntimeSlotJournalVersion, SlotID: request.SlotID, ClusterID: request.ClusterID,
		AllocationID: request.AllocationID, NodeID: request.NodeID, NodeBootID: request.NodeBootID,
		NetNSPath: netnsPath, NetNSIdentity: netnsIdentity, NetworkChain: consumer.NetworkChain,
		RunscContainerID: request.RunscContainerID, StableMount: stableMount, StableMountID: stableMountID,
		MountNamespaceID: hostMountNamespace,
	}))

	proof, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, proof.Validate())
	require.Equal(t, request.WriterAuthorityDigest, proof.RootFSProofDigest)
	require.Equal(t, 1, runtime.retireCalls)
	require.Zero(t, runtime.localCalls)
}

func TestNodeRuntimeRejectsJournalFallbackWhenAnotherWriterOwnsSlot(t *testing.T) {
	request := testNodeCleanupRequest()
	request.WriterRetireKind = protocol.WriterRetireKindPlannedPublish
	runtime := &cleanupRootFSRuntime{
		fakeRootFSRuntime: &fakeRootFSRuntime{},
		recovery: []rootfssession.RecoverySession{{
			Stage: rootfshandoff.StageRequest{Identity: rootfshandoff.Identity{
				SlotNonce: request.SlotID, PodUID: request.AllocationID,
				WriterGrantID: "another-grant",
			}},
		}},
	}
	runner := newFakeRunsc()
	daemon := &nodeRuntime{
		runtime: runtime, runner: runner, mounter: &fakeMounter{}, runtimeSlotNetwork: newFakeCtldNetwork(t).client,
		clusterID: request.ClusterID, nodeID: request.NodeID, nodeUID: request.NodeUID,
	}

	_, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Empty(t, runner.callsSnapshot())
}

func TestNodeRuntimeCleansAndReplaysGrantlessJournaledSlot(t *testing.T) {
	consumerRoot := t.TempDir()
	netnsRoot := filepath.Join(consumerRoot, "netns")
	stableMount := filepath.Join(consumerRoot, "alloc", "rootfs")
	netnsPath := filepath.Join(netnsRoot, "allocation.ns")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	require.NoError(t, os.MkdirAll(netnsRoot, 0o755))
	require.NoError(t, os.WriteFile(netnsPath, []byte("netns"), 0o600))
	netnsIdentity, err := networkNamespaceIdentity(netnsPath)
	require.NoError(t, err)
	stableMountID, err := stableMountIdentity(stableMount)
	require.NoError(t, err)
	mountNamespaceID, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	containerID := protocol.NomadRunscContainerID("slot-warm")
	registration := RuntimeSlotRegistration{
		Version: RuntimeSlotJournalVersion, SlotID: "slot-warm", ClusterID: "cluster-1",
		AllocationID: "allocation-warm", NodeID: "node-1", NodeBootID: "boot-1",
		NetNSPath: netnsPath, NetNSIdentity: netnsIdentity,
		NetworkChain: networkChainName(containerID), RunscContainerID: containerID,
		StableMount: stableMount, StableMountID: stableMountID, MountNamespaceID: mountNamespaceID,
	}
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	network := newFakeCtldNetwork(t)
	daemon := &nodeRuntime{
		runtime: &fakeRootFSRuntime{}, runner: runner, mounter: &fakeMounter{}, runtimeSlotNetwork: network.client, journal: journal,
		config:    Config{RootFSConsumerMountRoot: consumerRoot, RootFSConsumerNetNSRoot: netnsRoot},
		clusterID: registration.ClusterID, nodeID: registration.NodeID, nodeUID: "node-uid-1",
	}
	wrongMount := registration
	wrongMount.StableMountID = "mount-v1:0:0"
	require.ErrorIs(t, daemon.RegisterRuntimeSlot(t.Context(), wrongMount), errdefs.ErrFailedPrecondition)
	wrongNamespace := registration
	wrongNamespace.MountNamespaceID = "mnt:[0]"
	require.ErrorIs(t, daemon.RegisterRuntimeSlot(t.Context(), wrongNamespace), errdefs.ErrFailedPrecondition)
	require.NoError(t, daemon.RegisterRuntimeSlot(t.Context(), registration))
	request := protocol.NodeCleanupControlRequest{
		OperationID: "cleanup-warm", SlotID: registration.SlotID, ClusterID: registration.ClusterID,
		AllocationID: registration.AllocationID, NodeID: registration.NodeID, NodeUID: "node-uid-1",
		NodeBootID: registration.NodeBootID, NetNSIdentity: registration.NetNSIdentity,
		RunscContainerID: registration.RunscContainerID,
	}

	first, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, first.Validate())
	require.Empty(t, first.RootFSOperationID)
	require.Equal(t, []string{"kill:KILL", "delete:force", "state"}, runner.callsSnapshot())
	cleanups := network.cleanupCount()
	require.Equal(t, 1, cleanups)

	// A completed proof is durable data and remains replayable even when the
	// destructive runtime adapters are temporarily unavailable.
	daemon.runner = nil
	daemon.mounter = nil
	daemon.runtimeSlotNetwork = nil
	second, err := daemon.CleanupRuntimeSlot(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, []string{"kill:KILL", "delete:force", "state"}, runner.callsSnapshot())
	cleanups = network.cleanupCount()
	require.Equal(t, 1, cleanups)

	changed := request
	changed.NodeBootID = "another-boot"
	_, err = daemon.CleanupRuntimeSlot(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestNodeRuntimeBuildsCtldPrepareFromDurableJournal(t *testing.T) {
	root := t.TempDir()
	netnsRoot := filepath.Join(root, "netns")
	stableMount := filepath.Join(root, "alloc", "rootfs")
	netnsPath := filepath.Join(netnsRoot, "allocation.ns")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	require.NoError(t, os.MkdirAll(netnsRoot, 0o755))
	require.NoError(t, os.WriteFile(netnsPath, []byte("netns"), 0o600))
	netnsIdentity, err := networkNamespaceIdentity(netnsPath)
	require.NoError(t, err)
	stableMountID, err := stableMountIdentity(stableMount)
	require.NoError(t, err)
	mountNamespaceID, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	journal, err := newRuntimeSlotJournal(filepath.Join(root, "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	socket := filepath.Join(root, "ctld-network.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(socket, 0o600))
	registrations := make(chan protocol.RuntimeSlotNetworkRegistrationRequest, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != protocol.RuntimeSlotNetworkRegisterPath {
			http.NotFound(writer, request)
			return
		}
		var registration protocol.RuntimeSlotNetworkRegistrationRequest
		if err := json.NewDecoder(request.Body).Decode(&registration); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		registrations <- registration
		_ = json.NewEncoder(writer).Encode(protocol.RuntimeSlotNetworkRegistrationResponse{NetworkPolicyApplied: true})
	})}
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	go func() { _ = server.Serve(listener) }()
	client, err := protocol.NewRuntimeSlotNetworkClient(socket, time.Second)
	require.NoError(t, err)
	containerID := protocol.NomadRunscContainerID("slot-1")
	registration := RuntimeSlotRegistration{
		Version: RuntimeSlotJournalVersion, SlotID: "slot-1", ClusterID: "cluster-1",
		AllocationID: "allocation-1", NodeID: "node-1", NodeBootID: "boot-1",
		NetNSPath: netnsPath, NetNSIdentity: netnsIdentity, NetworkChain: networkChainName(containerID),
		RunscContainerID: containerID, StableMount: stableMount, StableMountID: stableMountID,
		MountNamespaceID: mountNamespaceID,
	}
	daemon := &nodeRuntime{
		journal: journal, runtimeSlotNetwork: client,
		config: Config{
			RootFSConsumerMountRoot: root, RootFSConsumerNetNSRoot: netnsRoot,
		},
		clusterID: registration.ClusterID, nodeID: registration.NodeID, nodeUID: "node-uid-1",
	}
	require.NoError(t, daemon.RegisterRuntimeSlot(t.Context(), registration))
	ctldRegistration := <-registrations
	require.Equal(t, registration.SlotID, ctldRegistration.SlotID)
	require.Equal(t, registration.NetNSIdentity, ctldRegistration.NetNSIdentity)
	require.Equal(t, "allocation.ns", ctldRegistration.NetNSRelativePath)
	policy := `{"version":"v1","sandboxId":"sandbox-1","teamId":"team-1","mode":"block-all"}`
	request := protocol.NodeNetworkPrepareControlRequest{
		OperationID: "operation-1", ClaimID: "claim-1", SlotID: registration.SlotID,
		ClusterID: registration.ClusterID, AllocationID: registration.AllocationID,
		NodeID: registration.NodeID, NodeUID: "node-uid-1", NodeBootID: registration.NodeBootID,
		NetNSIdentity: registration.NetNSIdentity, NetworkPolicy: policy,
		PolicyDigest: protocol.NetworkPolicyDigest(policy),
	}
	local, err := daemon.runtimeSlotNetworkPrepareRequest(request)
	require.NoError(t, err)
	require.Equal(t, request, local.Request)
	require.Equal(t, "allocation.ns", local.NetNSRelativePath)
	changed := request
	changed.AllocationID = "another-allocation"
	_, err = daemon.runtimeSlotNetworkPrepareRequest(changed)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestNodeRuntimeDelegatesPolicyAbsenceToCtld(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-owned Unix socket test requires root")
	}
	directory := t.TempDir()
	socket := filepath.Join(directory, "ctld-network.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(socket, 0o600))
	request := testNodeCleanupRequest()
	netnsPath := filepath.Join(directory, "allocation.ns")
	require.NoError(t, os.WriteFile(netnsPath, []byte("netns"), 0o600))
	netnsIdentity, err := networkNamespaceIdentity(netnsPath)
	require.NoError(t, err)
	request.NetNSIdentity = netnsIdentity
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, httpRequest *http.Request) {
		var received protocol.NodeCleanupControlRequest
		if err := json.NewDecoder(httpRequest.Body).Decode(&received); err != nil || received != request {
			http.Error(writer, "unexpected cleanup", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(writer).Encode(protocol.RuntimeSlotNetworkCleanupResponse{NetworkPolicyAbsent: true})
	})}
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	go func() { _ = server.Serve(listener) }()
	client, err := protocol.NewRuntimeSlotNetworkClient(socket, time.Second)
	require.NoError(t, err)
	daemon := &nodeRuntime{
		runtimeSlotNetwork: client,
		config:             Config{RootFSConsumerNetNSRoot: directory},
	}
	consumer := &rootfssession.ConsumerRegistration{NetNSPath: netnsPath, NetNSIdentity: request.NetNSIdentity}
	require.NoError(t, daemon.cleanupRuntimeSlotNetwork(t.Context(), request, consumer, request.NetNSIdentity, netnsPath))
}

type cleanupRootFSRuntime struct {
	*fakeRootFSRuntime
	recovery     []rootfssession.RecoverySession
	proof        rootfshandoff.CrashFenceProof
	retireResult *rootfssession.RetireResult
	forkResult   rootfshandoff.RunningForkCheckpointResult
	forkErr      error
	forkStage    rootfshandoff.StageRequest
	forkRequest  rootfshandoff.RunningForkCheckpointRequest
	forkCalls    int
	localCalls   int
	retireCalls  int
	reclaimCalls int
}

func (r *cleanupRootFSRuntime) RecoverySessions() ([]rootfssession.RecoverySession, error) {
	return append([]rootfssession.RecoverySession(nil), r.recovery...), nil
}

func (r *cleanupRootFSRuntime) CaptureRunningFork(
	_ context.Context,
	stage rootfshandoff.StageRequest,
	request rootfshandoff.RunningForkCheckpointRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	r.forkCalls++
	r.forkStage = stage
	r.forkRequest = request
	return r.forkResult, r.forkErr
}

func TestNodeRuntimeCapturesOnlyTheExactLiveRunningForkWriter(t *testing.T) {
	registration := testRuntimeSlotJournalRegistration(t, "slot-running-fork")
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	require.NoError(t, journal.Register(registration))

	stage := rootfshandoff.StageRequest{
		BindingVersion:    rootfshandoff.WriterBindingVersion,
		InitialGeneration: "generation-source-1",
		Identity: rootfshandoff.Identity{
			SlotNonce: registration.SlotID, PodUID: registration.AllocationID,
			NodeUID: "node-uid-1", BootID: registration.NodeBootID,
			RootFSID: "filesystem-1", WriterGrantID: "writer-grant-1", WriterEpoch: 7,
		},
	}
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	request := protocol.NodeRunningForkControlRequest{
		Fork: rootfshandoff.RunningForkCheckpointRequest{
			OperationID: "running-fork-operation-1", SourceSandboxID: "sandbox-source-1",
			TargetSandboxID: "sandbox-target-1", TargetGenerationID: "generation-target-1",
		},
		SourceFilesystemID:  stage.Identity.RootFSID,
		SourceWriterGrantID: stage.Identity.WriterGrantID, SourceWriterEpoch: stage.Identity.WriterEpoch,
		BindingVersion: stage.BindingVersion, BindingDigest: hex.EncodeToString(binding[:]),
		ExpectedSourceGenerationID: stage.InitialGeneration,
	}
	target := protocol.NodeChannelTarget{
		SlotID: registration.SlotID, ClusterID: registration.ClusterID,
		AllocationID: registration.AllocationID, NodeID: registration.NodeID,
		NodeUID: "node-uid-1", NodeBootID: registration.NodeBootID,
	}
	consumer := &rootfssession.ConsumerRegistration{ActiveKey: registration.SlotID}
	runtime := &cleanupRootFSRuntime{
		fakeRootFSRuntime: &fakeRootFSRuntime{},
		recovery:          []rootfssession.RecoverySession{{Stage: stage, Live: true, Consumer: consumer}},
		forkResult: rootfshandoff.RunningForkCheckpointResult{
			Proof: rootfshandoff.RunningForkCheckpointProof{OperationID: request.Fork.OperationID},
		},
	}
	daemon := &nodeRuntime{
		runtime: runtime, journal: journal, clusterID: registration.ClusterID,
		nodeID: registration.NodeID, nodeUID: target.NodeUID,
	}
	checkpoint, err := daemon.CaptureRunningRootFSFork(t.Context(), target, request)
	require.NoError(t, err)
	require.Equal(t, request.Fork.OperationID, checkpoint.Proof.OperationID)
	require.Equal(t, 1, runtime.forkCalls)
	require.Equal(t, stage, runtime.forkStage)
	require.Equal(t, request.Fork, runtime.forkRequest)

	runtime.recovery = append(runtime.recovery, runtime.recovery[0])
	_, err = daemon.CaptureRunningRootFSFork(t.Context(), target, request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Equal(t, 1, runtime.forkCalls)
	runtime.recovery = runtime.recovery[:1]

	staleRequest := request
	staleRequest.SourceWriterGrantID = "stale-writer-grant"
	_, err = daemon.CaptureRunningRootFSFork(t.Context(), target, staleRequest)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	require.Equal(t, 1, runtime.forkCalls)

	staleTarget := target
	staleTarget.NodeBootID = "stale-node-boot"
	_, err = daemon.CaptureRunningRootFSFork(t.Context(), staleTarget, request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Equal(t, 1, runtime.forkCalls)
}

func (r *cleanupRootFSRuntime) Retire(
	ctx context.Context,
	request rootfshandoff.StageRequest,
	operationID string,
) (rootfssession.RetireResult, error) {
	if r.retireResult == nil {
		return r.fakeRootFSRuntime.Retire(ctx, request, operationID)
	}
	r.retireCalls++
	r.recovery = nil
	result := *r.retireResult
	result.DetachProof = append([]byte(nil), r.retireResult.DetachProof...)
	return result, nil
}

func (r *cleanupRootFSRuntime) ReclaimVerifiedTerminal(
	context.Context,
	rootfshandoff.StageRequest,
) error {
	r.reclaimCalls++
	r.recovery = nil
	return nil
}

func (r *cleanupRootFSRuntime) FenceLocalRootFSWriter(
	_ context.Context,
	_ rootfshandoff.StageRequest,
	operationID string,
	_ CrashTaskObservation,
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

func TestNodeRuntimeRejectsNetworkNamespaceSymlinkEscapeBeforeCleanup(t *testing.T) {
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
	daemon := &nodeRuntime{
		runtime: runtime, runner: runner, mounter: &fakeMounter{}, runtimeSlotNetwork: newFakeCtldNetwork(t).client,
		config:    Config{RootFSConsumerMountRoot: consumerRoot, RootFSConsumerNetNSRoot: netnsRoot},
		clusterID: request.ClusterID, nodeID: request.NodeID, nodeUID: request.NodeUID,
	}

	_, err = daemon.CleanupRuntimeSlot(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Empty(t, runner.callsSnapshot())
	require.Zero(t, runtime.localCalls)
}

func testNodeCleanupRequest() protocol.NodeCleanupControlRequest {
	return protocol.NodeCleanupControlRequest{
		OperationID: "cleanup-1", WriterOperationID: "writer-1",
		WriterRetireKind: protocol.WriterRetireKindCrashAbandon, SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
		RunscContainerID: protocol.NomadRunscContainerID("slot-1"), WriterGrantID: "grant-1",
		WriterAuthorityDigest: strings.Repeat("ab", sha256.Size),
	}
}

func testLocalCrashProof(
	t *testing.T,
	stage rootfshandoff.StageRequest,
	consumer rootfssession.ConsumerRegistration,
	operationID, hostMountNamespace string,
) rootfshandoff.CrashFenceProof {
	t.Helper()
	bindingDigest, err := stage.BindingDigest()
	require.NoError(t, err)
	binding := hex.EncodeToString(bindingDigest[:])
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

func testUnboundLocalCrashProof(
	t *testing.T,
	stage rootfshandoff.StageRequest,
	operationID, hostMountNamespace string,
) rootfshandoff.CrashFenceProof {
	t.Helper()
	bindingDigest, err := stage.BindingDigest()
	require.NoError(t, err)
	binding := hex.EncodeToString(bindingDigest[:])
	observedAt := time.Unix(1_700_000_000, 0).UTC().Format(time.RFC3339Nano)
	session := rootfshandoff.CrashFenceSessionObservation{
		Parent: stage.Parent, RootFSID: stage.Identity.RootFSID, WriterEpoch: stage.Identity.WriterEpoch,
		OperationID: operationID, BindingDigest: binding, SessionState: rootfshandoff.StateTombstoned,
		BranchPath: "/var/lib/sandbox0/branch-unconsumed", NBDPoolAbsent: true,
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
		SlotNonce: stage.Identity.SlotNonce, ActiveKey: stage.Identity.ClaimID,
		ContainerAbsent: true, TaskAbsent: true, FrontendSnapshotAbsent: true, StableMountAbsent: true,
		SnapshotterState: rootfshandoff.StateTombstoned, Session: session, ObservedAt: observedAt,
	}
}

func TestNodeRuntimeConvergesDurablePlannedAndCrashIntents(t *testing.T) {
	runtime := &fakeRootFSRuntime{}
	daemon := &nodeRuntime{runtime: runtime}
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
