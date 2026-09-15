package nomadruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type heldNetworkRegistration struct {
	*fakeCtldNetwork
	entered chan struct{}
	release chan struct{}
}

func (n *heldNetworkRegistration) Register(ctx context.Context, request protocol.RuntimeSlotNetworkRegistrationRequest) error {
	select {
	case n.entered <- struct{}{}:
	default:
	}
	select {
	case <-n.release:
		return n.fakeCtldNetwork.Register(ctx, request)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// A timed-out registration may still be applying its default-deny policy when
// the region begins terminal cleanup. The resulting proof must not precede a
// late policy registration, including when the driver retries after recovery.
func TestRuntimeSlotCleanupFencesInFlightNetworkRegistration(t *testing.T) {
	root := t.TempDir()
	netnsRoot := filepath.Join(root, "netns")
	stableMount := filepath.Join(root, "alloc", "rootfs")
	netnsPath := filepath.Join(netnsRoot, "allocation.ns")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	require.NoError(t, os.MkdirAll(netnsRoot, 0o755))
	require.NoError(t, os.WriteFile(netnsPath, []byte("netns"), 0o600))
	netnsIdentity, err := networkNamespaceIdentity(netnsPath)
	require.NoError(t, err)
	mountID, err := stableMountIdentity(stableMount)
	require.NoError(t, err)
	mountNS, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	journal, err := newRuntimeSlotJournal(filepath.Join(root, "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	container := protocol.NomadRunscContainerID("slot-register-cleanup")
	registration := RuntimeSlotRegistration{
		Version: RuntimeSlotJournalVersion, SlotID: "slot-register-cleanup", ClusterID: "cluster-1",
		AllocationID: "allocation-1", NodeID: "node-1", NodeBootID: "boot-1",
		NetNSPath: netnsPath, NetNSIdentity: netnsIdentity, NetworkChain: networkChainName(container), RunscContainerID: container,
		StableMount: stableMount, StableMountID: mountID, MountNamespaceID: mountNS,
	}
	network := &heldNetworkRegistration{fakeCtldNetwork: newFakeCtldNetwork(t), entered: make(chan struct{}, 1), release: make(chan struct{})}
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	daemon := &nodeRuntime{
		runtime: &fakeRootFSRuntime{}, runner: runner, mounter: &fakeMounter{}, runtimeSlotNetwork: network, journal: journal,
		config:    Config{RootFSConsumerMountRoot: root, RootFSConsumerNetNSRoot: netnsRoot},
		clusterID: registration.ClusterID, nodeID: registration.NodeID, nodeUID: "node-uid-1",
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- daemon.RegisterRuntimeSlot(ctx, registration) }()
	select {
	case <-network.entered:
	case <-ctx.Done():
		t.Fatal("network registration did not start")
	}
	request := protocol.NodeCleanupControlRequest{
		OperationID: "cleanup-warm", SlotID: registration.SlotID, ClusterID: registration.ClusterID,
		AllocationID: registration.AllocationID, NodeID: registration.NodeID, NodeUID: "node-uid-1",
		NodeBootID: registration.NodeBootID, NetNSIdentity: registration.NetNSIdentity, RunscContainerID: container,
	}
	proof, err := daemon.CleanupRuntimeSlot(ctx, request)
	// Capture the late completion before assertions so a regression cannot leave
	// a test goroutine blocked while its journal is being closed by cleanup.
	close(network.release)
	registrationErr := <-done
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	require.Empty(t, proof.ProofDigest, "cleanup cannot prove absence ahead of a pending registration")
	require.ErrorIs(t, registrationErr, errdefs.ErrFailedPrecondition)
	record, err := journal.Get(registration.SlotID)
	require.NoError(t, err)
	require.NotNil(t, record.Cleanup, "the region's cleanup intent remains durable for retry")
	require.Nil(t, record.Proof)
	require.Zero(t, network.cleanupCount())
	proof, err = daemon.CleanupRuntimeSlot(ctx, request)
	require.NoError(t, err)
	require.NoError(t, proof.Validate())
	require.Equal(t, 1, network.cleanupCount())
	require.ErrorIs(t, daemon.RegisterRuntimeSlot(ctx, registration), errdefs.ErrFailedPrecondition)
	require.Len(t, network.registrationsSnapshot(), 1, "late retry must not recreate a terminal network registration")
}
