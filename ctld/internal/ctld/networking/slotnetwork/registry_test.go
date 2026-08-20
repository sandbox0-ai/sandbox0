package slotnetwork

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type fakeNamespaceInspector struct {
	mu       sync.Mutex
	paths    []string
	identity []string
	podIP    string
	err      error
}

func (i *fakeNamespaceInspector) Inspect(path, identity string) (string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.paths = append(i.paths, path)
	i.identity = append(i.identity, identity)
	return i.podIP, i.err
}

func TestRegistryPersistsExactPrepareAndCleanupAcrossHAReopen(t *testing.T) {
	directory := t.TempDir()
	netnsRoot := filepath.Join(directory, "netns")
	if err := ensureDirectory(netnsRoot); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(directory, "network.db")
	inspector := &fakeNamespaceInspector{podIP: "192.0.2.8"}
	registry := newTestRegistry(t, state, netnsRoot, inspector, time.Hour)
	autoAcknowledge(registry)
	registration := testRegistrationRequest()
	if err := registry.Register(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	request := testPrepareRequest()
	first, err := registry.Prepare(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Prepare(t.Context(), request)
	if err != nil || second != first {
		t.Fatalf("exact retry = %+v, %v; want %+v", second, err, first)
	}
	inspector.mu.Lock()
	if len(inspector.paths) != 2 || inspector.paths[0] != filepath.Join(netnsRoot, request.NetNSRelativePath) ||
		inspector.paths[1] != filepath.Join(netnsRoot, request.NetNSRelativePath) ||
		inspector.identity[0] != request.Request.NetNSIdentity || inspector.identity[1] != request.Request.NetNSIdentity {
		t.Fatalf("namespace inspections = %v, %v", inspector.paths, inspector.identity)
	}
	inspector.mu.Unlock()
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := newTestRegistry(t, state, netnsRoot, &fakeNamespaceInspector{err: errors.New("must not inspect exact replay")}, time.Hour)
	defer reopened.Close()
	autoAcknowledge(reopened)
	if err := reopened.Register(t.Context(), registration); err != nil {
		t.Fatalf("HA registration replay = %v", err)
	}
	replayed, err := reopened.Prepare(t.Context(), request)
	if err != nil || replayed != first {
		t.Fatalf("HA replay = %+v, %v; want %+v", replayed, err, first)
	}
	sandboxes, _, err := reopened.Snapshot()
	if err != nil || len(sandboxes) != 1 || sandboxes[0].PodIP != first.PodIP ||
		sandboxes[0].NetworkPolicyHash != request.Request.PolicyDigest {
		t.Fatalf("snapshot = %+v, %v", sandboxes, err)
	}
	cleanup := testCleanupRequest()
	if err := reopened.Cleanup(t.Context(), cleanup); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Cleanup(t.Context(), cleanup); err != nil {
		t.Fatalf("cleanup retry = %v", err)
	}
	sandboxes, _, err = reopened.Snapshot()
	if err != nil || len(sandboxes) != 0 {
		t.Fatalf("terminal snapshot = %+v, %v", sandboxes, err)
	}
	changed := cleanup
	changed.OperationID = "another-cleanup"
	if err := reopened.Cleanup(t.Context(), changed); !errdefs.IsAlreadyExists(err) {
		t.Fatalf("changed cleanup error = %v", err)
	}
}

func TestRegistryDoesNotAcknowledgeBeforeRedirectSync(t *testing.T) {
	directory := t.TempDir()
	netnsRoot := filepath.Join(directory, "netns")
	if err := ensureDirectory(netnsRoot); err != nil {
		t.Fatal(err)
	}
	registry := newTestRegistry(t, filepath.Join(directory, "network.db"), netnsRoot,
		&fakeNamespaceInspector{podIP: "192.0.2.8"}, time.Hour)
	defer registry.Close()
	autoAcknowledge(registry)
	if err := registry.Register(t.Context(), testRegistrationRequest()); err != nil {
		t.Fatal(err)
	}
	registry.SetNotify(nil)
	request := testPrepareRequest()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := registry.Prepare(ctx, request); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unacknowledged prepare error = %v", err)
	}
	sandboxes, revision, err := registry.Snapshot()
	if err != nil || len(sandboxes) != 1 {
		t.Fatalf("durable pending snapshot = %+v, %v", sandboxes, err)
	}
	registry.Acknowledge(revision)
	if _, err := registry.Prepare(t.Context(), request); err != nil {
		t.Fatalf("acknowledged exact retry = %v", err)
	}
}

func TestRegistryRequiresWarmRegistrationAndUniqueClaimOperation(t *testing.T) {
	directory := t.TempDir()
	netnsRoot := filepath.Join(directory, "netns")
	if err := ensureDirectory(netnsRoot); err != nil {
		t.Fatal(err)
	}
	registry := newTestRegistry(t, filepath.Join(directory, "network.db"), netnsRoot,
		&fakeNamespaceInspector{podIP: "192.0.2.8"}, time.Hour)
	defer registry.Close()
	autoAcknowledge(registry)
	prepare := testPrepareRequest()
	if _, err := registry.Prepare(t.Context(), prepare); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("unregistered prepare error = %v", err)
	}
	if err := registry.Register(t.Context(), testRegistrationRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Prepare(t.Context(), prepare); err != nil {
		t.Fatal(err)
	}

	secondRegistration := testRegistrationRequest()
	secondRegistration.SlotID = "slot-2"
	secondRegistration.AllocationID = "allocation-2"
	secondRegistration.NetNSRelativePath = "allocation-2"
	if err := registry.Register(t.Context(), secondRegistration); err != nil {
		t.Fatal(err)
	}
	secondPrepare := prepare
	secondPrepare.Request.SlotID = secondRegistration.SlotID
	secondPrepare.Request.AllocationID = secondRegistration.AllocationID
	secondPrepare.NetNSRelativePath = secondRegistration.NetNSRelativePath
	if _, err := registry.Prepare(t.Context(), secondPrepare); !errdefs.IsAlreadyExists(err) {
		t.Fatalf("reused operation error = %v", err)
	}
}

func TestRegistryRejectsChangedAndLegacyPolicyRequests(t *testing.T) {
	directory := t.TempDir()
	netnsRoot := filepath.Join(directory, "netns")
	if err := ensureDirectory(netnsRoot); err != nil {
		t.Fatal(err)
	}
	registry := newTestRegistry(t, filepath.Join(directory, "network.db"), netnsRoot,
		&fakeNamespaceInspector{podIP: "192.0.2.8"}, time.Hour)
	defer registry.Close()
	autoAcknowledge(registry)
	if err := registry.Register(t.Context(), testRegistrationRequest()); err != nil {
		t.Fatal(err)
	}
	request := testPrepareRequest()
	if _, err := registry.Prepare(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	corrupt := registry.entries[request.Request.SlotID].record
	registry.mu.Unlock()
	corrupt.TeamID = "another-team"
	if err := validateRecord(corrupt); err == nil {
		t.Fatal("claimed record with policy identity drift was accepted")
	}
	changed := request
	changed.Request.NetworkPolicy = `{"version":"v1","sandboxId":"sandbox-1","teamId":"team-1","mode":"allow-all"}`
	changed.Request.PolicyDigest = protocol.NetworkPolicyDigest(changed.Request.NetworkPolicy)
	if _, err := registry.Prepare(t.Context(), changed); !errdefs.IsAlreadyExists(err) {
		t.Fatalf("changed prepare error = %v", err)
	}
	legacy := request
	legacy.Request.SlotID = "slot-legacy"
	legacy.Request.OperationID = "operation-legacy"
	legacy.Request.AllocationID = "allocation-legacy"
	legacy.Request.NetworkPolicy = `{"mode":"block-all","allow":[]}`
	legacy.Request.PolicyDigest = protocol.NetworkPolicyDigest(legacy.Request.NetworkPolicy)
	if _, err := registry.Prepare(t.Context(), legacy); !errdefs.IsInvalidArgument(err) {
		t.Fatalf("legacy policy error = %v", err)
	}
	for _, raw := range []string{
		`{"version":"v2","sandboxId":"sandbox-1","teamId":"team-1","mode":"block-all"}`,
		`{"version":"v1","sandboxId":"","teamId":"team-1","mode":"block-all"}`,
		`{"version":"v1","sandboxId":"sandbox-1","teamId":"team-1","mode":"unknown"}`,
	} {
		invalid := testPrepareRequest()
		invalid.Request.NetworkPolicy = raw
		invalid.Request.PolicyDigest = protocol.NetworkPolicyDigest(raw)
		if _, err := validatePolicy(invalid); !errdefs.IsInvalidArgument(err) {
			t.Fatalf("invalid policy %q error = %v", raw, err)
		}
	}
}

func TestRegistryPrunesOnlyExpiredTerminalReplay(t *testing.T) {
	directory := t.TempDir()
	netnsRoot := filepath.Join(directory, "netns")
	if err := ensureDirectory(netnsRoot); err != nil {
		t.Fatal(err)
	}
	registry := newTestRegistry(t, filepath.Join(directory, "network.db"), netnsRoot,
		&fakeNamespaceInspector{podIP: "192.0.2.8"}, time.Nanosecond)
	defer registry.Close()
	autoAcknowledge(registry)
	if err := registry.Register(t.Context(), testRegistrationRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Prepare(t.Context(), testPrepareRequest()); err != nil {
		t.Fatal(err)
	}
	if err := registry.Cleanup(t.Context(), testCleanupRequest()); err != nil {
		t.Fatal(err)
	}
	if deleted, err := registry.Prune(time.Now().Add(time.Second)); err != nil || deleted != 1 {
		t.Fatalf("Prune() = %d, %v", deleted, err)
	}
	if err := registry.Cleanup(t.Context(), testCleanupRequest()); err != nil {
		t.Fatalf("grantless cleanup after prune = %v", err)
	}
}

func TestRegistryRejectsWritableRootsAndUnsafeStateFile(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-owned registry path test requires root")
	}
	tests := []struct {
		name  string
		setup func(string) (Config, error)
	}{
		{
			name: "writable netns root",
			setup: func(directory string) (Config, error) {
				netnsRoot := filepath.Join(directory, "netns")
				if err := os.Mkdir(netnsRoot, 0o770); err != nil {
					return Config{}, err
				}
				if err := os.Chmod(netnsRoot, 0o770); err != nil {
					return Config{}, err
				}
				return Config{StatePath: filepath.Join(directory, "network.db"), NetNSRoot: netnsRoot, NodeName: "node-1"}, nil
			},
		},
		{
			name: "writable state root",
			setup: func(directory string) (Config, error) {
				netnsRoot := filepath.Join(directory, "netns")
				stateRoot := filepath.Join(directory, "state")
				if err := os.Mkdir(netnsRoot, 0o750); err != nil {
					return Config{}, err
				}
				if err := os.Mkdir(stateRoot, 0o770); err != nil {
					return Config{}, err
				}
				if err := os.Chmod(stateRoot, 0o770); err != nil {
					return Config{}, err
				}
				return Config{StatePath: filepath.Join(stateRoot, "network.db"), NetNSRoot: netnsRoot, NodeName: "node-1"}, nil
			},
		},
		{
			name: "unsafe state file",
			setup: func(directory string) (Config, error) {
				netnsRoot := filepath.Join(directory, "netns")
				statePath := filepath.Join(directory, "network.db")
				if err := os.Mkdir(netnsRoot, 0o750); err != nil {
					return Config{}, err
				}
				if err := os.WriteFile(statePath, nil, 0o640); err != nil {
					return Config{}, err
				}
				if err := os.Chmod(statePath, 0o640); err != nil {
					return Config{}, err
				}
				return Config{StatePath: statePath, NetNSRoot: netnsRoot, NodeName: "node-1"}, nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := test.setup(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			registry, err := NewRegistry(config, &fakeNamespaceInspector{podIP: "192.0.2.8"})
			if registry != nil {
				_ = registry.Close()
			}
			if !errdefs.IsPermissionDenied(err) {
				t.Fatalf("unsafe registry path error = %v", err)
			}
		})
	}
}

func newTestRegistry(
	t *testing.T,
	state, netnsRoot string,
	inspector NamespaceInspector,
	retention time.Duration,
) *Registry {
	t.Helper()
	registry, err := NewRegistry(Config{
		StatePath: state, NetNSRoot: netnsRoot, NodeName: "node-1",
		TerminalRetention: retention, MaxRecords: 100,
	}, inspector)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func autoAcknowledge(registry *Registry) {
	registry.SetNotify(func() {
		_, revision, err := registry.Snapshot()
		if err == nil {
			registry.Acknowledge(revision)
		}
	})
}

func testPrepareRequest() protocol.RuntimeSlotNetworkPrepareRequest {
	policy := `{"version":"v1","sandboxId":"sandbox-1","teamId":"team-1","mode":"block-all"}`
	return protocol.RuntimeSlotNetworkPrepareRequest{
		Request: protocol.NodeNetworkPrepareControlRequest{
			OperationID: "operation-1", ClaimID: "claim-1", SlotID: "slot-1",
			ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1",
			NodeUID: "node-uid-1", NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
			NetworkPolicy: policy, PolicyDigest: protocol.NetworkPolicyDigest(policy),
		},
		NetNSRelativePath: "allocation-1",
	}
}

func testRegistrationRequest() protocol.RuntimeSlotNetworkRegistrationRequest {
	return protocol.RuntimeSlotNetworkRegistrationRequest{
		SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		NetNSIdentity: "netns-v1:1:2", NetNSRelativePath: "allocation-1",
	}
}

func testCleanupRequest() protocol.NodeCleanupControlRequest {
	return protocol.NodeCleanupControlRequest{
		OperationID: "cleanup-1", SlotID: "slot-1", ClusterID: "cluster-1",
		AllocationID: "allocation-1", NodeID: "node-1", NodeUID: "node-uid-1",
		NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
		RunscContainerID: protocol.NomadRunscContainerID("slot-1"),
	}
}

func ensureDirectory(path string) error {
	return os.MkdirAll(path, 0o750)
}
