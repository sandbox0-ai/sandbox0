package networking

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/conntrack"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/slotnetwork"
	apiconfig "github.com/sandbox0-ai/sandbox0/pkg/config"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"go.uber.org/zap"
)

type syncTestNamespaceInspector struct{}

func (syncTestNamespaceInspector) Inspect(string, string) (string, error) {
	return "192.0.2.8", nil
}

type syncTestRedirect struct {
	mu    sync.Mutex
	input [][]string
}

func (r *syncTestRedirect) Sync(_ context.Context, sandboxIPs, _ []string) error {
	r.mu.Lock()
	r.input = append(r.input, append([]string(nil), sandboxIPs...))
	r.mu.Unlock()
	return nil
}

func (r *syncTestRedirect) ForceSync(ctx context.Context, sandboxIPs, bypass []string) error {
	return r.Sync(ctx, sandboxIPs, bypass)
}

func (r *syncTestRedirect) Cleanup(context.Context) error { return nil }

func TestSyncRedirectAcknowledgesDurableRuntimeSlotPolicyAndAbsence(t *testing.T) {
	directory := t.TempDir()
	netnsRoot := filepath.Join(directory, "netns")
	if err := os.MkdirAll(netnsRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	registry, err := slotnetwork.NewRegistry(slotnetwork.Config{
		StatePath: filepath.Join(directory, "runtime-slot-network.db"),
		NetNSRoot: netnsRoot, NodeID: "node-1",
	}, syncTestNamespaceInspector{})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	registration := protocol.RuntimeSlotNetworkRegistrationRequest{
		SlotID: "slot-1", ClusterID: "cluster-1", AllocationID: "allocation-1",
		NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
		NetNSIdentity: "netns-v1:1:2", NetNSRelativePath: "allocation-1",
	}
	policyRaw := `{"version":"v1","sandboxId":"sandbox-1","teamId":"team-1","mode":"block-all"}`
	prepare := protocol.RuntimeSlotNetworkPrepareRequest{
		Request: protocol.NodeNetworkPrepareControlRequest{
			OperationID: "operation-1", ClaimID: "claim-1", SlotID: "slot-1",
			ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1",
			NodeUID: "node-uid-1", NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
			NetworkPolicy: policyRaw, PolicyDigest: protocol.NetworkPolicyDigest(policyRaw),
		},
		NetNSRelativePath: "allocation-1",
	}
	registrationResult := make(chan error, 1)
	go func() {
		registrationResult <- registry.Register(t.Context(), registration)
	}()
	waitForRuntimeSlotSnapshot(t, registry, 1)

	store := policy.NewStore(zap.NewNop())
	redirect := &syncTestRedirect{}
	daemon := &Daemon{cfg: &apiconfig.NetworkRuntimeConfig{NodeName: "node-1"}, logger: zap.NewNop()}
	if err := daemon.syncRedirect(
		t.Context(), registry, store, nil, redirect,
		conntrack.NewTracker(), nil, nil, false,
	); err != nil {
		t.Fatal(err)
	}
	if err := <-registrationResult; err != nil {
		t.Fatalf("Register() after warm redirect sync = %v", err)
	}
	warm := store.GetByIP("192.0.2.8")
	if warm == nil || warm.Mode != "block-all" || warm.TeamID != "sandbox0-runtime-slot-warm" {
		t.Fatalf("compiled warm runtime slot policy = %+v", warm)
	}

	prepareResult := make(chan error, 1)
	go func() {
		_, prepareErr := registry.Prepare(t.Context(), prepare)
		prepareResult <- prepareErr
	}()
	waitForRuntimeSlotClaimed(t, registry)
	if err := daemon.syncRedirect(
		t.Context(), registry, store, nil, redirect,
		conntrack.NewTracker(), nil, nil, false,
	); err != nil {
		t.Fatal(err)
	}
	if err := <-prepareResult; err != nil {
		t.Fatalf("Prepare() after redirect sync = %v", err)
	}
	if compiled := store.GetByIP("192.0.2.8"); compiled == nil || compiled.SandboxID != "sandbox-1" || compiled.TeamID != "team-1" {
		t.Fatalf("compiled runtime slot policy = %+v", compiled)
	}

	cleanup := protocol.NodeCleanupControlRequest{
		OperationID: "cleanup-1", SlotID: "slot-1", ClusterID: "cluster-1",
		AllocationID: "allocation-1", NodeID: "node-1", NodeUID: "node-uid-1",
		NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
		RunscContainerID: protocol.NomadRunscContainerID("slot-1"),
	}
	cleanupResult := make(chan error, 1)
	go func() { cleanupResult <- registry.Cleanup(t.Context(), cleanup) }()
	waitForRuntimeSlotSnapshot(t, registry, 0)
	if err := daemon.syncRedirect(
		t.Context(), registry, store, nil, redirect,
		conntrack.NewTracker(), nil, nil, false,
	); err != nil {
		t.Fatal(err)
	}
	if err := <-cleanupResult; err != nil {
		t.Fatalf("Cleanup() after redirect sync = %v", err)
	}
	if compiled := store.GetByIP("192.0.2.8"); compiled != nil {
		t.Fatalf("terminal runtime slot policy remains compiled: %+v", compiled)
	}
	redirect.mu.Lock()
	defer redirect.mu.Unlock()
	if len(redirect.input) != 3 || len(redirect.input[0]) != 1 || redirect.input[0][0] != "192.0.2.8" ||
		len(redirect.input[1]) != 1 || redirect.input[1][0] != "192.0.2.8" || len(redirect.input[2]) != 0 {
		t.Fatalf("redirect inputs = %+v", redirect.input)
	}
}

func waitForRuntimeSlotClaimed(t *testing.T, registry *slotnetwork.Registry) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		stats := registry.Stats()
		if stats.Claimed == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime slot stats = %+v, want one claimed slot", stats)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForRuntimeSlotSnapshot(t *testing.T, registry *slotnetwork.Registry, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		sandboxes, _, err := registry.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if len(sandboxes) == count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime slot snapshot count = %d, want %d", len(sandboxes), count)
		}
		time.Sleep(time.Millisecond)
	}
}
