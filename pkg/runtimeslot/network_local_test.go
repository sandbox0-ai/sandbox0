package runtimeslot

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

func TestRuntimeSlotNetworkClientUsesSecureExactUnixProtocol(t *testing.T) {
	directory := t.TempDir()
	socket := filepath.Join(directory, "ctld-network.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	policy := `{"mode":"block-all"}`
	control := testNodeChannelNetworkPrepare()
	control.NetworkPolicy = policy
	control.PolicyDigest = NetworkPolicyDigest(policy)
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case RuntimeSlotNetworkHealthPath:
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte(`{}`))
		case RuntimeSlotNetworkRegisterPath:
			_ = json.NewEncoder(writer).Encode(RuntimeSlotNetworkRegistrationResponse{NetworkPolicyApplied: true})
		case RuntimeSlotNetworkPreparePath:
			var local RuntimeSlotNetworkPrepareRequest
			if err := json.NewDecoder(request.Body).Decode(&local); err != nil || local.Request != control || local.NetNSRelativePath != "alloc-1" {
				http.Error(writer, "unexpected prepare", http.StatusBadRequest)
				return
			}
			token := rootfshandoff.NetworkPolicyToken{
				AllocationID: control.AllocationID, NetworkIncarnationID: RuntimeSlotNetworkIncarnationID(control), ClaimID: control.ClaimID,
				NetworkEpoch: 1, PolicyDigest: control.PolicyDigest, SourceIP: "192.0.2.2",
				CtldGeneration: "ctld-1", NetNSIdentity: control.NetNSIdentity,
			}
			_ = json.NewEncoder(writer).Encode(token)
		case RuntimeSlotNetworkCleanupPath:
			_ = json.NewEncoder(writer).Encode(RuntimeSlotNetworkCleanupResponse{NetworkPolicyAbsent: true})
		default:
			http.NotFound(writer, request)
		}
	})}
	defer server.Close()
	go func() { _ = server.Serve(listener) }()

	client, err := NewRuntimeSlotNetworkClient(socket, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	registration := RuntimeSlotNetworkRegistrationRequest{
		SlotID: control.SlotID, ClusterID: control.ClusterID, AllocationID: control.AllocationID,
		NodeID: control.NodeID, NodeUID: control.NodeUID, NodeBootID: control.NodeBootID,
		NetNSIdentity: control.NetNSIdentity, NetNSRelativePath: "alloc-1",
	}
	if err := client.Register(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	token, err := client.Prepare(t.Context(), RuntimeSlotNetworkPrepareRequest{
		Request: control, NetNSRelativePath: "alloc-1",
	})
	if err != nil || token.PolicyDigest != control.PolicyDigest {
		t.Fatalf("Prepare() = %+v, %v", token, err)
	}
	if err := client.Cleanup(t.Context(), testNodeChannelCleanupRequest()); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeSlotNetworkClientRejectsUnsafeSocketAndCrossRequestToken(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.sock")
	listener, err := net.Listen("unix", target)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(target, 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(directory, "link.sock")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	client, err := NewRuntimeSlotNetworkClient(symlink, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(t.Context()); !errdefs.IsPermissionDenied(err) {
		t.Fatalf("symlink socket error = %v", err)
	}

	_ = listener.Close()
	secondListener, err := net.Listen("unix", target)
	if err != nil {
		t.Fatal(err)
	}
	defer secondListener.Close()
	if err := os.Chmod(target, 0o666); err != nil {
		t.Fatal(err)
	}
	client, err = NewRuntimeSlotNetworkClient(target, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(context.Background()); !errdefs.IsPermissionDenied(err) {
		t.Fatalf("insecure socket error = %v", err)
	}
}

func TestRuntimeSlotNetworkPrepareRequestRejectsPathEscape(t *testing.T) {
	request := RuntimeSlotNetworkPrepareRequest{
		Request: testNodeChannelNetworkPrepare(), NetNSRelativePath: "alloc-1",
	}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"", ".", "../alloc-1", "/var/run/netns/alloc-1", "a/../alloc-1", strings.Repeat("x", 4097)} {
		request.NetNSRelativePath = path
		if err := request.Validate(); err == nil {
			t.Fatalf("path %q was accepted", path)
		}
	}
}

func TestRuntimeSlotNetworkRegistrationBindsPrepareAndPath(t *testing.T) {
	prepare := testNodeChannelNetworkPrepare()
	registration := RuntimeSlotNetworkRegistrationRequest{
		SlotID: prepare.SlotID, ClusterID: prepare.ClusterID, AllocationID: prepare.AllocationID,
		NodeID: prepare.NodeID, NodeUID: prepare.NodeUID, NodeBootID: prepare.NodeBootID,
		NetNSIdentity: prepare.NetNSIdentity, NetNSRelativePath: "alloc-1",
	}
	if err := registration.Validate(); err != nil || !registration.MatchesPrepare(prepare) {
		t.Fatalf("registration = %+v, %v", registration, err)
	}
	prepare.NodeBootID = "another-boot"
	if registration.MatchesPrepare(prepare) {
		t.Fatal("registration matched another physical incarnation")
	}
}
