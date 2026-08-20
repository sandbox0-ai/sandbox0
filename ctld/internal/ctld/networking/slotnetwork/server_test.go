package slotnetwork

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func TestControlServerAndClientApplyThenRemoveExactPolicy(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-owned Unix socket test requires root")
	}
	directory := t.TempDir()
	netnsRoot := filepath.Join(directory, "netns")
	if err := ensureDirectory(netnsRoot); err != nil {
		t.Fatal(err)
	}
	registry := newTestRegistry(t, filepath.Join(directory, "network.db"), netnsRoot,
		&fakeNamespaceInspector{podIP: "192.0.2.8"}, time.Hour)
	defer registry.Close()
	autoAcknowledge(registry)
	socket := filepath.Join(directory, "ctld-network.sock")
	server, err := StartControlServer(socket, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(t.Context())
	client, err := protocol.NewRuntimeSlotNetworkClient(socket, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := client.Register(t.Context(), testRegistrationRequest()); err != nil {
		t.Fatal(err)
	}
	token, err := client.Prepare(t.Context(), testPrepareRequest())
	if err != nil || token.PodIP != "192.0.2.8" {
		t.Fatalf("Prepare() = %+v, %v", token, err)
	}
	if err := client.Cleanup(t.Context(), testCleanupRequest()); err != nil {
		t.Fatal(err)
	}
}

func TestControlServerRefusesReachablePredecessor(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-owned Unix socket test requires root")
	}
	directory := t.TempDir()
	netnsRoot := filepath.Join(directory, "netns")
	if err := ensureDirectory(netnsRoot); err != nil {
		t.Fatal(err)
	}
	registry := newTestRegistry(t, filepath.Join(directory, "network.db"), netnsRoot,
		&fakeNamespaceInspector{podIP: "192.0.2.8"}, time.Hour)
	defer registry.Close()
	socket := filepath.Join(directory, "ctld-network.sock")
	first, err := StartControlServer(socket, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Shutdown(t.Context())
	if _, err := StartControlServer(socket, registry); err == nil {
		t.Fatal("second live control server was accepted")
	}
}

func TestControlServerRejectsWritableSocketDirectory(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-owned Unix socket test requires root")
	}
	directory := t.TempDir()
	netnsRoot := filepath.Join(directory, "netns")
	if err := ensureDirectory(netnsRoot); err != nil {
		t.Fatal(err)
	}
	registry := newTestRegistry(t, filepath.Join(directory, "network.db"), netnsRoot,
		&fakeNamespaceInspector{podIP: "192.0.2.8"}, time.Hour)
	defer registry.Close()
	socketRoot := filepath.Join(directory, "unsafe-sockets")
	if err := os.Mkdir(socketRoot, 0o770); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketRoot, 0o770); err != nil {
		t.Fatal(err)
	}
	if _, err := StartControlServer(filepath.Join(socketRoot, "ctld-network.sock"), registry); !errdefs.IsPermissionDenied(err) {
		t.Fatalf("writable socket directory error = %v", err)
	}
}
