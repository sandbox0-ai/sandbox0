//go:build linux

package ha

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	ctldregistration "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/kubeletregistration"
	ctldportal "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

func TestKubeletRegistrationSocketFollowsPrimaryLease(t *testing.T) {
	root, err := os.MkdirTemp("", "ctld-ha-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socket := filepath.Join(root, "plugins_registry", "volume.sandbox0.ai-reg.sock")
	config := ctldregistration.Config{
		SocketPath: socket,
		DriverName: "volume.sandbox0.ai",
		Endpoint:   "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock",
	}

	primaryCoordinator := newTestCoordinator(t, root, "a")
	primary, err := primaryCoordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("WaitForPrimary(primary) error = %v", err)
	}
	primary.Replicator.SetSnapshotProvider(func(context.Context, ctldportal.PortalReplicator) error { return nil })
	primaryRegistration, err := ctldregistration.NewServer(config)
	if err != nil {
		t.Fatalf("NewServer(primary) error = %v", err)
	}
	if err := primaryRegistration.Start(); err != nil {
		t.Fatalf("Start(primary registration) error = %v", err)
	}
	registerWithTestKubelet(t, socket)
	if !primaryRegistration.Registered() {
		t.Fatal("primary registration did not receive kubelet acknowledgement")
	}

	standbyCoordinator := newTestCoordinator(t, root, "b")
	standbyCtx, standbyCancel := context.WithCancel(context.Background())
	defer standbyCancel()
	standbyResult := waitForPrimaryAsync(standbyCtx, standbyCoordinator)
	waitForStandbys(t, primary.Replicator, 1)

	primaryRegistration.Stop()
	if err := primary.Close(); err != nil {
		t.Fatalf("Close(primary) error = %v", err)
	}
	promoted := receivePrimary(t, standbyResult)
	defer promoted.Close()

	promotedRegistration, err := ctldregistration.NewServer(config)
	if err != nil {
		t.Fatalf("NewServer(promoted) error = %v", err)
	}
	if err := promotedRegistration.Start(); err != nil {
		t.Fatalf("Start(promoted registration) error = %v", err)
	}
	defer promotedRegistration.Stop()
	registerWithTestKubelet(t, socket)
	if !promotedRegistration.Registered() {
		t.Fatal("promoted registration did not receive kubelet acknowledgement")
	}
}

func registerWithTestKubelet(t *testing.T, socket string) {
	t.Helper()
	connection, err := grpc.NewClient(
		"passthrough:///kubelet-registration",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := registerapi.NewRegistrationClient(connection)
	info, err := client.GetInfo(ctx, &registerapi.InfoRequest{})
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}
	if info.GetName() != "volume.sandbox0.ai" {
		t.Fatalf("GetInfo() name = %q", info.GetName())
	}
	if _, err := client.NotifyRegistrationStatus(ctx, &registerapi.RegistrationStatus{PluginRegistered: true}); err != nil {
		t.Fatalf("NotifyRegistrationStatus() error = %v", err)
	}
}
