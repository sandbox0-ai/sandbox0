package kubeletregistration

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

func TestServerRegistrationLifecycle(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "driver-reg.sock")
	server, err := NewServer(Config{
		SocketPath: socket,
		DriverName: "volume.sandbox0.ai",
		Endpoint:   "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock",
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(server.Stop)

	client, closeClient := newRegistrationClient(t, socket)
	defer closeClient()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := client.GetInfo(ctx, &registerapi.InfoRequest{})
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}
	if info.GetType() != registerapi.CSIPlugin || info.GetName() != "volume.sandbox0.ai" {
		t.Fatalf("GetInfo() = %#v", info)
	}
	if info.GetEndpoint() != "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock" {
		t.Fatalf("GetInfo() endpoint = %q", info.GetEndpoint())
	}
	if len(info.GetSupportedVersions()) != 1 || info.GetSupportedVersions()[0] != "1.0.0" {
		t.Fatalf("GetInfo() supported versions = %#v", info.GetSupportedVersions())
	}

	if server.Registered() {
		t.Fatal("Registered() = true before kubelet notification")
	}
	if _, err := client.NotifyRegistrationStatus(ctx, &registerapi.RegistrationStatus{PluginRegistered: true}); err != nil {
		t.Fatalf("NotifyRegistrationStatus(success) error = %v", err)
	}
	if !server.Registered() {
		t.Fatal("Registered() = false after successful kubelet notification")
	}

	server.Stop()
	if server.Registered() {
		t.Fatal("Registered() = true after Stop()")
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("registration socket still exists after Stop(): %v", err)
	}
}

func TestServerReportsRejectedRegistration(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "driver-reg.sock")
	server, err := NewServer(Config{
		SocketPath: socket,
		DriverName: "volume.sandbox0.ai",
		Endpoint:   "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock",
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer server.Stop()

	client, closeClient := newRegistrationClient(t, socket)
	defer closeClient()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.NotifyRegistrationStatus(ctx, &registerapi.RegistrationStatus{
		PluginRegistered: false,
		Error:            "driver validation failed",
	}); err != nil {
		t.Fatalf("NotifyRegistrationStatus(rejected) error = %v", err)
	}
	select {
	case err := <-server.Errors():
		if err == nil || !strings.Contains(err.Error(), "driver validation failed") {
			t.Fatalf("Errors() = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for registration rejection")
	}
	if server.Registered() {
		t.Fatal("Registered() = true after rejected registration")
	}
}

func TestNewServerValidatesConfig(t *testing.T) {
	tests := []Config{
		{DriverName: "driver", Endpoint: "/csi.sock"},
		{SocketPath: "/registration.sock", Endpoint: "/csi.sock"},
		{SocketPath: "/registration.sock", DriverName: "driver"},
	}
	for _, config := range tests {
		if _, err := NewServer(config); err == nil {
			t.Fatalf("NewServer(%#v) error = nil", config)
		}
	}
}

func newRegistrationClient(t *testing.T, socket string) (registerapi.RegistrationClient, func()) {
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
	return registerapi.NewRegistrationClient(connection), func() { _ = connection.Close() }
}
