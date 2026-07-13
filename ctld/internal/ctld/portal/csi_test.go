package portal

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestCSIServerStartCreatesUsableSocketBeforeReturning(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "csi.sock")
	server := NewCSIServer("node-a", &Manager{})
	serveErrors, err := server.Start(socket)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer server.Stop()
	if _, err := os.Stat(socket); err != nil {
		t.Fatalf("CSI socket is unavailable after Start(): %v", err)
	}

	connection, err := grpc.NewClient(
		"passthrough:///ctld-csi",
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
	info, err := csi.NewIdentityClient(connection).GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo() error = %v", err)
	}
	if info.GetName() != "volume.sandbox0.ai" {
		t.Fatalf("GetPluginInfo() name = %q", info.GetName())
	}

	select {
	case err := <-serveErrors:
		t.Fatalf("CSI server stopped unexpectedly: %v", err)
	default:
	}
}
