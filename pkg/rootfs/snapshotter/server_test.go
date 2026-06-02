package snapshotter

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestServeExposesRewrittenMountsOverGRPC(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "snapshotter.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &fakePrepareClient{
		response: &ctldapi.PrepareRootFSResponse{
			Prepared: true,
			UpperDir: "/s0fs/upper",
			WorkDir:  "/s0fs/work",
		},
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- Serve(ctx, ServerConfig{
			SocketPath: socketPath,
			Base: &fakeSnapshotter{mounts: []mount.Mount{{
				Type:    "overlay",
				Source:  "overlay",
				Options: []string{"lowerdir=/lower", "upperdir=/old-upper", "workdir=/old-work"},
			}}},
			Resolver: fakeResolver{
				ok:   true,
				meta: rootfs.Metadata{SandboxID: "sandbox-a", TeamID: "team-a", Mode: rootfs.ModeS0FSUpperdir, VolumeID: "rootfs-a", CtldPort: 8095},
			},
			PrepareClient: client,
		})
	}()
	conn := dialSnapshotterServer(t, socketPath)
	defer conn.Close()

	resp, err := snapshotsapi.NewSnapshotsClient(conn).Mounts(context.Background(), &snapshotsapi.MountsRequest{
		Key: "container-a",
	})
	if err != nil {
		t.Fatalf("Mounts() error = %v", err)
	}
	if len(resp.Mounts) != 1 {
		t.Fatalf("mount count = %d, want 1", len(resp.Mounts))
	}
	if got := optionValue(resp.Mounts[0].Options, "upperdir="); got != "/s0fs/upper" {
		t.Fatalf("upperdir = %q, want /s0fs/upper", got)
	}
	if got := optionValue(resp.Mounts[0].Options, "workdir="); got != "/s0fs/work" {
		t.Fatalf("workdir = %q, want /s0fs/work", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not stop after context cancellation")
	}
}

func TestServeNormalizesIncomingNamespaceForPrepare(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "snapshotter.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := &fakeSnapshotter{mounts: []mount.Mount{{
		Type:    "overlay",
		Source:  "overlay",
		Options: []string{"lowerdir=/lower", "upperdir=/old-upper", "workdir=/old-work"},
	}}}
	errCh := make(chan error, 1)
	go func() {
		errCh <- Serve(ctx, ServerConfig{
			SocketPath: socketPath,
			Base:       base,
		})
	}()
	conn := dialSnapshotterServer(t, socketPath)
	defer conn.Close()

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(namespaces.GRPCHeader, "custom"))
	if _, err := snapshotsapi.NewSnapshotsClient(conn).Prepare(reqCtx, &snapshotsapi.PrepareSnapshotRequest{
		Key: "extract-key",
	}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if base.namespace != "custom" {
		t.Fatalf("base namespace = %q, want custom", base.namespace)
	}
	if base.outgoingNamespace != "custom" {
		t.Fatalf("base outgoing namespace = %q, want custom", base.outgoingNamespace)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not stop after context cancellation")
	}
}

func dialSnapshotterServer(t *testing.T, socketPath string) *grpc.ClientConn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		conn, err := grpc.DialContext(ctx, "unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err == nil {
			return conn
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("dial snapshotter server: %v", lastErr)
	return nil
}
