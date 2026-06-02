package snapshotter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	snapshotservice "github.com/containerd/containerd/v2/contrib/snapshotservice"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
	"google.golang.org/grpc"
)

type ServerConfig struct {
	SocketPath    string
	Base          snapshots.Snapshotter
	Resolver      MetadataResolver
	PrepareClient rootfs.PrepareClient
	CtldAddress   string
}

func Serve(ctx context.Context, cfg ServerConfig) error {
	socketPath := strings.TrimSpace(cfg.SocketPath)
	if socketPath == "" {
		return fmt.Errorf("snapshotter socket path is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	wrapped, err := New(cfg.Base, cfg.Resolver, cfg.PrepareClient, WithCtldAddress(cfg.CtldAddress))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return fmt.Errorf("create snapshotter socket dir: %w", err)
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale snapshotter socket: %w", err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen snapshotter socket: %w", err)
	}
	defer listener.Close()
	defer os.Remove(socketPath)

	server := grpc.NewServer()
	snapshotsapi.RegisterSnapshotsServer(server, snapshotservice.FromSnapshotter(wrapped))
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	if err := server.Serve(listener); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	return nil
}
