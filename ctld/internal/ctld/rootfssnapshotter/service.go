package rootfssnapshotter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/v2/contrib/snapshotservice"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"google.golang.org/grpc"
)

type ServiceConfig struct {
	Root       string
	SocketPath string
	Namespace  string
	Store      objectstore.Store
	Observer   *Observer
}

// Service exposes Snapshotter through containerd's external proxy plugin API.
type Service struct {
	snapshotter *Snapshotter
	socketPath  string
	namespace   string
	server      *grpc.Server
	ready       atomic.Bool
	closeOnce   sync.Once
}

func NewService(cfg ServiceConfig) (*Service, error) {
	root := filepath.Clean(strings.TrimSpace(cfg.Root))
	if root == "" || root == "." {
		return nil, fmt.Errorf("rootfs snapshotter root is required")
	}
	socketPath := filepath.Clean(strings.TrimSpace(cfg.SocketPath))
	if socketPath == "" || socketPath == "." {
		return nil, fmt.Errorf("rootfs snapshotter socket path is required")
	}
	delegate, err := overlay.NewSnapshotter(
		filepath.Join(root, "overlay"),
		overlay.WithUpperdirLabel,
		overlay.AsynchronousRemove,
	)
	if err != nil {
		return nil, fmt.Errorf("create private overlay snapshotter: %w", err)
	}
	namespace := strings.TrimSpace(cfg.Namespace)
	if namespace == "" {
		namespace = "k8s.io"
	}
	snapshotter, err := NewSnapshotter(delegate, cfg.Store, WithObserver(cfg.Observer))
	if err != nil {
		_ = delegate.Close()
		return nil, err
	}
	return &Service{snapshotter: snapshotter, socketPath: socketPath, namespace: namespace}, nil
}

func (s *Service) Run(ctx context.Context) error {
	if s == nil || s.snapshotter == nil {
		return fmt.Errorf("rootfs snapshotter service is not configured")
	}
	recoveryContext := namespaces.WithNamespace(ctx, s.namespace)
	if err := s.snapshotter.Recover(recoveryContext); err != nil {
		return err
	}
	if err := prepareSocketPath(s.socketPath); err != nil {
		return err
	}
	listener, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("listen on rootfs snapshotter socket: %w", err)
	}
	defer listener.Close()
	defer os.Remove(s.socketPath)
	if err := os.Chmod(s.socketPath, 0o660); err != nil {
		return fmt.Errorf("chmod rootfs snapshotter socket: %w", err)
	}
	server := grpc.NewServer()
	snapshotsapi.RegisterSnapshotsServer(server, snapshotservice.FromSnapshotter(s.snapshotter))
	s.server = server
	s.ready.Store(true)
	defer s.ready.Store(false)
	go func() {
		<-ctx.Done()
		server.Stop()
	}()
	err = server.Serve(listener)
	if ctx.Err() != nil || errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

func (s *Service) Ready() bool {
	return s != nil && s.ready.Load() && s.snapshotter.HealthError() == nil
}

func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	s.closeOnce.Do(func() {
		s.ready.Store(false)
		if s.server != nil {
			s.server.Stop()
		}
		closeErr = s.snapshotter.Close()
	})
	return closeErr
}

func prepareSocketPath(socketPath string) error {
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return fmt.Errorf("create rootfs snapshotter socket directory: %w", err)
	}
	info, err := os.Lstat(socketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect rootfs snapshotter socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to replace non-socket path %s", socketPath)
	}
	if err := os.Remove(socketPath); err != nil {
		return fmt.Errorf("remove stale rootfs snapshotter socket: %w", err)
	}
	return nil
}
