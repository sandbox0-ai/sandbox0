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
	ctldrootfs "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"google.golang.org/grpc"
)

type ServiceConfig struct {
	Root          string
	SocketPath    string
	Namespace     string
	Store         objectstore.Store
	ObjectCache   *ctldrootfs.ObjectCache
	MetadataBytes int64
}

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
	if root == "" || root == "." || !filepath.IsAbs(root) {
		return nil, fmt.Errorf("rootfs snapshotter absolute root is required")
	}
	socketPath := filepath.Clean(strings.TrimSpace(cfg.SocketPath))
	if socketPath == "" || socketPath == "." || !filepath.IsAbs(socketPath) {
		return nil, fmt.Errorf("rootfs snapshotter absolute socket path is required")
	}
	delegate, err := overlay.NewSnapshotter(
		filepath.Join(root, "overlay"),
		overlay.WithUpperdirLabel,
		overlay.AsynchronousRemove,
	)
	if err != nil {
		return nil, fmt.Errorf("create rootfs overlay delegate: %w", err)
	}
	snapshotter, err := New(Config{
		Delegate:           delegate,
		Store:              cfg.Store,
		ObjectCache:        cfg.ObjectCache,
		MetadataCacheBytes: cfg.MetadataBytes,
	})
	if err != nil {
		_ = delegate.Close()
		return nil, err
	}
	namespace := strings.TrimSpace(cfg.Namespace)
	if namespace == "" {
		namespace = "k8s.io"
	}
	return &Service{snapshotter: snapshotter, socketPath: socketPath, namespace: namespace}, nil
}

func (s *Service) Run(ctx context.Context) error {
	if s == nil || s.snapshotter == nil {
		return fmt.Errorf("rootfs snapshotter service is not configured")
	}
	if err := s.snapshotter.Recover(namespaces.WithNamespace(ctx, s.namespace)); err != nil {
		return err
	}
	if err := prepareSocket(s.socketPath); err != nil {
		return err
	}
	listener, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("listen on rootfs snapshotter socket: %w", err)
	}
	defer listener.Close()
	defer os.Remove(s.socketPath)
	if err := os.Chmod(s.socketPath, 0o660); err != nil {
		return fmt.Errorf("set rootfs snapshotter socket permissions: %w", err)
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

func (s *Service) Ready() error {
	if s == nil || !s.ready.Load() {
		return fmt.Errorf("rootfs snapshotter service is not serving")
	}
	return s.snapshotter.HealthError()
}

func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	var result error
	s.closeOnce.Do(func() {
		s.ready.Store(false)
		if s.server != nil {
			s.server.Stop()
		}
		result = s.snapshotter.Close()
	})
	return result
}

func prepareSocket(value string) error {
	if err := os.MkdirAll(filepath.Dir(value), 0o755); err != nil {
		return err
	}
	info, err := os.Lstat(value)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to replace non-socket path %s", value)
	}
	return os.Remove(value)
}
