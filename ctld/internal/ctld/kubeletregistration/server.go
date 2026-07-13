package kubeletregistration

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
)

var supportedVersions = []string{"1.0.0"}

type Config struct {
	SocketPath string
	DriverName string
	Endpoint   string
}

// Server exposes the kubelet plugin-registration API for the active ctld.
type Server struct {
	registerapi.UnimplementedRegistrationServer

	config Config

	mu       sync.Mutex
	grpc     *grpc.Server
	listener net.Listener
	errors   chan error
	started  bool
	stopped  bool

	registered atomic.Bool
	errorOnce  sync.Once
}

func NewServer(config Config) (*Server, error) {
	config.SocketPath = strings.TrimSpace(config.SocketPath)
	config.DriverName = strings.TrimSpace(config.DriverName)
	config.Endpoint = strings.TrimSpace(config.Endpoint)
	if config.SocketPath == "" {
		return nil, fmt.Errorf("kubelet registration socket is required")
	}
	if config.DriverName == "" {
		return nil, fmt.Errorf("kubelet registration driver name is required")
	}
	if config.Endpoint == "" {
		return nil, fmt.Errorf("kubelet registration endpoint is required")
	}
	return &Server{config: config, errors: make(chan error, 1)}, nil
}

// Start creates the node-global registration socket before serving in the
// background. Only the ctld process holding the HA primary lease may call it.
func (s *Server) Start() error {
	if s == nil {
		return fmt.Errorf("kubelet registration server is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return fmt.Errorf("kubelet registration server is stopped")
	}
	if s.started {
		return fmt.Errorf("kubelet registration server already started")
	}
	if err := os.MkdirAll(filepath.Dir(s.config.SocketPath), 0o755); err != nil {
		return fmt.Errorf("create kubelet registration directory: %w", err)
	}
	if err := os.Remove(s.config.SocketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale kubelet registration socket: %w", err)
	}
	listener, err := net.Listen("unix", s.config.SocketPath)
	if err != nil {
		return fmt.Errorf("listen on kubelet registration socket: %w", err)
	}
	if err := os.Chmod(s.config.SocketPath, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(s.config.SocketPath)
		return fmt.Errorf("set kubelet registration socket permissions: %w", err)
	}
	server := grpc.NewServer()
	registerapi.RegisterRegistrationServer(server, s)
	s.grpc = server
	s.listener = listener
	s.started = true
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.reportError(fmt.Errorf("serve kubelet registration socket: %w", err))
		}
	}()
	return nil
}

func (s *Server) Errors() <-chan error {
	if s == nil {
		return nil
	}
	return s.errors
}

func (s *Server) Registered() bool {
	return s != nil && s.registered.Load()
}

func (s *Server) Stop() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	if !s.started {
		s.mu.Unlock()
		return
	}
	server := s.grpc
	listener := s.listener
	s.mu.Unlock()

	s.registered.Store(false)
	if server != nil {
		server.Stop()
	}
	if listener != nil {
		_ = listener.Close()
	}
	_ = os.Remove(s.config.SocketPath)
}

func (s *Server) GetInfo(context.Context, *registerapi.InfoRequest) (*registerapi.PluginInfo, error) {
	return &registerapi.PluginInfo{
		Type:              registerapi.CSIPlugin,
		Name:              s.config.DriverName,
		Endpoint:          s.config.Endpoint,
		SupportedVersions: append([]string(nil), supportedVersions...),
	}, nil
}

func (s *Server) NotifyRegistrationStatus(_ context.Context, status *registerapi.RegistrationStatus) (*registerapi.RegistrationStatusResponse, error) {
	if status != nil && status.GetPluginRegistered() {
		s.registered.Store(true)
		log.Printf("kubelet accepted CSI registration for %q at %q", s.config.DriverName, s.config.Endpoint)
		return &registerapi.RegistrationStatusResponse{}, nil
	}
	s.registered.Store(false)
	message := "kubelet rejected CSI plugin registration"
	if status != nil && strings.TrimSpace(status.GetError()) != "" {
		message += ": " + strings.TrimSpace(status.GetError())
	}
	s.reportError(errors.New(message))
	return &registerapi.RegistrationStatusResponse{}, nil
}

func (s *Server) reportError(err error) {
	if s == nil || err == nil {
		return
	}
	s.errorOnce.Do(func() {
		s.errors <- err
	})
}
