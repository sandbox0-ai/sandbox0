package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	ctldnetworking "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"go.uber.org/zap"
)

const (
	networkRuntimeShutdownTimeout      = 7 * time.Second
	networkRuntimeTelemetryStopTimeout = 5 * time.Second
	networkRuntimeStartupTimeout       = 45 * time.Second
)

// primaryService is a ctld subsystem that runs only while this process owns
// the node-local HA primary lease.
type primaryService interface {
	Run(context.Context) error
	Ready() bool
}

type primaryServiceFactory func() (primaryService, error)

type primaryServiceHandle struct {
	service primaryService
	errors  chan error
	done    chan struct{}

	mu  sync.RWMutex
	err error
}

func startPrimaryService(ctx context.Context, service primaryService) *primaryServiceHandle {
	handle := &primaryServiceHandle{
		service: service,
		errors:  make(chan error, 1),
		done:    make(chan struct{}),
	}
	go func() {
		err := service.Run(ctx)
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			err = nil
		}
		if err == nil && ctx.Err() == nil {
			err = fmt.Errorf("service stopped unexpectedly")
		}
		handle.mu.Lock()
		handle.err = err
		handle.mu.Unlock()
		handle.errors <- err
		close(handle.done)
	}()
	return handle
}

func (h *primaryServiceHandle) Ready() bool {
	return h != nil && h.service != nil && h.service.Ready()
}

func (h *primaryServiceHandle) Errors() <-chan error {
	if h == nil {
		return nil
	}
	return h.errors
}

func (h *primaryServiceHandle) Wait(ctx context.Context) error {
	if h == nil {
		return nil
	}
	select {
	case <-h.done:
		h.mu.RLock()
		defer h.mu.RUnlock()
		return h.err
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.done:
		h.mu.RLock()
		defer h.mu.RUnlock()
		return h.err
	}
}

func configuredNetworkRuntimeFactory(
	path, ctldHTTPAddr, runtimeWatchHTTPAddr string,
	runtimeSlotsOnly bool,
) (primaryServiceFactory, error) {
	ctldPort, err := listenerPort(ctldHTTPAddr, "ctld HTTP")
	if err != nil {
		return nil, err
	}
	runtimeWatchPort := 0
	if !runtimeSlotsOnly {
		runtimeWatchPort, err = listenerPort(runtimeWatchHTTPAddr, "ctld runtime watch")
		if err != nil {
			return nil, err
		}
		if ctldPort == runtimeWatchPort {
			return nil, fmt.Errorf("ctld HTTP port and runtime watch port must differ")
		}
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	if err := validateRuntimeSlotNetworkPaths(stateRoot, runtimeSlotNetworkSocket, runtimeSlotNetNSRoot); err != nil {
		return nil, err
	}
	cfg, err := loadNetworkRuntimeConfig(path)
	if err != nil {
		return nil, err
	}
	reservedPorts := map[int]string{ctldPort: "ctld HTTP port"}
	if runtimeWatchPort != 0 {
		reservedPorts[runtimeWatchPort] = "ctld runtime watch port"
	}
	if err := cfg.ValidateListenerPorts(reservedPorts); err != nil {
		return nil, err
	}
	return func() (primaryService, error) {
		return newNetworkRuntimeService(cfg.DeepCopy(), runtimeWatchPort, runtimeSlotsOnly)
	}, nil
}

func validateRuntimeSlotNetworkPaths(state, socket, netnsRoot string) error {
	for name, value := range map[string]string{
		"ctld state root": state, "runtime slot network socket": socket,
		"runtime slot netns root": netnsRoot,
	} {
		trimmed := strings.TrimSpace(value)
		if !filepath.IsAbs(trimmed) || filepath.Clean(trimmed) != trimmed || trimmed == string(filepath.Separator) {
			return fmt.Errorf("%s must be a canonical non-root absolute path", name)
		}
	}
	return nil
}

func listenerPort(address, label string) (int, error) {
	_, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		return 0, fmt.Errorf("parse %s address %q: %w", label, address, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("parse %s port %q", label, rawPort)
	}
	return port, nil
}

type networkRuntimeService struct {
	daemon        *ctldnetworking.Daemon
	logger        *zap.Logger
	observability *observability.Provider
}

func loadNetworkRuntimeConfig(configPath string) (*apiconfig.NetworkRuntimeConfig, error) {
	cfg, err := apiconfig.LoadNetworkRuntimeConfigFromPath(configPath)
	if err != nil {
		return nil, err
	}
	cfg.NodeName = strings.TrimSpace(cfg.NodeName)
	if cfg.NodeName == "" {
		cfg.NodeName = strings.TrimSpace(nodeName)
	}
	if expected := strings.TrimSpace(nodeName); expected != "" && cfg.NodeName != expected {
		return nil, fmt.Errorf("network runtime node name %q does not match ctld node name %q", cfg.NodeName, expected)
	}
	return cfg, nil
}

func newNetworkRuntimeService(
	cfg *apiconfig.NetworkRuntimeConfig,
	runtimeWatchPort int,
	runtimeSlotsOnly bool,
) (*networkRuntimeService, error) {
	if cfg == nil {
		return nil, fmt.Errorf("network runtime config is nil")
	}
	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "ctld",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		return nil, fmt.Errorf("create network runtime logger: %w", err)
	}
	provider, err := observability.New(observability.ConfigFromEnv("ctld", logger))
	if err != nil {
		_ = logger.Sync()
		return nil, fmt.Errorf("create network runtime observability: %w", err)
	}
	logger.Info("Starting ctld network runtime",
		zap.String("node", cfg.NodeName),
		zap.Int("health_port", cfg.HealthPort),
		zap.Int("metrics_port", cfg.MetricsPort),
		zap.Int("proxy_http_port", cfg.ProxyHTTPPort),
		zap.Int("proxy_https_port", cfg.ProxyHTTPSPort),
	)
	runtimeWatchPorts := []int(nil)
	if runtimeWatchPort != 0 {
		runtimeWatchPorts = []int{runtimeWatchPort}
	}
	return &networkRuntimeService{
		daemon: ctldnetworking.New(cfg, logger, provider, ctldnetworking.Options{
			RuntimeWatchTCPPorts:     runtimeWatchPorts,
			RuntimeSlotStatePath:     filepath.Join(stateRoot, "runtime-slot-network.db"),
			RuntimeSlotControlSocket: runtimeSlotNetworkSocket,
			RuntimeSlotNetNSRoot:     runtimeSlotNetNSRoot,
			RuntimeSlotsOnly:         runtimeSlotsOnly,
		}),
		logger:        logger,
		observability: provider,
	}, nil
}

func (s *networkRuntimeService) Run(ctx context.Context) (runErr error) {
	if s == nil || s.daemon == nil {
		return fmt.Errorf("ctld network runtime is not initialized")
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), networkRuntimeTelemetryStopTimeout)
		defer cancel()
		if s.observability != nil {
			runErr = errors.Join(runErr, s.observability.Shutdown(shutdownCtx))
		}
		if s.logger != nil {
			s.logger.Info("Stopped ctld network runtime")
			_ = s.logger.Sync()
		}
	}()
	return s.runDaemonWithStartupDeadline(ctx)
}

func (s *networkRuntimeService) runDaemonWithStartupDeadline(ctx context.Context) error {
	daemonCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.daemon.Run(daemonCtx) }()
	timer := time.NewTimer(networkRuntimeStartupTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			cancel()
			return <-done
		case <-ticker.C:
			if s.daemon.Ready() {
				return <-done
			}
		case <-timer.C:
			cancel()
			err := <-done
			return errors.Join(fmt.Errorf("ctld network runtime did not become ready within %s", networkRuntimeStartupTimeout), err)
		}
	}
}

func (s *networkRuntimeService) Ready() bool {
	return s != nil && s.daemon != nil && s.daemon.Ready()
}
