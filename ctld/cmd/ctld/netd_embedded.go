package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	netddaemon "github.com/sandbox0-ai/sandbox0/netd/pkg/daemon"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"go.uber.org/zap"
)

const (
	embeddedNetdShutdownTimeout      = 7 * time.Second
	embeddedNetdTelemetryStopTimeout = 5 * time.Second
	embeddedNetdStartupTimeout       = 45 * time.Second
)

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

func configuredEmbeddedNetdFactory(path, ctldHTTPAddr string) (primaryServiceFactory, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	cfg, err := loadEmbeddedNetdConfig(path)
	if err != nil {
		return nil, err
	}
	_, rawPort, err := net.SplitHostPort(ctldHTTPAddr)
	if err != nil {
		return nil, fmt.Errorf("parse ctld HTTP address %q: %w", ctldHTTPAddr, err)
	}
	ctldPort, err := strconv.Atoi(rawPort)
	if err != nil {
		return nil, fmt.Errorf("parse ctld HTTP port %q: %w", rawPort, err)
	}
	if err := cfg.ValidateListenerPorts(map[int]string{ctldPort: "ctld HTTP port"}); err != nil {
		return nil, err
	}
	return func() (primaryService, error) {
		return newEmbeddedNetdService(cfg.DeepCopy())
	}, nil
}

type embeddedNetdService struct {
	daemon        *netddaemon.Daemon
	logger        *zap.Logger
	observability *observability.Provider
}

func loadEmbeddedNetdConfig(configPath string) (*apiconfig.NetdConfig, error) {
	cfg, err := apiconfig.LoadNetdConfigFromPath(configPath)
	if err != nil {
		return nil, err
	}
	cfg.NodeName = strings.TrimSpace(cfg.NodeName)
	if cfg.NodeName == "" {
		cfg.NodeName = strings.TrimSpace(nodeName)
	}
	if expected := strings.TrimSpace(nodeName); expected != "" && cfg.NodeName != expected {
		return nil, fmt.Errorf("netd node name %q does not match ctld node name %q", cfg.NodeName, expected)
	}
	return cfg, nil
}

func newEmbeddedNetdService(cfg *apiconfig.NetdConfig) (*embeddedNetdService, error) {
	if cfg == nil {
		return nil, fmt.Errorf("netd config is nil")
	}
	logger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "netd",
		Level:       cfg.LogLevel,
	})
	if err != nil {
		return nil, fmt.Errorf("create netd logger: %w", err)
	}
	provider, err := observability.New(observability.ConfigFromEnv("netd", logger))
	if err != nil {
		_ = logger.Sync()
		return nil, fmt.Errorf("create netd observability: %w", err)
	}
	logger.Info("Starting embedded netd",
		zap.String("node", cfg.NodeName),
		zap.Int("health_port", cfg.HealthPort),
		zap.Int("metrics_port", cfg.MetricsPort),
		zap.Int("proxy_http_port", cfg.ProxyHTTPPort),
		zap.Int("proxy_https_port", cfg.ProxyHTTPSPort),
	)
	return &embeddedNetdService{
		daemon:        netddaemon.New(cfg, logger, provider),
		logger:        logger,
		observability: provider,
	}, nil
}

func (s *embeddedNetdService) Run(ctx context.Context) (runErr error) {
	if s == nil || s.daemon == nil {
		return fmt.Errorf("embedded netd is not initialized")
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), embeddedNetdTelemetryStopTimeout)
		defer cancel()
		if s.observability != nil {
			runErr = errors.Join(runErr, s.observability.Shutdown(shutdownCtx))
		}
		if s.logger != nil {
			s.logger.Info("Stopped embedded netd")
			_ = s.logger.Sync()
		}
	}()
	return s.runDaemonWithStartupDeadline(ctx)
}

func (s *embeddedNetdService) runDaemonWithStartupDeadline(ctx context.Context) error {
	daemonCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.daemon.Run(daemonCtx) }()
	timer := time.NewTimer(embeddedNetdStartupTimeout)
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
			return errors.Join(fmt.Errorf("embedded netd did not become ready within %s", embeddedNetdStartupTimeout), err)
		}
	}
}

func (s *embeddedNetdService) Ready() bool {
	return s != nil && s.daemon != nil && s.daemon.Ready()
}
