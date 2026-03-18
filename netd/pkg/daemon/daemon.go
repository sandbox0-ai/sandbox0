package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/apply"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/conntrack"
	netdmetering "github.com/sandbox0-ai/sandbox0/netd/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/proxy"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/redirect"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/watcher"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Daemon struct {
	cfg           *config.NetdConfig
	logger        *zap.Logger
	healthServer  *http.Server
	metricsServer *http.Server
	proxyServer   *proxy.Server
	ready         atomic.Bool
}

func New(cfg *config.NetdConfig, logger *zap.Logger) *Daemon {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Daemon{
		cfg:    cfg,
		logger: logger,
	}
}

func (d *Daemon) Run(ctx context.Context) error {
	if d.cfg == nil {
		return fmt.Errorf("netd config is nil")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := d.startServers(); err != nil {
		return err
	}
	proxyExitCh := make(chan error, 1)
	if err := d.runNetd(runCtx, cancel, proxyExitCh); err != nil {
		return err
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-proxyExitCh:
		cancel()
	}

	<-runCtx.Done()

	shutdownCtx := context.Background()
	if d.cfg.ShutdownDelay.Duration > 0 {
		var cancel context.CancelFunc
		shutdownCtx, cancel = context.WithTimeout(context.Background(), d.cfg.ShutdownDelay.Duration)
		defer cancel()
	}
	shutdownErr := d.shutdown(shutdownCtx)
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		if shutdownErr != nil {
			d.logger.Error("Shutdown completed with error", zap.Error(shutdownErr))
		}
		return runErr
	}
	return shutdownErr
}

func (d *Daemon) runNetd(ctx context.Context, cancel context.CancelFunc, proxyExitCh chan<- error) error {
	if d.cfg.NodeName == "" {
		return fmt.Errorf("node name is required")
	}
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return fmt.Errorf("create k8s client: %w", err)
	}

	netdWatcher := watcher.NewWatcher(client, d.cfg.ResyncPeriod.Duration, d.logger)
	policyStore := policy.NewStore(d.logger)
	platformState := newPlatformPolicyState(d.cfg, policyStore, d.logger)
	conntrackManager, err := conntrack.NewManager(d.logger)
	if err != nil {
		d.logger.Warn("Conntrack manager disabled", zap.Error(err))
	}
	defer conntrackManager.Close()
	tracker := conntrack.NewTracker()
	var usageAggregator *netdmetering.Aggregator
	var meteringPool *pgxpool.Pool
	if d.cfg.DatabaseURL != "" {
		pool, err := dbpool.New(ctx, dbpool.Options{
			DatabaseURL:     d.cfg.DatabaseURL,
			DefaultMaxConns: 5,
			DefaultMinConns: 1,
		})
		if err != nil {
			return fmt.Errorf("create netd database pool: %w", err)
		}
		meteringPool = pool
		defer meteringPool.Close()
		if err := meteringpkg.RunMigrations(ctx, meteringPool, &zapLoggerAdapter{logger: d.logger}); err != nil {
			return fmt.Errorf("run metering migrations: %w", err)
		}
		usageAggregator = netdmetering.NewAggregator(
			netdmetering.NewRecorder(meteringpkg.NewRepository(meteringPool)),
			d.cfg.RegionID,
			d.cfg.ClusterID,
			d.cfg.NodeName,
			d.logger,
		)
	}
	syncTrigger := make(chan struct{}, 1)
	triggerSync := func() {
		select {
		case syncTrigger <- struct{}{}:
		default:
		}
	}

	netdWatcher.SetSandboxHandlers(func(info *watcher.SandboxInfo) {
		if info == nil {
			return
		}
		platformState.OnSandboxUpsert(info)
		changed, prevHash := policyStore.UpsertFromSandbox(info)
		if changed && info.PodIP != "" {
			flows := tracker.PopBySrc(info.PodIP)
			p := policyStore.GetByIP(info.PodIP)
			var flowsToKill []conntrack.FlowKey
			// Only kill flows that are denied by the new policy.
			// This prevents a race condition where a new connection established
			// immediately after the policy update but before this handler runs
			// would be killed if we blindly cleared all flows.
			for _, flow := range flows {
				proto := "tcp"
				if flow.Proto == 17 {
					proto = "udp"
				}
				if !policy.AllowEgressL4(p, net.IP(flow.DstIP.AsSlice()), int(flow.DstPort), proto) {
					flowsToKill = append(flowsToKill, flow)
				}
			}
			if len(flowsToKill) > 0 {
				conntrackManager.CleanupFlows(ctx, flowsToKill)
			}
		}
		d.logger.Info("Sandbox policy handler triggered",
			zap.String("sandbox", info.Namespace+"/"+info.Name),
			zap.String("pod_ip", info.PodIP),
			zap.Bool("policy_changed", changed),
			zap.String("policy_hash", info.NetworkPolicyHash),
			zap.String("prev_hash", prevHash),
		)
		triggerSync()
	}, func(info *watcher.SandboxInfo) {
		if info != nil {
			platformState.OnSandboxDelete(info)
			policyStore.DeleteByKey(info.Namespace, info.Name)
			flows := tracker.PopBySrc(info.PodIP)
			conntrackManager.CleanupFlows(ctx, flows)
			d.logger.Info("Sandbox delete handler triggered",
				zap.String("sandbox", info.Namespace+"/"+info.Name),
				zap.String("pod_ip", info.PodIP),
			)
		}
		triggerSync()
	})
	netdWatcher.SetServiceHandlers(platformState.OnServiceUpsert, platformState.OnServiceDelete)
	netdWatcher.SetEndpointsHandlers(platformState.OnEndpointsUpsert, platformState.OnEndpointsDelete)
	if err := netdWatcher.Start(ctx); err != nil {
		return err
	}

	proxyOpts := []proxy.ServerOption{}
	if d.cfg.EgressBrokerURL != "" {
		privateKey, keyErr := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
		if keyErr != nil {
			return fmt.Errorf("load netd internal auth private key: %w", keyErr)
		}
		tokenGenerator := internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     "netd",
			PrivateKey: privateKey,
			TTL:        30 * time.Second,
		})
		proxyOpts = append(proxyOpts, proxy.WithEgressAuthResolver(proxy.NewHTTPEgressAuthResolver(
			d.cfg.EgressBrokerURL,
			d.cfg.EgressBrokerTimeout.Duration,
			netdEgressAuthTokenProvider{generator: tokenGenerator},
		)))
	}
	proxyServer, err := proxy.NewServer(d.cfg, policyStore, tracker, usageAggregator, d.logger, proxyOpts...)
	if err != nil {
		return err
	}
	d.proxyServer = proxyServer
	proxyServer.Start(ctx)
	if proxyExitCh != nil {
		go func() {
			err := <-proxyServer.Done()
			select {
			case proxyExitCh <- err:
			default:
			}
			if cancel != nil {
				cancel()
			}
		}()
	}

	redirectManager := redirect.NewManager(redirect.Config{
		PreferNFT:      d.cfg.PreferNFT != nil && *d.cfg.PreferNFT,
		ProxyHTTPPort:  d.cfg.ProxyHTTPPort,
		ProxyHTTPSPort: d.cfg.ProxyHTTPSPort,
	}, d.logger)
	patcher := apply.NewPatcher(client, d.logger)

	syncOnce := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(d.cfg.ResyncPeriod.Duration)
		defer ticker.Stop()
		triggerSync()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-syncTrigger:
			}
			if err := d.syncRedirect(ctx, netdWatcher, policyStore, redirectManager, patcher); err != nil {
				d.logger.Error("Failed to sync redirect rules", zap.Error(err))
				if d.cfg.FailClosed {
					d.ready.Store(false)
				}
			} else {
				d.ready.Store(true)
			}
			select {
			case syncOnce <- struct{}{}:
			default:
			}
		}
	}()

	if usageAggregator != nil {
		go d.runMeteringFlushLoop(ctx, usageAggregator)
	}

	select {
	case <-syncOnce:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type netdEgressAuthTokenProvider struct {
	generator *internalauth.Generator
}

func (p netdEgressAuthTokenProvider) Token(context.Context) (string, error) {
	if p.generator == nil {
		return "", fmt.Errorf("internal auth generator is not configured")
	}
	return p.generator.GenerateSystem("manager", internalauth.GenerateOptions{})
}

func (d *Daemon) runMeteringFlushLoop(ctx context.Context, aggregator *netdmetering.Aggregator) {
	interval := d.cfg.MeteringReportInterval.Duration
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if err := aggregator.Flush(context.Background()); err != nil {
				d.logger.Error("Failed to flush netd metering windows during shutdown", zap.Error(err))
			}
			return
		case <-ticker.C:
			if err := aggregator.Flush(ctx); err != nil {
				d.logger.Error("Failed to flush netd metering windows", zap.Error(err))
			}
		}
	}
}

type zapLoggerAdapter struct {
	logger *zap.Logger
}

func (l *zapLoggerAdapter) Printf(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Sugar().Infof(format, args...)
}

func (l *zapLoggerAdapter) Fatalf(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Sugar().Errorf(format, args...)
}

func (d *Daemon) syncRedirect(
	ctx context.Context,
	netdWatcher *watcher.Watcher,
	policyStore *policy.Store,
	redirectManager redirect.Manager,
	patcher *apply.Patcher,
) error {
	if netdWatcher == nil || redirectManager == nil || patcher == nil {
		return fmt.Errorf("missing watcher or redirect manager or patcher or policy store")
	}
	sandboxes := netdWatcher.ListSandboxesByNode(d.cfg.NodeName)
	sandboxIPs := make([]string, 0, len(sandboxes))
	for _, sandbox := range sandboxes {
		if sandbox.PodIP == "" {
			continue
		}
		sandboxIPs = append(sandboxIPs, sandbox.PodIP)
	}

	bypassCIDRs := []string{}
	if d.cfg.ClusterDNSCIDR != "" {
		bypassCIDRs = append(bypassCIDRs, d.cfg.ClusterDNSCIDR)
	}
	if len(d.cfg.PlatformAllowedCIDRs) > 0 {
		bypassCIDRs = append(bypassCIDRs, d.cfg.PlatformAllowedCIDRs...)
	}
	if policyStore != nil {
		bypassCIDRs = append(bypassCIDRs, policyStore.AllowedPlatformCIDRs()...)
	}

	d.logger.Info(
		"Syncing redirect rules",
		zap.Int("sandboxes_total", len(sandboxes)),
		zap.Int("sandbox_ips", len(sandboxIPs)),
		zap.Strings("sandbox_ips", sandboxIPs),
		zap.Strings("bypass_cidrs", bypassCIDRs),
	)
	if err := redirectManager.Sync(ctx, sandboxIPs, bypassCIDRs); err != nil {
		return err
	}
	patchedCount := 0
	if err := patcher.SyncAppliedHashes(ctx, sandboxes); err != nil {
		d.logger.Warn("Failed to sync applied hashes", zap.Error(err))
	} else {
		for _, sandbox := range sandboxes {
			if sandbox.NetworkPolicyHash != "" && sandbox.NetworkPolicyHash == sandbox.NetworkAppliedHash {
				patchedCount++
			}
		}
	}
	d.logger.Info("Redirect rules synced",
		zap.Int("sandboxes_patched", patchedCount),
		zap.Int("sandboxes_total", len(sandboxes)),
	)
	return nil
}

func (d *Daemon) startServers() error {
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", d.handleHealth)
	healthMux.HandleFunc("/readyz", d.handleReady)

	d.healthServer = &http.Server{
		Addr:              net.JoinHostPort("", fmt.Sprintf("%d", d.cfg.HealthPort)),
		Handler:           healthMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	d.metricsServer = &http.Server{
		Addr:              net.JoinHostPort("", fmt.Sprintf("%d", d.cfg.MetricsPort)),
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	if err := d.listenAndServe(d.healthServer, "health"); err != nil {
		return err
	}
	if err := d.listenAndServe(d.metricsServer, "metrics"); err != nil {
		return err
	}

	return nil
}

func (d *Daemon) listenAndServe(server *http.Server, name string) error {
	if server == nil {
		return fmt.Errorf("server %s is nil", name)
	}
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", name, err)
	}
	d.logger.Info("HTTP server listening",
		zap.String("name", name),
		zap.String("addr", server.Addr),
	)

	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			d.logger.Error("HTTP server error",
				zap.String("name", name),
				zap.Error(err),
			)
		}
	}()
	return nil
}

func (d *Daemon) shutdown(ctx context.Context) error {
	var shutdownErr error
	if d.proxyServer != nil {
		if err := d.proxyServer.Shutdown(ctx); err != nil {
			shutdownErr = err
			d.logger.Error("Failed to shutdown proxy server", zap.Error(err))
		}
	}
	if d.healthServer != nil {
		if err := d.healthServer.Shutdown(ctx); err != nil {
			shutdownErr = err
			d.logger.Error("Failed to shutdown health server", zap.Error(err))
		}
	}
	if d.metricsServer != nil {
		if err := d.metricsServer.Shutdown(ctx); err != nil {
			shutdownErr = err
			d.logger.Error("Failed to shutdown metrics server", zap.Error(err))
		}
	}
	return shutdownErr
}

func (d *Daemon) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (d *Daemon) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !d.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}
