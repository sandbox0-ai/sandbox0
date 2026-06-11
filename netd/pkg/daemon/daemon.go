package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
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
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Daemon struct {
	cfg             *config.NetdConfig
	logger          *zap.Logger
	healthServer    *http.Server
	metricsServer   *http.Server
	proxyServer     *proxy.Server
	obsProvider     *observability.Provider
	runtimeMu       sync.Mutex
	conntrackCloser runtimeResource
	meteringCloser  runtimeResource
	meteringDone    <-chan struct{}
	ready           atomic.Bool
}

type runtimeResource interface {
	Close()
}

func New(cfg *config.NetdConfig, logger *zap.Logger, obsProvider *observability.Provider) *Daemon {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Daemon{
		cfg:         cfg,
		logger:      logger,
		obsProvider: obsProvider,
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
	if d.obsProvider != nil {
		d.obsProvider.K8s.WrapConfig(k8sConfig)
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
	tracker := conntrack.NewTracker()
	var usageAggregator *netdmetering.Aggregator
	var meteringPool *pgxpool.Pool
	runtimeResourcesRegistered := false
	defer func() {
		if runtimeResourcesRegistered {
			return
		}
		if meteringPool != nil {
			meteringPool.Close()
		}
		if conntrackManager != nil {
			conntrackManager.Close()
		}
	}()
	if d.cfg.DatabaseURL != "" {
		pool, err := dbpool.New(ctx, dbpool.Options{
			DatabaseURL:     d.cfg.DatabaseURL,
			DefaultMaxConns: 5,
			DefaultMinConns: 1,
			ConfigModifier:  d.dbConfigModifier(),
		})
		if err != nil {
			return fmt.Errorf("create netd database pool: %w", err)
		}
		meteringPool = pool
		if err := meteringpkg.RunMigrations(ctx, meteringPool, observability.NewMigrateLogger(d.logger)); err != nil {
			return fmt.Errorf("run metering migrations: %w", err)
		}
		if err := quota.RunMigrations(ctx, meteringPool, observability.NewMigrateLogger(d.logger)); err != nil {
			return fmt.Errorf("run quota migrations: %w", err)
		}
		usageAggregator = netdmetering.NewAggregator(
			netdmetering.NewRecorder(meteringpkg.NewRepository(meteringPool)),
			d.cfg.RegionID,
			d.cfg.ClusterID,
			d.cfg.NodeName,
			d.logger,
		)
		usageAggregator.SetQuotaStore(quota.NewRepository(meteringPool))
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
		d.logger.Info("Sandbox policy change observed",
			zap.String("sandbox", info.Namespace+"/"+info.Name),
			zap.String("pod_ip", info.PodIP),
			zap.String("policy_hash", info.NetworkPolicyHash),
		)
		triggerSync()
	}, func(info *watcher.SandboxInfo) {
		if info != nil {
			d.logger.Info("Sandbox policy delete observed",
				zap.String("sandbox", info.Namespace+"/"+info.Name),
				zap.String("pod_ip", info.PodIP),
			)
		}
		triggerSync()
	})
	netdWatcher.SetServiceHandlers(func(*watcher.ServiceInfo) {
		triggerSync()
	}, func(*watcher.ServiceInfo) {
		triggerSync()
	})
	netdWatcher.SetEndpointsHandlers(func(*watcher.EndpointsInfo) {
		triggerSync()
	}, func(*watcher.EndpointsInfo) {
		triggerSync()
	})
	if err := netdWatcher.Start(ctx); err != nil {
		return err
	}

	proxyOpts := []proxy.ServerOption{}
	if d.cfg.EgressAuthResolverURL != "" {
		privateKey, keyErr := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
		if keyErr != nil {
			return fmt.Errorf("load netd internal auth private key: %w", keyErr)
		}
		tokenGenerator := internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     "netd",
			PrivateKey: privateKey,
			TTL:        30 * time.Second,
		})
		proxyOpts = append(proxyOpts, proxy.WithEgressAuthResolver(proxy.NewHTTPEgressAuthResolverWithHTTPClient(
			d.cfg.EgressAuthResolverURL,
			d.cfg.EgressAuthResolverTimeout.Duration,
			netdEgressAuthTokenProvider{generator: tokenGenerator},
			d.egressAuthHTTPClient(),
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
			if err := d.syncRedirect(ctx, netdWatcher, policyStore, platformState, redirectManager, patcher, tracker, conntrackManager, proxyServer); err != nil {
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

	select {
	case <-syncOnce:
		var conntrackCloser runtimeResource
		if conntrackManager != nil {
			conntrackCloser = conntrackManager
		}
		var meteringCloser runtimeResource
		if meteringPool != nil {
			meteringCloser = meteringPool
		}
		d.registerRuntimeResources(conntrackCloser, meteringCloser)
		if usageAggregator != nil {
			d.startMeteringFlushLoop(ctx, usageAggregator)
		}
		runtimeResourcesRegistered = true
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Daemon) dbConfigModifier() func(*pgxpool.Config) error {
	if d == nil || d.obsProvider == nil {
		return nil
	}
	return d.obsProvider.Pgx.ConfigModifier()
}

func (d *Daemon) egressAuthHTTPClient() *http.Client {
	if d == nil || d.obsProvider == nil {
		timeout := 2 * time.Second
		if d != nil && d.cfg != nil && d.cfg.EgressAuthResolverTimeout.Duration > 0 {
			timeout = d.cfg.EgressAuthResolverTimeout.Duration
		}
		return &http.Client{Timeout: timeout}
	}
	timeout := d.cfg.EgressAuthResolverTimeout.Duration
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return d.obsProvider.HTTP.NewClient(httpobs.Config{Timeout: timeout})
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

func (d *Daemon) startMeteringFlushLoop(ctx context.Context, aggregator *netdmetering.Aggregator) {
	done := make(chan struct{})
	d.runtimeMu.Lock()
	d.meteringDone = done
	d.runtimeMu.Unlock()

	go func() {
		defer close(done)
		d.runMeteringFlushLoop(ctx, aggregator)
	}()
}

func (d *Daemon) registerRuntimeResources(conntrackCloser runtimeResource, meteringCloser runtimeResource) {
	d.runtimeMu.Lock()
	defer d.runtimeMu.Unlock()
	d.conntrackCloser = conntrackCloser
	d.meteringCloser = meteringCloser
}

func (d *Daemon) waitForMeteringFlushLoop(ctx context.Context) {
	d.runtimeMu.Lock()
	done := d.meteringDone
	d.runtimeMu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
		d.logger.Warn("Timed out waiting for netd metering flush loop to stop", zap.Error(ctx.Err()))
	}
}

func (d *Daemon) closeRuntimeResources() {
	d.runtimeMu.Lock()
	meteringCloser := d.meteringCloser
	conntrackCloser := d.conntrackCloser
	d.meteringCloser = nil
	d.conntrackCloser = nil
	d.meteringDone = nil
	d.runtimeMu.Unlock()

	closeRuntimeResource(meteringCloser)
	closeRuntimeResource(conntrackCloser)
}

func closeRuntimeResource(resource runtimeResource) {
	if resource != nil {
		resource.Close()
	}
}

func (d *Daemon) syncRedirect(
	ctx context.Context,
	netdWatcher *watcher.Watcher,
	policyStore *policy.Store,
	platformState *platformPolicyState,
	redirectManager redirect.Manager,
	patcher *apply.Patcher,
	tracker *conntrack.Tracker,
	conntrackManager *conntrack.Manager,
	proxyServer *proxy.Server,
) error {
	if netdWatcher == nil || redirectManager == nil || patcher == nil {
		return fmt.Errorf("missing watcher or redirect manager or patcher or policy store")
	}
	sandboxes := netdWatcher.ListSandboxesByNode(d.cfg.NodeName)
	services := netdWatcher.ListServices()
	endpoints := netdWatcher.ListEndpoints()
	sandboxIPs := make([]string, 0, len(sandboxes))
	for _, sandbox := range sandboxes {
		if sandbox.PodIP == "" {
			continue
		}
		sandboxIPs = append(sandboxIPs, sandbox.PodIP)
	}
	if policyStore != nil {
		result := policyStore.ReconcileSandboxes(sandboxes)
		for _, podIP := range result.RemovedIPs {
			if proxyServer != nil {
				proxyServer.ForgetSandboxDNS(podIP)
			}
			cleanupTrackedFlows(ctx, tracker, conntrackManager, podIP)
		}
		for _, change := range result.Changed {
			if change.Initial || change.PodIP == "" {
				continue
			}
			cleanupDeniedTrackedFlows(ctx, tracker, conntrackManager, policyStore, change.PodIP)
		}
	}
	if platformState != nil {
		platformState.Reconcile(sandboxes, services, endpoints)
	}

	dnsCIDRs := clusterDNSCIDRs(d.cfg.ClusterDNSCIDR, services, endpoints)
	bypassCIDRs := []string{}
	if len(d.cfg.PlatformAllowedCIDRs) > 0 {
		bypassCIDRs = append(bypassCIDRs, excludeCIDRs(d.cfg.PlatformAllowedCIDRs, dnsCIDRs)...)
	}
	if policyStore != nil {
		bypassCIDRs = append(bypassCIDRs, excludeCIDRs(policyStore.AllowedPlatformCIDRs(), dnsCIDRs)...)
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

func clusterDNSCIDRs(configured string, services []*watcher.ServiceInfo, endpoints []*watcher.EndpointsInfo) []string {
	out := []string{}
	if strings.TrimSpace(configured) != "" {
		out = append(out, configured)
	}
	endpointsByService := make(map[string]*watcher.EndpointsInfo, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == nil {
			continue
		}
		endpointsByService[endpoint.Namespace+"/"+endpoint.Name] = endpoint
	}
	for _, service := range services {
		if !isClusterDNSService(service) {
			continue
		}
		if service.ClusterIP != "" && strings.ToLower(service.ClusterIP) != "none" {
			out = append(out, service.ClusterIP)
		}
		if endpoint := endpointsByService[service.Namespace+"/"+service.Name]; endpoint != nil {
			out = append(out, endpoint.Addresses...)
		}
	}
	return out
}

func excludeCIDRs(values []string, excluded []string) []string {
	if len(values) == 0 || len(excluded) == 0 {
		return append([]string(nil), values...)
	}
	exclusionSet := make(map[string]struct{}, len(excluded))
	for _, value := range excluded {
		if key := cidrKey(value); key != "" {
			exclusionSet[key] = struct{}{}
		}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := exclusionSet[cidrKey(value)]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}

func cidrKey(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if _, network, err := net.ParseCIDR(value); err == nil && network != nil {
		return network.String()
	}
	if ip := net.ParseIP(value); ip != nil {
		if ip.To4() != nil {
			return ip.String() + "/32"
		}
		return ip.String() + "/128"
	}
	return value
}

func cleanupTrackedFlows(
	ctx context.Context,
	tracker *conntrack.Tracker,
	conntrackManager *conntrack.Manager,
	podIP string,
) int {
	if tracker == nil || podIP == "" {
		return 0
	}
	flows := tracker.PopBySrc(podIP)
	if len(flows) > 0 && conntrackManager != nil {
		conntrackManager.CleanupFlows(ctx, flows)
	}
	return len(flows)
}

func cleanupDeniedTrackedFlows(
	ctx context.Context,
	tracker *conntrack.Tracker,
	conntrackManager *conntrack.Manager,
	policyStore *policy.Store,
	podIP string,
) int {
	if tracker == nil || policyStore == nil || podIP == "" {
		return 0
	}
	flows := tracker.PopBySrc(podIP)
	p := policyStore.GetByIP(podIP)
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
		allowed := policy.AllowEgressL4(p, net.IP(flow.DstIP.AsSlice()), int(flow.DstPort), proto)
		if flow.Host != "" || flow.App != "" {
			allowed = policy.AllowEgressDestination(p, net.IP(flow.DstIP.AsSlice()), int(flow.DstPort), proto, flow.Host, flow.App)
		}
		if !allowed {
			flowsToKill = append(flowsToKill, flow)
		}
	}
	if len(flowsToKill) > 0 && conntrackManager != nil {
		conntrackManager.CleanupFlows(ctx, flowsToKill)
	}
	return len(flowsToKill)
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
	d.waitForMeteringFlushLoop(ctx)
	d.closeRuntimeResources()
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
