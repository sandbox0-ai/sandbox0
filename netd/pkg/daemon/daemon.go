package daemon

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/netd/pkg/apply"
	"github.com/sandbox0-ai/infra/netd/pkg/conntrack"
	"github.com/sandbox0-ai/infra/netd/pkg/policy"
	"github.com/sandbox0-ai/infra/netd/pkg/proxy"
	"github.com/sandbox0-ai/infra/netd/pkg/redirect"
	"github.com/sandbox0-ai/infra/netd/pkg/watcher"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Daemon struct {
	cfg           *config.NetdConfig
	logger        *zap.Logger
	healthServer  *http.Server
	metricsServer *http.Server
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
	if err := d.startServers(); err != nil {
		return err
	}
	if err := d.runNetd(ctx); err != nil {
		return err
	}

	<-ctx.Done()

	shutdownCtx := context.Background()
	if d.cfg.ShutdownDelay.Duration > 0 {
		var cancel context.CancelFunc
		shutdownCtx, cancel = context.WithTimeout(context.Background(), d.cfg.ShutdownDelay.Duration)
		defer cancel()
	}
	return d.shutdown(shutdownCtx)
}

func (d *Daemon) runNetd(ctx context.Context) error {
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
		changed, prevHash := policyStore.UpsertFromSandbox(info)
		if changed && info.PodIP != "" {
			flows := tracker.PopBySrc(info.PodIP)
			conntrackManager.CleanupFlows(ctx, flows)
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

	proxyServer, err := proxy.NewServer(d.cfg, policyStore, tracker, d.logger)
	if err != nil {
		return err
	}
	proxyServer.Start(ctx)
	defer proxyServer.Shutdown(context.Background())

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
			if err := d.syncRedirect(ctx, netdWatcher, redirectManager, patcher, policyStore); err != nil {
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
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Daemon) syncRedirect(
	ctx context.Context,
	netdWatcher *watcher.Watcher,
	redirectManager redirect.Manager,
	patcher *apply.Patcher,
	policyStore *policy.Store,
) error {
	if netdWatcher == nil || redirectManager == nil || patcher == nil || policyStore == nil {
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
	for _, sandbox := range sandboxes {
		if sandbox == nil || sandbox.NetworkPolicyHash == "" {
			continue
		}
		policyStore.MarkApplied(sandbox.Namespace, sandbox.Name, sandbox.NetworkPolicyHash)
	}
	patchedCount := 0
	if err := patcher.SyncAppliedHashes(ctx, sandboxes, policyStore); err != nil {
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
