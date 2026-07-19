package daemon

import (
	"context"
	"database/sql"
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
	meteringclickhouse "github.com/sandbox0-ai/sandbox0/pkg/metering/clickhouse"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/activeconnections"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/concurrency"
	teamquotanetwork "github.com/sandbox0-ai/sandbox0/pkg/teamquota/network"
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

type sqlRuntimeResource struct {
	db *sql.DB
}

func (r sqlRuntimeResource) Close() {
	if r.db != nil {
		_ = r.db.Close()
	}
}

type multiRuntimeResource []runtimeResource

func (m multiRuntimeResource) Close() {
	for _, resource := range m {
		closeRuntimeResource(resource)
	}
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
		return fmt.Errorf("ctld network runtime config is nil")
	}
	d.ready.Store(false)
	defer d.ready.Store(false)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	serverExitCh := make(chan error, 2)
	if err := d.startServers(serverExitCh); err != nil {
		shutdownCtx, shutdownCancel := d.shutdownContext()
		defer shutdownCancel()
		_ = d.shutdown(shutdownCtx)
		return err
	}
	proxyExitCh := make(chan error, 1)
	if err := d.runNetd(runCtx, cancel, proxyExitCh); err != nil {
		shutdownCtx, shutdownCancel := d.shutdownContext()
		defer shutdownCancel()
		return errors.Join(err, d.shutdown(shutdownCtx))
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-proxyExitCh:
		cancel()
	case runErr = <-serverExitCh:
		cancel()
	}

	<-runCtx.Done()

	shutdownCtx, shutdownCancel := d.shutdownContext()
	defer shutdownCancel()
	shutdownErr := d.shutdown(shutdownCtx)
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		if shutdownErr != nil {
			d.logger.Error("Shutdown completed with error", zap.Error(shutdownErr))
		}
		return runErr
	}
	return shutdownErr
}

// Ready reports whether the ctld network runtime has successfully synchronized
// node redirect state. Ctld uses it as part of the primary readiness gate.
func (d *Daemon) Ready() bool {
	return d != nil && d.ready.Load()
}

func (d *Daemon) shutdownContext() (context.Context, context.CancelFunc) {
	if d.cfg != nil && d.cfg.ShutdownDelay.Duration > 0 {
		return context.WithTimeout(context.Background(), d.cfg.ShutdownDelay.Duration)
	}
	return context.WithCancel(context.Background())
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
	var databasePool *pgxpool.Pool
	var meteringDB runtimeResource
	runtimeResourcesRegistered := false
	defer func() {
		if runtimeResourcesRegistered {
			return
		}
		if meteringDB != nil {
			meteringDB.Close()
		}
		if databasePool != nil {
			databasePool.Close()
		}
		if conntrackManager != nil {
			conntrackManager.Close()
		}
	}()
	if strings.TrimSpace(d.cfg.DatabaseURL) == "" {
		return fmt.Errorf("DATABASE_URL is required for network team quota enforcement")
	}
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     d.cfg.DatabaseURL,
		DefaultMaxConns: 5,
		DefaultMinConns: 1,
		ConfigModifier:  d.dbConfigModifier(),
	})
	if err != nil {
		return fmt.Errorf("create ctld network runtime database pool: %w", err)
	}
	databasePool = pool
	if err := teamquota.RunMigrations(ctx, databasePool, observability.NewMigrateLogger(d.logger)); err != nil {
		return fmt.Errorf("run team quota migrations: %w", err)
	}
	stateIdentity, err := teamquota.ClaimRegionStateIdentity(
		ctx,
		databasePool,
		teamquota.RegionStateIdentityConfig{
			RegionID:        d.cfg.RegionID,
			ExpectedStateID: d.cfg.TeamQuotaDistributedEnforcement.StateID,
			RedisURL:        d.cfg.TeamQuotaDistributedEnforcement.RedisURL,
			RedisKeyPrefix:  d.cfg.TeamQuotaDistributedEnforcement.RedisKeyPrefix,
			RedisTimeout:    d.cfg.TeamQuotaDistributedEnforcement.RedisTimeout.Duration,
		},
	)
	if err != nil {
		return fmt.Errorf("validate Team Quota region state identity: %w", err)
	}
	d.cfg.TeamQuotaDistributedEnforcement.RedisKeyPrefix = stateIdentity.KeyPrefix
	if d.cfg.Metering.Enabled {
		db, err := d.openMetering(ctx)
		if err != nil {
			return err
		}
		meteringDB = sqlRuntimeResource{db: db}
		usageAggregator = netdmetering.NewAggregator(
			netdmetering.NewRecorder(meteringoutbox.NewRepository(databasePool)),
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

	if strings.TrimSpace(d.cfg.TeamQuotaDistributedEnforcement.RedisURL) == "" {
		return fmt.Errorf("team quota Redis URL is required for network quota enforcement")
	}
	teamQuotaRepo := teamquota.NewRepository(databasePool)
	networkLimiter, err := teamquotanetwork.NewRedis(
		ctx,
		teamQuotaRepo,
		teamquotanetwork.Config{
			RegionID:       d.cfg.RegionID,
			RedisURL:       d.cfg.TeamQuotaDistributedEnforcement.RedisURL,
			RedisKeyPrefix: d.cfg.TeamQuotaDistributedEnforcement.RedisKeyPrefix,
			RedisTimeout:   d.cfg.TeamQuotaDistributedEnforcement.RedisTimeout.Duration,
			PolicyCacheTTL: d.cfg.TeamQuotaDistributedEnforcement.PolicyCacheTTL.Duration,
		},
	)
	if err != nil {
		return fmt.Errorf("create network team quota enforcer: %w", err)
	}
	networkQuota := &proxy.TeamNetworkQuota{Limiter: networkLimiter}
	activeConnectionQuota, err := activeconnections.NewRedis(
		ctx,
		teamQuotaRepo,
		concurrency.Config{
			RegionID:       d.cfg.RegionID,
			RedisURL:       d.cfg.TeamQuotaDistributedEnforcement.RedisURL,
			RedisKeyPrefix: d.cfg.TeamQuotaDistributedEnforcement.RedisKeyPrefix,
			RedisTimeout:   d.cfg.TeamQuotaDistributedEnforcement.RedisTimeout.Duration,
			PolicyCacheTTL: d.cfg.TeamQuotaDistributedEnforcement.PolicyCacheTTL.Duration,
			LeaseTTL:       d.cfg.TeamQuotaDistributedEnforcement.LeaseTTL.Duration,
			RenewInterval:  d.cfg.TeamQuotaDistributedEnforcement.RenewInterval.Duration,
		},
	)
	if err != nil {
		_ = networkQuota.Close()
		return fmt.Errorf("create active connection team quota enforcer: %w", err)
	}
	networkQuotaOwnedByProxy := false
	defer func() {
		if !networkQuotaOwnedByProxy {
			_ = networkQuota.Close()
			_ = activeConnectionQuota.Close()
		}
	}()

	proxyOpts := []proxy.ServerOption{
		proxy.WithTeamNetworkQuota(networkQuota),
		proxy.WithActiveConnectionQuota(activeConnectionQuota),
	}
	if d.cfg.EgressAuthResolverURL != "" {
		privateKey, keyErr := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
		if keyErr != nil {
			return fmt.Errorf("load ctld network runtime internal auth private key: %w", keyErr)
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
	networkQuotaOwnedByProxy = true
	d.proxyServer = proxyServer
	if d.cfg.EgressAuthResolverURL != "" && databasePool != nil {
		startCredentialSourceRotationListener(ctx, databasePool, d.logger, proxyServer)
	}
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
		if meteringDB != nil {
			meteringCloser = multiRuntimeResource{meteringDB}
		}
		if databasePool != nil {
			if meteringCloser == nil {
				meteringCloser = databasePool
			} else {
				meteringCloser = append(meteringCloser.(multiRuntimeResource), databasePool)
			}
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

func (d *Daemon) openMetering(ctx context.Context) (*sql.DB, error) {
	if d == nil || d.cfg == nil || !d.cfg.Metering.Enabled {
		return nil, nil
	}
	ch := d.cfg.Metering.ClickHouse
	timeout := ch.ConnectTimeout.Duration
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	openConfig := meteringclickhouse.OpenConfig{
		DSN: strings.TrimSpace(ch.DSN),
		Schema: meteringclickhouse.Config{
			Database:          ch.Database,
			EventsTable:       ch.EventsTable,
			WindowsTable:      ch.WindowsTable,
			WatermarksTable:   ch.WatermarksTable,
			SandboxStateTable: ch.SandboxStateTable,
			StorageStateTable: ch.StorageStateTable,
		},
		Migrate: !ch.SkipSchemaMigration,
	}
	db, _, err := meteringclickhouse.Open(connectCtx, openConfig)
	if err != nil {
		deferredDB, _, deferredErr := meteringclickhouse.OpenDeferred(openConfig)
		if deferredErr != nil {
			return nil, fmt.Errorf("initialize deferred clickhouse metering backend after %v: %w", err, deferredErr)
		}
		d.logger.Warn("Metering ClickHouse backend is unavailable; usage capture will continue in PostgreSQL", zap.Error(err))
		return deferredDB, nil
	}
	d.logger.Info("Metering ClickHouse backend initialized",
		zap.String("database", ch.Database),
		zap.String("events_table", ch.EventsTable),
		zap.String("windows_table", ch.WindowsTable),
		zap.Bool("schema_migration", !ch.SkipSchemaMigration),
	)
	return db, nil
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
				d.logger.Error("Failed to flush ctld network runtime metering windows during shutdown", zap.Error(err))
			}
			return
		case <-ticker.C:
			if err := aggregator.Flush(ctx); err != nil {
				d.logger.Error("Failed to flush ctld network runtime metering windows", zap.Error(err))
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
		d.logger.Warn("Timed out waiting for ctld network runtime metering flush loop to stop", zap.Error(ctx.Err()))
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
) (err error) {
	started := time.Now()
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
		}
		daemonMetrics.RecordRedirectSync(result, time.Since(started))
	}()
	if netdWatcher == nil || redirectManager == nil || patcher == nil {
		return fmt.Errorf("missing watcher or redirect manager or patcher or policy store")
	}
	// Redirect rules only need local source pods, while platform peer deny must
	// know every active sandbox so cross-node private traffic is still blocked.
	stageStarted := time.Now()
	localSandboxes := netdWatcher.ListSandboxesByNode(d.cfg.NodeName)
	allSandboxes := localSandboxes
	if d.cfg.NodeName != "" {
		allSandboxes = netdWatcher.ListSandboxesByNode("")
	}
	services := netdWatcher.ListServices()
	endpoints := netdWatcher.ListEndpoints()
	sandboxIPs := make([]string, 0, len(localSandboxes))
	for _, sandbox := range localSandboxes {
		if sandbox.PodIP == "" {
			continue
		}
		sandboxIPs = append(sandboxIPs, sandbox.PodIP)
	}
	daemonMetrics.RecordRedirectSyncStage("list_inputs", "success", time.Since(stageStarted))
	daemonMetrics.SetRedirectSyncObjectCount("local_sandboxes", len(localSandboxes))
	daemonMetrics.SetRedirectSyncObjectCount("total_sandboxes", len(allSandboxes))
	daemonMetrics.SetRedirectSyncObjectCount("services", len(services))
	daemonMetrics.SetRedirectSyncObjectCount("endpoints", len(endpoints))
	daemonMetrics.SetRedirectSyncObjectCount("sandbox_ips", len(sandboxIPs))

	policyChanged := 0
	policyRemovedIPs := 0
	if policyStore != nil {
		stageStarted = time.Now()
		result := policyStore.ReconcileSandboxes(localSandboxes)
		policyChanged = len(result.Changed)
		policyRemovedIPs = len(result.RemovedIPs)
		for _, podIP := range result.RemovedIPs {
			if proxyServer != nil {
				proxyServer.ForgetSandboxDNS(podIP)
				proxyServer.ForgetSandboxUDPSessions(podIP)
			}
			cleanupTrackedFlows(ctx, tracker, conntrackManager, podIP)
		}
		for _, change := range result.Changed {
			if change.Initial || change.PodIP == "" {
				continue
			}
			if proxyServer != nil {
				proxyServer.ForgetSandboxUDPSessions(change.PodIP)
			}
			cleanupDeniedTrackedFlows(ctx, tracker, conntrackManager, policyStore, change.PodIP)
		}
		daemonMetrics.RecordRedirectSyncStage("policy_reconcile", "success", time.Since(stageStarted))
	}
	daemonMetrics.SetRedirectSyncObjectCount("policy_changed", policyChanged)
	daemonMetrics.SetRedirectSyncObjectCount("policy_removed_ips", policyRemovedIPs)

	if platformState != nil {
		stageStarted = time.Now()
		platformState.Reconcile(allSandboxes, services, endpoints)
		daemonMetrics.RecordRedirectSyncStage("platform_reconcile", "success", time.Since(stageStarted))
	}

	dnsCIDRs := clusterDNSCIDRs(d.cfg.ClusterDNSCIDR, services, endpoints)
	configuredBypassCIDRs := []string{}
	if len(d.cfg.PlatformAllowedCIDRs) > 0 {
		configuredBypassCIDRs = append(configuredBypassCIDRs, d.cfg.PlatformAllowedCIDRs...)
	}
	platformBypassCIDRs := []string{}
	if policyStore != nil {
		platformBypassCIDRs = append(platformBypassCIDRs, policyStore.AllowedPlatformCIDRs()...)
	}
	bypassCIDRs := redirectBypassCIDRs(dnsCIDRs, configuredBypassCIDRs, platformBypassCIDRs)
	daemonMetrics.SetRedirectSyncObjectCount("bypass_cidrs", len(bypassCIDRs))

	d.logger.Info(
		"Syncing redirect rules",
		zap.Int("sandboxes_local", len(localSandboxes)),
		zap.Int("sandboxes_total", len(allSandboxes)),
		zap.Int("sandbox_ips", len(sandboxIPs)),
		zap.Strings("sandbox_ips", sandboxIPs),
		zap.Strings("bypass_cidrs", bypassCIDRs),
	)
	stageStarted = time.Now()
	if err := redirectManager.Sync(ctx, sandboxIPs, bypassCIDRs); err != nil {
		daemonMetrics.RecordRedirectSyncStage("redirect_sync", "error", time.Since(stageStarted))
		return err
	}
	daemonMetrics.RecordRedirectSyncStage("redirect_sync", "success", time.Since(stageStarted))

	patchedCount := 0
	stageStarted = time.Now()
	if err := patcher.SyncAppliedHashes(ctx, localSandboxes); err != nil {
		daemonMetrics.RecordRedirectSyncStage("patch_applied_hashes", "error", time.Since(stageStarted))
		d.logger.Warn("Failed to sync applied hashes", zap.Error(err))
	} else {
		daemonMetrics.RecordRedirectSyncStage("patch_applied_hashes", "success", time.Since(stageStarted))
		for _, sandbox := range localSandboxes {
			if sandbox.NetworkPolicyHash != "" && sandbox.NetworkPolicyHash == sandbox.NetworkAppliedHash {
				patchedCount++
			}
		}
	}
	daemonMetrics.SetRedirectSyncObjectCount("patched_hashes", patchedCount)
	d.logger.Info("Redirect rules synced",
		zap.Int("sandboxes_patched", patchedCount),
		zap.Int("sandboxes_local", len(localSandboxes)),
		zap.Int("sandboxes_total", len(allSandboxes)),
	)
	return nil
}

func redirectBypassCIDRs(dnsCIDRs, configuredCIDRs, platformCIDRs []string) []string {
	out := make([]string, 0, len(dnsCIDRs)+len(configuredCIDRs)+len(platformCIDRs))
	out = append(out, dnsCIDRs...)
	out = append(out, configuredCIDRs...)
	out = append(out, platformCIDRs...)
	return out
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

func (d *Daemon) startServers(serverErrors chan<- error) error {
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

	if err := d.listenAndServe(d.healthServer, "health", serverErrors); err != nil {
		return err
	}
	if err := d.listenAndServe(d.metricsServer, "metrics", serverErrors); err != nil {
		return err
	}

	return nil
}

func (d *Daemon) listenAndServe(server *http.Server, name string, serverErrors chan<- error) error {
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
			wrapped := fmt.Errorf("%s HTTP server: %w", name, err)
			d.logger.Error("HTTP server error",
				zap.String("name", name),
				zap.Error(err),
			)
			select {
			case serverErrors <- wrapped:
			default:
			}
		}
	}()
	return nil
}

func (d *Daemon) shutdown(ctx context.Context) error {
	d.ready.Store(false)
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
