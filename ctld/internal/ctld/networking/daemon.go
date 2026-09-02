package networking

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
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/conntrack"
	networkmetering "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/metering"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/proxy"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/redirect"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/slotnetwork"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	meteringclickhouse "github.com/sandbox0-ai/sandbox0/pkg/metering/clickhouse"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"go.uber.org/zap"
)

type Daemon struct {
	cfg                      *config.NetworkRuntimeConfig
	logger                   *zap.Logger
	healthServer             *http.Server
	metricsServer            *http.Server
	proxyServer              *proxy.Server
	obsProvider              *observability.Provider
	runtimeSlotStatePath     string
	runtimeSlotControlSocket string
	runtimeSlotNetNSRoot     string
	runtimeMu                sync.Mutex
	conntrackCloser          runtimeResource
	meteringCloser           runtimeResource
	meteringDone             <-chan struct{}
	runtimeSlotRegistry      runtimeSlotRegistryCloser
	runtimeSlotControl       runtimeSlotControlCloser
	ready                    atomic.Bool
}

type Options struct {
	RuntimeSlotStatePath     string
	RuntimeSlotControlSocket string
	RuntimeSlotNetNSRoot     string
}

type runtimeResource interface {
	Close()
}

type runtimeSlotRegistryCloser interface {
	Close() error
}

type runtimeSlotControlCloser interface {
	Shutdown(context.Context) error
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

func New(cfg *config.NetworkRuntimeConfig, logger *zap.Logger, obsProvider *observability.Provider, options Options) *Daemon {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Daemon{
		cfg:                      cfg,
		logger:                   logger,
		obsProvider:              obsProvider,
		runtimeSlotStatePath:     strings.TrimSpace(options.RuntimeSlotStatePath),
		runtimeSlotControlSocket: strings.TrimSpace(options.RuntimeSlotControlSocket),
		runtimeSlotNetNSRoot:     strings.TrimSpace(options.RuntimeSlotNetNSRoot),
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
	if err := d.runNetworking(runCtx, cancel, proxyExitCh); err != nil {
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

func (d *Daemon) runNetworking(ctx context.Context, cancel context.CancelFunc, proxyExitCh chan<- error) error {
	if d.cfg.NodeName == "" {
		return fmt.Errorf("node name is required")
	}
	policyStore := policy.NewStore(d.logger)
	platformState := newPlatformPolicyState(d.cfg, policyStore, d.logger)
	conntrackManager, err := conntrack.NewManager(d.logger)
	if err != nil {
		d.logger.Warn("Conntrack manager disabled", zap.Error(err))
	}
	tracker := conntrack.NewTracker()
	var usageAggregator *networkmetering.Aggregator
	var databasePool *pgxpool.Pool
	var quotaRepo *quota.Repository
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
	if d.cfg.DatabaseURL != "" {
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
		if err := quota.RunMigrations(ctx, databasePool, observability.NewMigrateLogger(d.logger)); err != nil {
			return fmt.Errorf("run quota migrations: %w", err)
		}
		quotaRepo = quota.NewRepository(databasePool)
		if err := quotaRepo.EnsureDefaultPolicies(ctx, "ctld_network_legacy_bandwidth", networkTeamQuotaDefaults(d.cfg)); err != nil {
			return fmt.Errorf("bootstrap network quota defaults: %w", err)
		}
	}
	if d.cfg.Metering.Enabled {
		if databasePool == nil {
			return fmt.Errorf("DATABASE_URL is required when metering is enabled")
		}
		db, _, err := d.openMetering(ctx)
		if err != nil {
			return err
		}
		meteringDB = sqlRuntimeResource{db: db}
		usageAggregator = networkmetering.NewAggregator(
			networkmetering.NewRecorder(meteringoutbox.NewRepository(databasePool)),
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
	runtimeSlotRegistry, runtimeSlotControl, err := d.startRuntimeSlotNetworkControl(triggerSync)
	if err != nil {
		return err
	}
	if runtimeSlotControl != nil && proxyExitCh != nil {
		go func() {
			select {
			case <-ctx.Done():
				return
			case controlErr := <-runtimeSlotControl.Errors():
				select {
				case proxyExitCh <- controlErr:
				default:
				}
				if cancel != nil {
					cancel()
				}
			}
		}()
	}

	proxyOpts := []proxy.ServerOption{}
	if quotaRepo != nil {
		if strings.TrimSpace(d.cfg.RedisURL) == "" {
			d.logger.Warn("Network Team Quota is using node-local token state; configure region Redis for cross-node enforcement")
		}
		policies, policyErr := quota.NewCachedPolicyStore(ctx, databasePool, quotaRepo, quota.DefaultPolicyCacheTTL)
		if policyErr != nil {
			return fmt.Errorf("create network quota policy cache: %w", policyErr)
		}
		teamQuotaOption, optionErr := proxy.WithTeamQuotaBandwidth(ctx, d.cfg, policies)
		if optionErr != nil {
			_ = policies.Close()
			return fmt.Errorf("create network quota limiter: %w", optionErr)
		}
		if teamQuotaOption != nil {
			proxyOpts = append(proxyOpts, teamQuotaOption)
		}
	}
	if d.cfg.EgressAuthResolverURL != "" {
		privateKey, keyErr := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
		if keyErr != nil {
			return fmt.Errorf("load ctld network runtime internal auth private key: %w", keyErr)
		}
		tokenGenerator := internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     "ctld",
			PrivateKey: privateKey,
			TTL:        30 * time.Second,
		})
		proxyOpts = append(proxyOpts, proxy.WithEgressAuthResolver(proxy.NewHTTPEgressAuthResolverWithHTTPClient(
			d.cfg.EgressAuthResolverURL,
			d.cfg.EgressAuthResolverTimeout.Duration,
			networkingEgressAuthTokenProvider{generator: tokenGenerator},
			d.egressAuthHTTPClient(),
		)))
	}
	proxyServer, err := proxy.NewServer(d.cfg, policyStore, tracker, usageAggregator, d.logger, proxyOpts...)
	if err != nil {
		return err
	}
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

	syncOnce := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(d.cfg.ResyncPeriod.Duration)
		defer ticker.Stop()
		triggerSync()
		for {
			forceRedirectSync := false
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				forceRedirectSync = true
			case <-syncTrigger:
			}
			syncErr := d.syncRedirect(ctx, runtimeSlotRegistry, policyStore, platformState, redirectManager, tracker, conntrackManager, proxyServer, forceRedirectSync)
			if syncErr == nil && forceRedirectSync && runtimeSlotRegistry != nil {
				pruned, err := runtimeSlotRegistry.Prune(time.Now())
				if err != nil {
					syncErr = fmt.Errorf("prune runtime slot network registry: %w", err)
				} else if pruned > 0 {
					d.logger.Info("Pruned terminal runtime slot network records", zap.Int("records", pruned))
				}
			}
			if syncErr != nil {
				d.logger.Error("Failed to synchronize ctld network runtime", zap.Error(syncErr))
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

func networkTeamQuotaDefaults(cfg *config.NetworkRuntimeConfig) []quota.DefaultLimit {
	if cfg == nil {
		return nil
	}
	defaults := make([]quota.DefaultLimit, 0, 2)
	add := func(dimension quota.Dimension, rate int64) {
		if rate <= 0 {
			return
		}
		burst := cfg.TeamBandwidthBurstBytes
		if burst <= 0 {
			burst = rate
		}
		defaults = append(defaults, quota.DefaultLimit{
			Dimension:  dimension,
			LimitValue: rate,
			IntervalMS: int64(time.Second / time.Millisecond),
			BurstValue: burst,
		})
	}
	add(quota.DimensionNetworkEgress, cfg.TeamEgressBandwidthBytesPerSecond)
	add(quota.DimensionNetworkIngress, cfg.TeamIngressBandwidthBytesPerSecond)
	return defaults
}

func (d *Daemon) dbConfigModifier() func(*pgxpool.Config) error {
	if d == nil || d.obsProvider == nil {
		return nil
	}
	return d.obsProvider.Pgx.ConfigModifier()
}

func (d *Daemon) openMetering(ctx context.Context) (*sql.DB, *meteringclickhouse.Repository, error) {
	if d == nil || d.cfg == nil || !d.cfg.Metering.Enabled {
		return nil, nil, nil
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
	db, repo, err := meteringclickhouse.Open(connectCtx, openConfig)
	if err != nil {
		deferredDB, deferredRepo, deferredErr := meteringclickhouse.OpenDeferred(openConfig)
		if deferredErr != nil {
			return nil, nil, fmt.Errorf("initialize deferred clickhouse metering backend after %v: %w", err, deferredErr)
		}
		d.logger.Warn("Metering ClickHouse backend is unavailable; usage capture will continue in PostgreSQL", zap.Error(err))
		return deferredDB, deferredRepo, nil
	}
	d.logger.Info("Metering ClickHouse backend initialized",
		zap.String("database", ch.Database),
		zap.String("events_table", ch.EventsTable),
		zap.String("windows_table", ch.WindowsTable),
		zap.Bool("schema_migration", !ch.SkipSchemaMigration),
	)
	return db, repo, nil
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

type networkingEgressAuthTokenProvider struct {
	generator *internalauth.Generator
}

func (p networkingEgressAuthTokenProvider) Token(context.Context) (string, error) {
	if p.generator == nil {
		return "", fmt.Errorf("internal auth generator is not configured")
	}
	return p.generator.GenerateSystem("manager", internalauth.GenerateOptions{})
}

func (d *Daemon) runMeteringFlushLoop(ctx context.Context, aggregator *networkmetering.Aggregator) {
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

func (d *Daemon) startMeteringFlushLoop(ctx context.Context, aggregator *networkmetering.Aggregator) {
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

func (d *Daemon) startRuntimeSlotNetworkControl(notify func()) (*slotnetwork.Registry, *slotnetwork.ControlServer, error) {
	configured := 0
	for _, value := range []string{d.runtimeSlotStatePath, d.runtimeSlotControlSocket, d.runtimeSlotNetNSRoot} {
		if value != "" {
			configured++
		}
	}
	if configured == 0 {
		return nil, nil, nil
	}
	if configured != 3 {
		return nil, nil, fmt.Errorf("runtime slot network state, control socket, and netns root must be configured together")
	}
	registry, err := slotnetwork.NewRegistry(slotnetwork.Config{
		StatePath: d.runtimeSlotStatePath, NetNSRoot: d.runtimeSlotNetNSRoot,
		NodeID: d.cfg.NodeName,
	}, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("initialize runtime slot network registry: %w", err)
	}
	if _, err := registry.Prune(time.Now()); err != nil {
		_ = registry.Close()
		return nil, nil, fmt.Errorf("prune runtime slot network registry: %w", err)
	}
	registry.SetNotify(notify)
	control, err := slotnetwork.StartControlServer(d.runtimeSlotControlSocket, registry)
	if err != nil {
		_ = registry.Close()
		return nil, nil, fmt.Errorf("start runtime slot network control server: %w", err)
	}
	d.runtimeMu.Lock()
	d.runtimeSlotRegistry = registry
	d.runtimeSlotControl = control
	d.runtimeMu.Unlock()
	return registry, control, nil
}

func (d *Daemon) closeRuntimeSlotNetworkControl(ctx context.Context) error {
	d.runtimeMu.Lock()
	registry := d.runtimeSlotRegistry
	control := d.runtimeSlotControl
	d.runtimeSlotRegistry = nil
	d.runtimeSlotControl = nil
	d.runtimeMu.Unlock()
	var result error
	// Stop accepting and drain control requests before closing the registry
	// they use. Closing in the opposite order exposes a transient 503 to an
	// otherwise valid in-flight manager request during ctld HA handoff.
	if control != nil {
		result = control.Shutdown(ctx)
	}
	if registry != nil {
		result = errors.Join(result, registry.Close())
	}
	return result
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
	runtimeSlots *slotnetwork.Registry,
	policyStore *policy.Store,
	platformState *platformPolicyState,
	redirectManager redirect.Manager,
	tracker *conntrack.Tracker,
	conntrackManager *conntrack.Manager,
	proxyServer *proxy.Server,
	forceRedirectSync bool,
) (err error) {
	started := time.Now()
	defer func() {
		result := "success"
		if err != nil {
			result = "error"
		}
		daemonMetrics.RecordRedirectSync(result, time.Since(started))
	}()
	if redirectManager == nil || policyStore == nil || runtimeSlots == nil {
		return fmt.Errorf("runtime-slot registry, redirect manager, and policy store are required")
	}

	stageStarted := time.Now()
	sandboxes, revision, err := runtimeSlots.Snapshot()
	if err != nil {
		return fmt.Errorf("snapshot runtime-slot network policies: %w", err)
	}
	stats := runtimeSlots.Stats()
	daemonMetrics.SetRedirectSyncObjectCount("runtime_slot_warm", stats.Warm)
	daemonMetrics.SetRedirectSyncObjectCount("runtime_slot_claimed", stats.Claimed)
	daemonMetrics.SetRedirectSyncObjectCount("runtime_slot_orphaned", stats.Orphaned)
	daemonMetrics.SetRedirectSyncObjectCount("runtime_slot_terminal", stats.Terminal)
	pendingRevisions := uint64(0)
	if stats.Revision > stats.AppliedRevision {
		pendingRevisions = stats.Revision - stats.AppliedRevision
	}
	daemonMetrics.SetRedirectSyncObjectCount("runtime_slot_pending_revisions", int(pendingRevisions))

	sourceIPs := make([]string, 0, len(sandboxes))
	for _, sandbox := range sandboxes {
		if sandbox != nil && sandbox.SourceIP != "" {
			sourceIPs = append(sourceIPs, sandbox.SourceIP)
		}
	}
	daemonMetrics.RecordRedirectSyncStage("list_inputs", "success", time.Since(stageStarted))
	daemonMetrics.SetRedirectSyncObjectCount("local_sandboxes", len(sandboxes))
	daemonMetrics.SetRedirectSyncObjectCount("total_sandboxes", len(sandboxes))
	daemonMetrics.SetRedirectSyncObjectCount("runtime_slot_sandboxes", len(sandboxes))
	daemonMetrics.SetRedirectSyncObjectCount("sandbox_ips", len(sourceIPs))

	stageStarted = time.Now()
	result := policyStore.ReconcileSandboxes(sandboxes)
	for _, sourceIP := range result.RemovedIPs {
		if proxyServer != nil {
			proxyServer.ForgetSandboxDNS(sourceIP)
		}
		cleanupTrackedFlows(ctx, tracker, conntrackManager, sourceIP)
	}
	for _, change := range result.Changed {
		if change.Initial || change.SourceIP == "" {
			continue
		}
		cleanupDeniedTrackedFlows(ctx, tracker, conntrackManager, policyStore, change.SourceIP)
	}
	for _, sandbox := range sandboxes {
		compiled := policyStore.GetByIP(sandbox.SourceIP)
		if compiled == nil || compiled.OwnerKind != sandbox.OwnerKind ||
			compiled.SandboxID != sandbox.SandboxID || compiled.TeamID != sandbox.TeamID {
			return fmt.Errorf("runtime-slot network policy %s was not compiled into the exact source IP", sandbox.Key())
		}
	}
	daemonMetrics.RecordRedirectSyncStage("policy_reconcile", "success", time.Since(stageStarted))
	daemonMetrics.SetRedirectSyncObjectCount("policy_changed", len(result.Changed))
	daemonMetrics.SetRedirectSyncObjectCount("policy_removed_ips", len(result.RemovedIPs))

	if platformState != nil {
		stageStarted = time.Now()
		platformState.Reconcile(sandboxes)
		daemonMetrics.RecordRedirectSyncStage("platform_reconcile", "success", time.Since(stageStarted))
	}

	configuredBypassCIDRs := append([]string(nil), d.cfg.DNSResolverCIDRs...)
	configuredBypassCIDRs = append(configuredBypassCIDRs, d.cfg.PlatformAllowedCIDRs...)
	platformBypassCIDRs := policyStore.AllowedPlatformCIDRs()
	bypassCIDRs := redirectBypassCIDRs(configuredBypassCIDRs, platformBypassCIDRs)
	daemonMetrics.SetRedirectSyncObjectCount("bypass_cidrs", len(bypassCIDRs))

	d.logger.Info("Syncing redirect rules",
		zap.Int("sandboxes", len(sandboxes)),
		zap.Strings("source_ips", sourceIPs),
		zap.Strings("bypass_cidrs", bypassCIDRs),
	)
	stageStarted = time.Now()
	var redirectErr error
	if forceRedirectSync {
		redirectErr = redirectManager.ForceSync(ctx, sourceIPs, bypassCIDRs)
	} else {
		redirectErr = redirectManager.Sync(ctx, sourceIPs, bypassCIDRs)
	}
	if redirectErr != nil {
		daemonMetrics.RecordRedirectSyncStage("redirect_sync", "error", time.Since(stageStarted))
		return redirectErr
	}
	daemonMetrics.RecordRedirectSyncStage("redirect_sync", "success", time.Since(stageStarted))
	runtimeSlots.Acknowledge(revision)
	d.logger.Info("Redirect rules synced", zap.Int("sandboxes", len(sandboxes)))
	return nil
}

func redirectBypassCIDRs(parts ...[]string) []string {
	var size int
	for _, values := range parts {
		size += len(values)
	}
	out := make([]string, 0, size)
	for _, values := range parts {
		out = append(out, values...)
	}
	return out
}

func cleanupTrackedFlows(
	ctx context.Context,
	tracker *conntrack.Tracker,
	conntrackManager *conntrack.Manager,
	sourceIP string,
) int {
	if tracker == nil || sourceIP == "" {
		return 0
	}
	flows := tracker.PopBySrc(sourceIP)
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
	sourceIP string,
) int {
	if tracker == nil || policyStore == nil || sourceIP == "" {
		return 0
	}
	flows := tracker.PopBySrc(sourceIP)
	p := policyStore.GetByIP(sourceIP)
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
	if err := d.closeRuntimeSlotNetworkControl(ctx); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
		d.logger.Error("Failed to shutdown runtime slot network control", zap.Error(err))
	}
	if d.proxyServer != nil {
		if err := d.proxyServer.Shutdown(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
			d.logger.Error("Failed to shutdown proxy server", zap.Error(err))
		}
	}
	if d.healthServer != nil {
		if err := d.healthServer.Shutdown(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
			d.logger.Error("Failed to shutdown health server", zap.Error(err))
		}
	}
	if d.metricsServer != nil {
		if err := d.metricsServer.Shutdown(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
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
