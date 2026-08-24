package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	ctldha "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/ha"
	ctldserver "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/server"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
)

var (
	httpAddr                 = ":8095"
	nodeName                 = os.Getenv("NODE_NAME")
	stateRoot                = "/var/lib/sandbox0/ctld"
	haSlot                   = os.Getenv("CTLD_HA_SLOT")
	haProbe                  string
	haProbeSocket            = "/run/sandbox0/ctld-ha.sock"
	haMetricsAddr            string
	networkRuntimeConfigPath = strings.TrimSpace(os.Getenv("CTLD_NETWORK_CONFIG_PATH"))
	runtimeSlotNetworkSocket = "/host-run/sandbox0/ctld-runtime-slot-network.sock"
	runtimeSlotNetNSRoot     = "/host-run/netns"
)

const (
	httpShutdownTimeout           = 5 * time.Second
	runtimeMetricsShutdownTimeout = 7 * time.Second
	nomadRuntimeShutdownTimeout   = 10 * time.Second
	shutdownGraceMargin           = 5 * time.Second
	minimumTerminationGrace       = httpShutdownTimeout + runtimeMetricsShutdownTimeout +
		networkRuntimeShutdownTimeout + nomadRuntimeShutdownTimeout + shutdownGraceMargin
)

var errPrimaryShutdownIncomplete = errors.New("ctld primary service shutdown incomplete")

var retainedPrimaryLeases struct {
	sync.Mutex
	leases []*ctldha.PrimaryLease
}

func main() {
	flag.StringVar(&httpAddr, "http-addr", ":8095", "HTTP listen address for ctld health and metrics")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "Nomad node name")
	flag.StringVar(&stateRoot, "state-root", "/var/lib/sandbox0/ctld", "host-local root for ctld state")
	flag.StringVar(&haSlot, "ha-slot", os.Getenv("CTLD_HA_SLOT"), "stable ctld HA deployment slot")
	flag.StringVar(&haProbe, "ha-probe", "", "run one ctld HA probe (live or ready) and exit")
	flag.StringVar(&haProbeSocket, "ha-probe-socket", "/run/sandbox0/ctld-ha.sock", "container-local ctld HA probe socket")
	flag.StringVar(&haMetricsAddr, "ha-metrics-addr", "", "dedicated pre-election HTTP listen address for ctld HA metrics; empty disables it")
	flag.StringVar(&networkRuntimeConfigPath, "ctld-networking-config-path", strings.TrimSpace(os.Getenv("CTLD_NETWORK_CONFIG_PATH")), "ctld network runtime config path")
	flag.StringVar(&runtimeSlotNetworkSocket, "runtime-slot-network-socket", "/host-run/sandbox0/ctld-runtime-slot-network.sock", "host-visible root-only runtime-slot network control socket")
	flag.StringVar(&runtimeSlotNetNSRoot, "runtime-slot-netns-root", "/host-run/netns", "ctld mount of the host Nomad network namespace root")
	flag.Parse()

	log.Println("Starting ctld")
	defer log.Println("Stopped ctld")
	if err := run(); err != nil {
		log.Fatalf("ctld stopped with error: %v", err)
	}
}

func run() error {
	if haProbe != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return ctldha.RunProbe(ctx, haProbeSocket, haProbe, httpAddr)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()
	cfg, err := config.LoadCtldConfigStrict()
	if err != nil {
		return err
	}
	if !cfg.NomadRuntime.Enabled {
		return fmt.Errorf("ctld requires nomad_runtime.enabled=true")
	}
	if strings.TrimSpace(nodeName) == "" {
		return fmt.Errorf("ctld node name is required")
	}
	networkFactory, err := configuredNetworkRuntimeFactory(networkRuntimeConfigPath, httpAddr)
	if err != nil {
		return fmt.Errorf("validate ctld network runtime config: %w", err)
	}
	nomadFactory, err := configuredNomadRuntimeFactory(cfg, runtimeSlotNetworkSocket)
	if err != nil {
		return fmt.Errorf("validate ctld Nomad runtime config: %w", err)
	}
	if nomadFactory == nil {
		return fmt.Errorf("ctld Nomad runtime factory is required")
	}
	primaryFn := func(ctx context.Context, options primaryRunOptions) error {
		options.ctldConfig = cfg
		options.nomadRuntimeFactory = nomadFactory
		return runPrimary(ctx, options)
	}
	coordinator, err := ctldha.NewCoordinator(ctldha.Config{RootDir: stateRoot, Slot: haSlot})
	if err != nil {
		return err
	}
	haMetricsServer, err := ctldha.StartMetricsServer(ctx, haMetricsAddr, coordinator, nodeName, haSlot)
	if err != nil {
		return err
	}
	if haMetricsServer != nil {
		defer haMetricsServer.Close()
		log.Printf("ctld HA metrics server listening on %q", haMetricsServer.Addr())
	}
	probeServer, err := ctldha.StartProbeServer(ctx, haProbeSocket, coordinator)
	if err != nil {
		return err
	}
	defer probeServer.Close()
	if haMetricsServer == nil {
		return runHAPrimary(ctx, coordinator, probeServer.SetServiceReady, networkFactory, primaryFn)
	}
	primaryErrors := make(chan error, 1)
	go func() {
		primaryErrors <- runHAPrimary(ctx, coordinator, probeServer.SetServiceReady, networkFactory, primaryFn)
	}()
	select {
	case err := <-primaryErrors:
		return err
	case err := <-haMetricsServer.Errors():
		cancel()
		return errors.Join(fmt.Errorf("ctld HA metrics server: %w", err), <-primaryErrors)
	}
}

type primaryRunner func(context.Context, primaryRunOptions) error

func runHAPrimary(
	ctx context.Context,
	coordinator *ctldha.Coordinator,
	setReady func(bool),
	networkFactory primaryServiceFactory,
	runPrimaryFn primaryRunner,
) error {
	lease, err := coordinator.WaitForPrimary(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	runErr := runPrimaryFn(ctx, primaryRunOptions{setReady: setReady, networkFactory: networkFactory})
	// A timed-out privileged service may still own NBD, mount, Bolt, or network
	// resources. Keep the flock open until the process exits so the standby
	// cannot overlap ownership.
	if errors.Is(runErr, errPrimaryShutdownIncomplete) {
		retainPrimaryLeaseUntilExit(lease)
		return runErr
	}
	return errors.Join(runErr, lease.Close())
}

func retainPrimaryLeaseUntilExit(lease *ctldha.PrimaryLease) {
	if lease == nil {
		return
	}
	retainedPrimaryLeases.Lock()
	retainedPrimaryLeases.leases = append(retainedPrimaryLeases.leases, lease)
	retainedPrimaryLeases.Unlock()
}

type primaryRunOptions struct {
	setReady            func(bool)
	networkFactory      primaryServiceFactory
	nomadRuntimeFactory nomadRuntimeFactory
	ctldConfig          *config.CtldConfig
}

type healthController struct {
	ready   func() bool
	healthy func() bool
}

func (c healthController) Ready() bool   { return c.ready == nil || c.ready() }
func (c healthController) Healthy() bool { return c.healthy == nil || c.healthy() }

func runPrimary(parent context.Context, options primaryRunOptions) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if options.ctldConfig == nil || options.networkFactory == nil || options.nomadRuntimeFactory == nil {
		return fmt.Errorf("ctld primary requires config, network runtime, and Nomad runtime")
	}

	logger, err := observability.NewLogger(observability.LoggerConfig{ServiceName: "ctld", Level: "info"})
	if err != nil {
		return fmt.Errorf("create ctld logger: %w", err)
	}
	defer logger.Sync()
	obsProvider, err := observability.New(observability.ConfigFromEnv("ctld", logger))
	if err != nil {
		return fmt.Errorf("create ctld observability: %w", err)
	}
	defer obsProvider.Shutdown(context.WithoutCancel(ctx))

	var dbPool *pgxpool.Pool
	if options.ctldConfig.DatabaseURL != "" {
		dbPool, err = initCtldDatabase(ctx, options.ctldConfig, obsProvider)
		if err != nil {
			log.Printf("ctld object-store request metering disabled: %v", err)
		} else if dbPool != nil {
			defer dbPool.Close()
		}
	}
	objectStoreMeter := startCtldObjectStoreRequestMetering(ctx, options.ctldConfig, dbPool, nodeName, logger)
	defer flushCtldObjectStoreRequestMetering(objectStoreMeter, logger)

	serviceErrors := make(chan error, 1)
	var networkHandle, nomadHandle *primaryServiceHandle
	healthy := func() bool { return ctx.Err() == nil }
	ready := func() bool {
		return healthy() && networkHandle != nil && networkHandle.Ready() && nomadHandle != nil && nomadHandle.Ready()
	}
	httpServer := newHTTPServer(httpAddr, healthController{ready: ready, healthy: healthy})
	httpServer.Handler = httpobs.ServerMiddleware(obsProvider.HTTPServerConfig(logger))(httpServer.Handler)
	httpServer.ConnState = httpobs.NewConnStateTracker(obsProvider.HTTPServerConfig(nil)).Wrap(httpServer.ConnState)
	httpListener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("listen for ctld HTTP server: %w", err)
	}
	defer httpListener.Close()

	networkService, err := options.networkFactory()
	if err != nil || networkService == nil {
		return fmt.Errorf("initialize ctld network runtime: %w", errors.Join(err, nilServiceError(networkService)))
	}
	networkHandle = startPrimaryService(ctx, networkService)
	log.Printf("ctld primary started network runtime")

	nomadService, err := options.nomadRuntimeFactory(logger)
	if err != nil || nomadService == nil {
		cancel()
		shutdownErr := waitPrimaryService(networkHandle, networkRuntimeShutdownTimeout)
		return errors.Join(fmt.Errorf("initialize ctld Nomad runtime: %w", errors.Join(err, nilServiceError(nomadService))), shutdownErr)
	}
	nomadHandle = startPrimaryService(ctx, nomadService)
	log.Printf("ctld primary started Nomad runtime")

	runtimeMetricsHandle := startCtldRuntimeMetrics(
		ctx, options.ctldConfig, options.ctldConfig.NomadRuntime.SocketPath, obsProvider, logger,
	)
	go func() {
		if err := httpServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
			serviceErrors <- fmt.Errorf("ctld HTTP server: %w", err)
		}
	}()
	if options.setReady != nil {
		options.setReady(ready())
		defer options.setReady(false)
		go publishReadiness(ctx, options.setReady, ready)
	}

	var runErr error
	networkFailed, nomadFailed := false, false
	select {
	case <-parent.Done():
		log.Printf("ctld primary shutting down: %v", parent.Err())
	case runErr = <-serviceErrors:
	case serviceErr := <-networkHandle.Errors():
		if err, failed := networkRuntimeExitError(parent.Err(), serviceErr); failed {
			networkFailed, runErr = true, err
		}
	case serviceErr := <-nomadHandle.Errors():
		if err, failed := primaryServiceExitError("Nomad runtime", parent.Err(), serviceErr); failed {
			nomadFailed, runErr = true, err
		}
	}
	cancel()
	if options.setReady != nil {
		options.setReady(false)
	}

	metricsCtx, metricsCancel := context.WithTimeout(context.Background(), runtimeMetricsShutdownTimeout)
	runErr = errors.Join(runErr, runtimeMetricsHandle.Shutdown(metricsCtx))
	metricsCancel()
	httpCtx, httpCancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	runErr = errors.Join(runErr, httpServer.Shutdown(httpCtx))
	httpCancel()
	if !networkFailed {
		runErr = errors.Join(runErr, waitPrimaryService(networkHandle, networkRuntimeShutdownTimeout))
	}
	if !nomadFailed {
		runErr = errors.Join(runErr, waitPrimaryService(nomadHandle, nomadRuntimeShutdownTimeout))
	}
	return runErr
}

func nilServiceError(service primaryService) error {
	if service == nil {
		return fmt.Errorf("factory returned a nil service")
	}
	return nil
}

func publishReadiness(ctx context.Context, publish func(bool), ready func() bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publish(ready())
		}
	}
}

func waitPrimaryService(handle *primaryServiceHandle, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := handle.Wait(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(errPrimaryShutdownIncomplete, err)
	}
	return err
}

func primaryServiceExitError(name string, parentErr, serviceErr error) (error, bool) {
	if parentErr != nil && (serviceErr == nil || errors.Is(serviceErr, context.Canceled)) {
		return nil, false
	}
	if serviceErr == nil {
		serviceErr = fmt.Errorf("service stopped unexpectedly")
	}
	return fmt.Errorf("ctld %s: %w", name, serviceErr), true
}

func networkRuntimeExitError(parentErr, networkErr error) (error, bool) {
	if parentErr != nil && (networkErr == nil || errors.Is(networkErr, context.Canceled)) {
		return nil, false
	}
	if networkErr == nil {
		networkErr = fmt.Errorf("service stopped unexpectedly")
	}
	return fmt.Errorf("ctld network runtime: %w", networkErr), true
}

func newHTTPServer(addr string, controller ctldserver.Controller) *http.Server {
	return &http.Server{Addr: addr, Handler: ctldserver.NewMux(controller), ReadHeaderTimeout: 5 * time.Second}
}

func initCtldDatabase(ctx context.Context, cfg *config.CtldConfig, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	if cfg == nil || cfg.DatabaseURL == "" {
		return nil, nil
	}
	var modifier func(*pgxpool.Config) error
	if obsProvider != nil {
		modifier = obsProvider.Pgx.ConfigModifier()
	}
	return dbpool.New(ctx, dbpool.Options{
		DatabaseURL: cfg.DatabaseURL, MaxConns: int32(cfg.DatabaseMaxConns), MinConns: int32(cfg.DatabaseMinConns),
		DefaultMaxConns: 5, DefaultMinConns: 1, ConfigModifier: modifier,
	})
}
