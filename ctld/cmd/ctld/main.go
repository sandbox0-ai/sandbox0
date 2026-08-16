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
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	ctldha "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/ha"
	ctldpower "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/power"
	ctldrootfs "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfs"
	ctldruntimewatch "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/runtimewatch"
	ctldserver "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/server"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/k8s"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

var (
	httpAddr                       = ":8095"
	runtimeWatchAddr               = fmt.Sprintf(":%d", runtimecontrol.DefaultCtldWatchPort)
	kubeconfig                     = ""
	criEndpoint                    = "/host-run/containerd/containerd.sock"
	containerdEndpoint             = "/host-run/containerd/containerd.sock"
	containerdRoot                 = "/host-run/containerd"
	containerdHostRoot             = "/run/containerd"
	containerdDataRoot             = "/host-var-lib/containerd"
	containerdHostDataRoot         = "/var/lib/containerd"
	containerdNamespace            = "k8s.io"
	nodeName                       = os.Getenv("NODE_NAME")
	stateRoot                      = "/var/lib/sandbox0/ctld"
	rootFSObjectCacheMaxBytes      = "20Gi"
	rootFSObjectCacheMinFreeBytes  = "0"
	rootFSObjectCacheMaxAge        time.Duration
	rootFSObjectCacheSweepInterval = time.Minute
	podName                        = os.Getenv("POD_NAME")
	haSlot                         = os.Getenv("CTLD_HA_SLOT")
	haProbe                        string
	haProbeSocket                  = "/run/sandbox0/ctld-ha.sock"
	haMetricsAddr                  string
	networkRuntimeConfigPath       = strings.TrimSpace(os.Getenv("CTLD_NETWORK_CONFIG_PATH"))
)

const (
	httpShutdownTimeout           = 5 * time.Second
	runtimeMetricsShutdownTimeout = 7 * time.Second
	shutdownGraceMargin           = 5 * time.Second
	minimumTerminationGrace       = httpShutdownTimeout + runtimeMetricsShutdownTimeout + shutdownGraceMargin
)

func main() {
	flag.StringVar(&httpAddr, "http-addr", ":8095", "HTTP listen address for ctld health and control endpoints")
	flag.StringVar(
		&runtimeWatchAddr,
		"runtime-watch-addr",
		fmt.Sprintf(":%d", runtimecontrol.DefaultCtldWatchPort),
		"dedicated HTTP listen address for the procd runtime event stream",
	)
	flag.StringVar(&kubeconfig, "kubeconfig", "", "optional kubeconfig path used by ctld")
	flag.StringVar(&criEndpoint, "cri-endpoint", "/host-run/containerd/containerd.sock", "host CRI socket used to read pod sandbox stats")
	flag.StringVar(&containerdEndpoint, "containerd-endpoint", "/host-run/containerd/containerd.sock", "host containerd socket used for rootfs diff/apply")
	flag.StringVar(&containerdRoot, "containerd-root", "/host-run/containerd", "host containerd runtime root mounted into ctld")
	flag.StringVar(&containerdHostRoot, "containerd-host-root", "/run/containerd", "host containerd runtime root path used in containerd mount requests")
	flag.StringVar(&containerdDataRoot, "containerd-data-root", "/host-var-lib/containerd", "host containerd data root mounted into ctld")
	flag.StringVar(&containerdHostDataRoot, "containerd-host-data-root", "/var/lib/containerd", "host containerd data root path returned by containerd snapshotters")
	flag.StringVar(&containerdNamespace, "containerd-namespace", "k8s.io", "containerd namespace used by Kubernetes")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "current node name used to validate local sandbox ownership")
	flag.StringVar(&stateRoot, "state-root", "/var/lib/sandbox0/ctld", "host-local root for ctld rootfs state and cache")
	flag.StringVar(&rootFSObjectCacheMaxBytes, "rootfs-object-cache-max-bytes", "20Gi", "maximum node-local rootfs object cache size; set to 0 to disable")
	flag.StringVar(&rootFSObjectCacheMinFreeBytes, "rootfs-object-cache-min-free-bytes", "0", "minimum free bytes to preserve on the rootfs object cache filesystem")
	flag.DurationVar(&rootFSObjectCacheMaxAge, "rootfs-object-cache-max-age", 0, "maximum age for node-local rootfs cache objects; 0 disables age-based eviction")
	flag.DurationVar(&rootFSObjectCacheSweepInterval, "rootfs-object-cache-sweep-interval", time.Minute, "interval for node-local rootfs object cache garbage collection")
	flag.StringVar(&haSlot, "ha-slot", os.Getenv("CTLD_HA_SLOT"), "stable ctld HA deployment slot")
	flag.StringVar(&haProbe, "ha-probe", "", "run one ctld HA probe (live or ready) and exit")
	flag.StringVar(&haProbeSocket, "ha-probe-socket", "/run/sandbox0/ctld-ha.sock", "container-local ctld HA probe socket")
	flag.StringVar(&haMetricsAddr, "ha-metrics-addr", "", "dedicated pre-election HTTP listen address for ctld HA metrics; empty disables it")
	flag.StringVar(&networkRuntimeConfigPath, "ctld-networking-config-path", strings.TrimSpace(os.Getenv("CTLD_NETWORK_CONFIG_PATH")), "explicit ctld network runtime config path; empty disables network policy enforcement")
	flag.Parse()

	log.Println("Starting ctld")
	defer func() { log.Println("Stopped ctld") }()
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
	networkFactory, err := configuredNetworkRuntimeFactory(networkRuntimeConfigPath, httpAddr, runtimeWatchAddr)
	if err != nil {
		return fmt.Errorf("validate ctld network runtime config: %w", err)
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
		return runHAPrimary(ctx, coordinator, probeServer.SetServiceReady, networkFactory, runPrimary)
	}
	primaryErrors := make(chan error, 1)
	go func() {
		primaryErrors <- runHAPrimary(ctx, coordinator, probeServer.SetServiceReady, networkFactory, runPrimary)
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
	defer lease.Close()
	return runPrimaryFn(ctx, primaryRunOptions{
		setReady:       setReady,
		networkFactory: networkFactory,
	})
}

type primaryRunOptions struct {
	setReady       func(bool)
	networkFactory primaryServiceFactory
}

func runPrimary(parent context.Context, options primaryRunOptions) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	zapLogger, err := observability.NewLogger(observability.LoggerConfig{
		ServiceName: "ctld",
		Level:       "info",
	})
	if err != nil {
		log.Printf("ctld observability disabled: create zap logger: %v", err)
	}
	var obsProvider *observability.Provider
	if zapLogger != nil {
		defer zapLogger.Sync()
		obsProvider, err = observability.New(observability.ConfigFromEnv("ctld", zapLogger))
		if err != nil {
			log.Printf("ctld observability disabled: %v", err)
			obsProvider = nil
		} else {
			defer obsProvider.Shutdown(ctx)
		}
	}

	var k8sClient kubernetes.Interface
	if client, err := k8s.NewClientWithObservability(kubeconfig, obsProvider); err != nil {
		log.Printf("ctld kubernetes client disabled: %v", err)
	} else {
		k8sClient = client
	}

	ctldCfg := apiconfig.LoadCtldConfig()
	var dbPool *pgxpool.Pool
	if ctldCfg.DatabaseURL != "" {
		dbPool, err = initCtldDatabase(ctx, ctldCfg, obsProvider)
		if err != nil {
			log.Printf("ctld object store request metering disabled: %v", err)
		} else {
			defer dbPool.Close()
		}
	}
	objectStoreRequestMeter := startCtldObjectStoreRequestMetering(
		ctx,
		ctldCfg,
		dbPool,
		nodeName,
		zapLogger,
	)
	defer flushCtldObjectStoreRequestMetering(objectStoreRequestMeter, zapLogger)
	var ctldMetricsRegistry prometheus.Registerer
	if obsProvider != nil {
		ctldMetricsRegistry = obsProvider.MetricsRegistryOrNil()
	}

	serviceErrors := make(chan error, 2)
	networkRequired := options.networkFactory != nil
	var networkHandle *primaryServiceHandle
	serviceHealthy := func() bool {
		return ctx.Err() == nil
	}
	serviceReady := func() bool {
		return serviceHealthy() &&
			(!networkRequired || (networkHandle != nil && networkHandle.Ready()))
	}

	var runtimeWatchServer *ctldruntimewatch.Server
	var runtimeWatchHTTPServer *http.Server
	var runtimeWatchHandler cache.ResourceEventHandler
	if k8sClient != nil {
		runtimeHub := ctldruntimewatch.NewHub(ctldruntimewatch.NewPodStatusSink(k8sClient))
		runtimeWatchServer = ctldruntimewatch.NewServer(runtimeHub)
		runtimeWatchHTTPServer = &http.Server{
			Addr:              runtimeWatchAddr,
			Handler:           runtimeWatchServer,
			ReadHeaderTimeout: 5 * time.Second,
		}
		runtimeWatchHandler = runtimeHub.PodEventHandler()
		go runtimeHub.Run(ctx, 4)
	}
	podCache := buildNodePodCache(ctx, k8sClient, runtimeWatchHandler)
	probeController := buildProbeController(k8sClient, obsProvider, podCache)
	rootFSObserver := ctldrootfs.NewObserver(ctldMetricsRegistry, zapLogger)
	containerdRuntime := buildContainerdRuntime(rootFSObserver)
	defer containerdRuntime.Close()
	runtimeMetricsHandle := startCtldRuntimeMetrics(ctx, ctldCfg, containerdRuntime, podCache, obsProvider, zapLogger)
	httpServer := newHTTPServer(httpAddr, combinedController{
		Controller:  probeController,
		RootFS:      buildRootFSController(ctx, &ctldCfg.RootFSObjectStorage, objectStoreRequestMeter, containerdRuntime, rootFSObserver),
		ReadyCheck:  serviceReady,
		HealthCheck: serviceHealthy,
	})
	if obsProvider != nil {
		httpServer.Handler = httpobs.ServerMiddleware(obsProvider.HTTPServerConfig(zapLogger))(httpServer.Handler)
		httpServer.ConnState = httpobs.NewConnStateTracker(obsProvider.HTTPServerConfig(nil)).Wrap(httpServer.ConnState)
	}
	httpListener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("listen for ctld HTTP server: %w", err)
	}
	var runtimeWatchListener net.Listener
	if runtimeWatchHTTPServer != nil {
		runtimeWatchListener, err = net.Listen("tcp", runtimeWatchAddr)
		if err != nil {
			_ = httpListener.Close()
			return fmt.Errorf("listen for ctld runtime watch server: %w", err)
		}
	}
	if options.networkFactory != nil {
		networkService, err := options.networkFactory()
		if err != nil {
			_ = httpListener.Close()
			if runtimeWatchListener != nil {
				_ = runtimeWatchListener.Close()
			}
			return fmt.Errorf("initialize ctld network runtime: %w", err)
		}
		if networkService == nil {
			_ = httpListener.Close()
			if runtimeWatchListener != nil {
				_ = runtimeWatchListener.Close()
			}
			return fmt.Errorf("initialize ctld network runtime: factory returned a nil service")
		}
		networkHandle = startPrimaryService(ctx, networkService)
		log.Printf("ctld primary started network runtime")
	}
	go func() {
		if err := httpServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
			serviceErrors <- fmt.Errorf("ctld HTTP server: %w", err)
		}
	}()
	if runtimeWatchHTTPServer != nil {
		go func() {
			if err := runtimeWatchHTTPServer.Serve(runtimeWatchListener); err != nil &&
				!errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
				serviceErrors <- fmt.Errorf("ctld runtime watch server: %w", err)
			}
		}()
	}
	if options.setReady != nil {
		options.setReady(serviceReady())
		defer options.setReady(false)
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					options.setReady(serviceReady())
				}
			}
		}()
	}
	var networkErrors <-chan error
	if networkHandle != nil {
		networkErrors = networkHandle.Errors()
	}
	var runErr error
	networkFailed := false
	select {
	case <-parent.Done():
		log.Printf("ctld primary shutting down: %v", parent.Err())
	case runErr = <-serviceErrors:
		log.Printf("ctld primary service failed: %v", runErr)
	case err := <-networkErrors:
		if networkRunErr, failed := networkRuntimeExitError(parent.Err(), err); !failed {
			log.Printf("ctld primary shutting down: %v", parent.Err())
		} else {
			networkFailed = true
			runErr = networkRunErr
			log.Printf("ctld primary service failed: %v", runErr)
		}
	}
	cancel()
	if options.setReady != nil {
		options.setReady(false)
	}
	var networkShutdownCtx context.Context
	var networkShutdownCancel context.CancelFunc
	if networkHandle != nil {
		networkShutdownCtx, networkShutdownCancel = context.WithTimeout(context.Background(), networkRuntimeShutdownTimeout)
		defer networkShutdownCancel()
	}
	httpShutdownCtx, httpShutdownCancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	_ = httpServer.Shutdown(httpShutdownCtx)
	if runtimeWatchHTTPServer != nil {
		_ = runtimeWatchHTTPServer.Shutdown(httpShutdownCtx)
	}
	httpShutdownCancel()
	runtimeMetricsShutdownCtx, runtimeMetricsShutdownCancel := context.WithTimeout(context.Background(), runtimeMetricsShutdownTimeout)
	if err := runtimeMetricsHandle.Shutdown(runtimeMetricsShutdownCtx); err != nil {
		log.Printf("ctld runtime metric producer shutdown completed with errors: %v", err)
	}
	runtimeMetricsShutdownCancel()
	if networkHandle != nil {
		if err := networkHandle.Wait(networkShutdownCtx); err != nil && !networkFailed {
			runErr = errors.Join(runErr, fmt.Errorf("shutdown ctld network runtime: %w", err))
		}
	}
	return runErr
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
	return &http.Server{Addr: addr, Handler: ctldserver.NewMux(controller)}
}

func buildProbeController(k8sClient kubernetes.Interface, obsProvider *observability.Provider, podCache *ctldpower.PodCache) ctldserver.Controller {
	if k8sClient == nil {
		log.Printf("ctld probe control disabled: kubernetes client unavailable")
		return ctldserver.NotImplementedController{}
	}
	resolver := ctldpower.NewPodResolver(k8sClient, nodeName)
	controller := ctldpower.NewController(resolver)
	if obsProvider != nil {
		controller.HTTPClient = obsProvider.HTTP.NewClient(httpobs.Config{Timeout: 2 * time.Second})
	}

	if podCache != nil {
		resolver.SetPodCache(podCache.PodLister(), podCache.PodIndexer())
	}
	return controller
}

func buildNodePodCache(ctx context.Context, k8sClient kubernetes.Interface, handlers ...cache.ResourceEventHandler) *ctldpower.PodCache {
	if k8sClient == nil {
		return nil
	}
	podCache, err := ctldpower.NewNodePodCache(k8sClient, nodeName, 0)
	if err != nil {
		log.Printf("ctld pod cache disabled: %v", err)
		return nil
	}
	for _, handler := range handlers {
		if handler == nil {
			continue
		}
		if err := podCache.AddEventHandler(handler); err != nil {
			log.Printf("ctld pod cache handler disabled: %v", err)
			return nil
		}
	}
	podCache.Start(ctx)
	go func() {
		syncCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if !podCache.WaitForSync(syncCtx) && ctx.Err() == nil {
			log.Printf("ctld pod cache did not sync before timeout; live kubernetes lookups remain enabled")
		}
	}()
	return podCache
}

func buildRootFSController(
	ctx context.Context,
	storageCfg *apiconfig.RootFSObjectStorageConfig,
	requestObserver objectstore.RequestObserver,
	runtime *ctldrootfs.ContainerdRuntime,
	observer *ctldrootfs.Observer,
) rootFSHandler {
	store, cacheEncryption, err := buildRootFSObjectStore(storageCfg, requestObserver)
	if err != nil {
		log.Printf("ctld rootfs object store disabled: %v", err)
	}
	objectCache := buildRootFSObjectCache(ctx, observer, cacheEncryption)
	return ctldrootfs.NewController(ctldrootfs.Config{
		Runtime:     runtime,
		Store:       store,
		SnapshotDir: filepath.Join(stateRoot, "rootfs", "prepared"),
		ObjectCache: objectCache,
		Observer:    observer,
	})
}

func buildContainerdRuntime(observer *ctldrootfs.Observer) *ctldrootfs.ContainerdRuntime {
	return ctldrootfs.NewContainerdRuntime(ctldrootfs.ContainerdRuntimeConfig{
		CRIEndpoint:            criEndpoint,
		ContainerdEndpoint:     containerdEndpoint,
		ContainerdRoot:         containerdRoot,
		ContainerdHostRoot:     containerdHostRoot,
		ContainerdDataRoot:     containerdDataRoot,
		ContainerdHostDataRoot: containerdHostDataRoot,
		RootFSCacheDir:         filepath.Join(stateRoot, "rootfs"),
		Namespace:              containerdNamespace,
		Observer:               observer,
	})
}

func buildRootFSObjectCache(ctx context.Context, observer *ctldrootfs.Observer, encryption objectstore.EncryptionConfig) *ctldrootfs.ObjectCache {
	maxBytes, err := parseByteQuantity(rootFSObjectCacheMaxBytes)
	if err != nil {
		log.Printf("ctld rootfs object cache disabled: %v", err)
		return nil
	}
	minFreeBytes, err := parseByteQuantity(rootFSObjectCacheMinFreeBytes)
	if err != nil {
		log.Printf("ctld rootfs object cache disabled: %v", err)
		return nil
	}
	cache := ctldrootfs.NewObjectCache(ctldrootfs.ObjectCacheConfig{
		Dir:           filepath.Join(stateRoot, "rootfs", "objects"),
		MaxBytes:      maxBytes,
		MinFreeBytes:  minFreeBytes,
		MaxAge:        rootFSObjectCacheMaxAge,
		SweepInterval: rootFSObjectCacheSweepInterval,
		Observer:      observer,
		Encryption:    encryption,
	})
	if cache != nil {
		cache.Start(ctx)
		log.Printf("ctld rootfs object cache enabled: max_bytes=%d min_free_bytes=%d max_age=%s sweep_interval=%s", maxBytes, minFreeBytes, rootFSObjectCacheMaxAge, rootFSObjectCacheSweepInterval)
	}
	return cache
}

func parseByteQuantity(raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	switch strings.ToLower(value) {
	case "", "0", "off", "disabled", "false":
		return 0, nil
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, fmt.Errorf("parse %q as byte quantity: %w", raw, err)
	}
	bytes := quantity.Value()
	if bytes < 0 {
		return 0, fmt.Errorf("byte quantity must be non-negative: %q", raw)
	}
	return bytes, nil
}

func buildRootFSObjectStore(cfg *apiconfig.RootFSObjectStorageConfig, requestObserver objectstore.RequestObserver) (objectstore.Store, objectstore.EncryptionConfig, error) {
	if cfg == nil {
		return nil, objectstore.EncryptionConfig{}, fmt.Errorf("storage config is not configured")
	}
	store, err := objectstore.Create(objectstore.Config{
		Type:            cfg.Type,
		Bucket:          cfg.Bucket,
		Region:          cfg.Region,
		Endpoint:        cfg.Endpoint,
		AccessKey:       cfg.AccessKey,
		SecretKey:       cfg.SecretKey,
		SessionToken:    cfg.SessionToken,
		RequestObserver: requestObserver,
	})
	if err != nil {
		return nil, objectstore.EncryptionConfig{}, err
	}
	encryption := objectstore.EncryptionConfig{}
	if cfg.ObjectEncryptionEnabled {
		keyPEM, err := objectstore.LoadEncryptionKey(cfg.ObjectEncryptionKeyPath)
		if err != nil {
			return nil, objectstore.EncryptionConfig{}, err
		}
		keyEncryptor, err := objectstore.NewKeyEncryptor(keyPEM, cfg.ObjectEncryptionPassphrase)
		if err != nil {
			return nil, objectstore.EncryptionConfig{}, err
		}
		encryption = objectstore.EncryptionConfig{
			Enabled:      true,
			Algorithm:    cfg.ObjectEncryptionAlgo,
			KeyEncryptor: keyEncryptor,
		}
		store = objectstore.Encrypting(store, encryption)
	}
	return store, encryption, nil
}

func initCtldDatabase(ctx context.Context, cfg *apiconfig.CtldConfig, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	if cfg == nil || cfg.DatabaseURL == "" {
		return nil, nil
	}
	var modifier func(*pgxpool.Config) error
	if obsProvider != nil {
		modifier = obsProvider.Pgx.ConfigModifier()
	}
	return dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     cfg.DatabaseURL,
		MaxConns:        int32(cfg.DatabaseMaxConns),
		MinConns:        int32(cfg.DatabaseMinConns),
		DefaultMaxConns: 5,
		DefaultMinConns: 1,
		ConfigModifier:  modifier,
	})
}

type combinedController struct {
	ctldserver.Controller
	RootFS      rootFSHandler
	ReadyCheck  func() bool
	HealthCheck func() bool
}

func (c combinedController) Ready() bool {
	return c.ReadyCheck == nil || c.ReadyCheck()
}

func (c combinedController) Healthy() bool {
	return c.HealthCheck == nil || c.HealthCheck()
}

func (c combinedController) Probe(r *http.Request, sandboxID string, kind sandboxprobe.Kind) (sandboxprobe.Response, int) {
	return c.Controller.Probe(r, sandboxID, kind)
}

func (c combinedController) InspectRootFS(r *http.Request, req ctldapi.InspectRootFSRequest) (ctldapi.InspectRootFSResponse, int) {
	if c.RootFS == nil {
		return ctldapi.InspectRootFSResponse{Error: "ctld rootfs inspect not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.InspectRootFS(r, req)
}

func (c combinedController) SaveRootFS(r *http.Request, req ctldapi.SaveRootFSRequest) (ctldapi.SaveRootFSResponse, int) {
	if c.RootFS == nil {
		return ctldapi.SaveRootFSResponse{Error: "ctld rootfs save not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.SaveRootFS(r, req)
}

func (c combinedController) PrepareRootFSSnapshot(r *http.Request, req ctldapi.PrepareRootFSSnapshotRequest) (ctldapi.PrepareRootFSSnapshotResponse, int) {
	if c.RootFS == nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Error: "ctld rootfs snapshot prepare not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.PrepareRootFSSnapshot(r, req)
}

func (c combinedController) PublishRootFSSnapshot(r *http.Request, req ctldapi.PublishRootFSSnapshotRequest) (ctldapi.PublishRootFSSnapshotResponse, int) {
	if c.RootFS == nil {
		return ctldapi.PublishRootFSSnapshotResponse{Error: "ctld rootfs snapshot publish not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.PublishRootFSSnapshot(r, req)
}

func (c combinedController) AbortRootFSSnapshot(r *http.Request, req ctldapi.AbortRootFSSnapshotRequest) (ctldapi.AbortRootFSSnapshotResponse, int) {
	if c.RootFS == nil {
		return ctldapi.AbortRootFSSnapshotResponse{Error: "ctld rootfs snapshot abort not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.AbortRootFSSnapshot(r, req)
}

func (c combinedController) ApplyRootFS(r *http.Request, req ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int) {
	if c.RootFS == nil {
		return ctldapi.ApplyRootFSResponse{Error: "ctld rootfs apply not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.ApplyRootFS(r, req)
}

type rootFSHandler interface {
	ctldserver.RootFSController
	ctldserver.RootFSSnapshotController
}
