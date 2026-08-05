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
	ctldregistration "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/kubeletregistration"
	ctldportal "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
	ctldpower "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/power"
	ctldrootfs "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfs"
	ctldrootfsstore "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	ctldruntimewatch "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/runtimewatch"
	ctldserver "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/server"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/fuseportal"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/k8s"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfslease"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	storagedb "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	storagevolume "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

var (
	httpAddr                    = ":8095"
	runtimeWatchAddr            = fmt.Sprintf(":%d", runtimecontrol.DefaultCtldWatchPort)
	kubeconfig                  = ""
	criEndpoint                 = "/host-run/containerd/containerd.sock"
	containerdEndpoint          = "/host-run/containerd/containerd.sock"
	containerdDataRoot          = "/host-var-lib/containerd"
	containerdHostDataRoot      = "/var/lib/containerd"
	containerdNamespace         = "k8s.io"
	nodeName                    = os.Getenv("NODE_NAME")
	portalRoot                  = "/var/lib/sandbox0/ctld"
	kubeletPodsRoot             = "/var/lib/kubelet/pods"
	csiSocket                   = "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock"
	kubeletRegistrationSocket   string
	kubeletRegistrationEndpoint string
	podName                     = os.Getenv("POD_NAME")
	podNamespace                = os.Getenv("POD_NAMESPACE")
	haSlot                      = os.Getenv("CTLD_HA_SLOT")
	haProbe                     string
	haProbeSocket               = "/run/sandbox0/ctld-ha.sock"
	haMetricsAddr               string
	networkRuntimeConfigPath    = strings.TrimSpace(os.Getenv("NETD_CONFIG_PATH"))
)

const (
	httpShutdownTimeout           = 5 * time.Second
	runtimeMetricsShutdownTimeout = 7 * time.Second
	portalShutdownTimeout         = 25 * time.Second
	shutdownGraceMargin           = 5 * time.Second
	minimumTerminationGrace       = httpShutdownTimeout + runtimeMetricsShutdownTimeout + portalShutdownTimeout + shutdownGraceMargin
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
	flag.StringVar(&containerdDataRoot, "containerd-data-root", "/host-var-lib/containerd", "host containerd data root mounted into ctld")
	flag.StringVar(&containerdHostDataRoot, "containerd-host-data-root", "/var/lib/containerd", "host containerd data root path returned by containerd snapshotters")
	flag.StringVar(&containerdNamespace, "containerd-namespace", "k8s.io", "containerd namespace used by Kubernetes")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "current node name used to validate local sandbox ownership")
	flag.StringVar(&portalRoot, "volume-portal-root", "/var/lib/sandbox0/ctld", "host-local root for ctld volume portal WAL and cache")
	flag.StringVar(&kubeletPodsRoot, "kubelet-pods-root", "/var/lib/kubelet/pods", "host kubelet pod directory used to recover stale sandbox0 CSI mounts")
	flag.StringVar(&csiSocket, "csi-socket", "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock", "CSI endpoint socket for sandbox volume portals")
	flag.StringVar(&kubeletRegistrationSocket, "kubelet-registration-socket", "", "kubelet plugin-registration socket; empty disables embedded registration")
	flag.StringVar(&kubeletRegistrationEndpoint, "kubelet-registration-endpoint", "", "host-visible CSI endpoint returned to kubelet; defaults to csi-socket")
	flag.StringVar(&haSlot, "ha-slot", os.Getenv("CTLD_HA_SLOT"), "stable ctld HA deployment slot")
	flag.StringVar(&haProbe, "ha-probe", "", "run one ctld HA probe (live or ready) and exit")
	flag.StringVar(&haProbeSocket, "ha-probe-socket", "/run/sandbox0/ctld-ha.sock", "container-local ctld HA probe socket")
	flag.StringVar(&haMetricsAddr, "ha-metrics-addr", "", "dedicated pre-election HTTP listen address for ctld HA metrics; empty disables it")
	flag.StringVar(&networkRuntimeConfigPath, "netd-config-path", strings.TrimSpace(os.Getenv("NETD_CONFIG_PATH")), "explicit ctld network runtime config path; empty disables network policy enforcement")
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
	coordinator, err := ctldha.NewCoordinator(ctldha.Config{RootDir: portalRoot, Slot: haSlot})
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
	ownerID := ""
	if strings.TrimSpace(nodeName) != "" {
		ownerID = "ctld-node/" + strings.TrimSpace(nodeName)
	}
	return runPrimaryFn(ctx, primaryRunOptions{
		replicator:     lease.Replicator,
		ownerID:        ownerID,
		recovery:       lease.Recovery,
		setReady:       setReady,
		networkFactory: networkFactory,
	})
}

type primaryRunOptions struct {
	replicator     *ctldha.Replicator
	ownerID        string
	recovery       []ctldha.RecoveredPortal
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
	storageCfg := &ctldCfg.StorageProxyConfig
	var repo *storagedb.Repository
	var dbPool *pgxpool.Pool
	if storageCfg.DatabaseURL != "" {
		dbPool, err = initPortalDatabase(ctx, storageCfg, obsProvider)
		if err != nil {
			log.Printf("ctld volume registry disabled: %v", err)
		} else {
			repo = storagedb.NewRepository(dbPool)
			defer dbPool.Close()
		}
	}
	objectStoreRequestMeter := startCtldObjectStoreRequestMetering(
		ctx,
		storageCfg,
		dbPool,
		nodeName,
		zapLogger,
	)
	defer flushCtldObjectStoreRequestMetering(objectStoreRequestMeter, zapLogger)
	var ctldMetricsRegistry prometheus.Registerer
	if obsProvider != nil {
		ctldMetricsRegistry = obsProvider.MetricsRegistryOrNil()
	}

	podUIDLister := activePodUIDLister(k8sClient, nodeName)
	portalObserver := ctldportal.NewObserver(ctldMetricsRegistry, zapLogger)
	portalManager := ctldportal.NewManager(ctldportal.Config{
		NodeName:           nodeName,
		RootDir:            portalRoot,
		KubeletPodsRoot:    kubeletPodsRoot,
		Logger:             zapLogger,
		StorageConfig:      storageCfg,
		StorageObserver:    newPortalStorageObserver(storageCfg, repo, dbPool),
		RequestObserver:    objectStoreRequestMeter,
		Observer:           portalObserver,
		Repository:         repo,
		PodName:            podName,
		PodNamespace:       podNamespace,
		OwnerID:            options.ownerID,
		ActivePodUIDLister: podUIDLister,
		Replicator:         options.replicator,
		RequireStandby:     true,
	})
	for i := range options.recovery {
		recovered := &options.recovery[i]
		for {
			err := portalManager.RestorePortal(ctx, recovered.Manifest, recovered.Channel)
			if err == nil {
				recovered.Channel = nil
				break
			}
			log.Printf("ctld portal %q recovery failed; retrying: %v", recovered.Manifest.Key, err)
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
		}
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 30*time.Second)
	if err := portalManager.CleanupStalePortals(cleanupCtx); err != nil && cleanupCtx.Err() == nil {
		log.Printf("ctld stale portal cleanup completed with errors: %v", err)
	}
	if err := portalManager.CleanupStaleCSIMounts(cleanupCtx); err != nil && cleanupCtx.Err() == nil {
		log.Printf("ctld stale CSI mount cleanup completed with errors: %v", err)
	}
	cleanupCancel()
	if options.replicator != nil {
		options.replicator.SetSnapshotProvider(func(ctx context.Context, target ctldportal.PortalReplicator) error {
			return portalManager.SyncTo(ctx, target)
		})
	}
	go portalManager.Run(ctx)
	csiServer := ctldportal.NewCSIServer(nodeName, portalManager)
	csiErrors, err := csiServer.Start(csiSocket)
	if err != nil {
		return fmt.Errorf("start ctld volume portal CSI server: %w", err)
	}
	defer csiServer.Stop()

	var registrationServer *ctldregistration.Server
	var registrationErrors <-chan error
	if strings.TrimSpace(kubeletRegistrationSocket) != "" {
		registrationEndpoint := strings.TrimSpace(kubeletRegistrationEndpoint)
		if registrationEndpoint == "" {
			registrationEndpoint = csiSocket
		}
		registrationServer, err = ctldregistration.NewServer(ctldregistration.Config{
			SocketPath: kubeletRegistrationSocket,
			DriverName: volumeportal.DriverName,
			Endpoint:   registrationEndpoint,
		})
		if err != nil {
			return err
		}
		if err := registrationServer.Start(); err != nil {
			return fmt.Errorf("start kubelet CSI registration server: %w", err)
		}
		log.Printf("ctld primary kubelet registration server listening on %q for CSI endpoint %q", kubeletRegistrationSocket, registrationEndpoint)
		defer registrationServer.Stop()
		registrationErrors = registrationServer.Errors()
	}
	serviceErrors := make(chan error, 2)
	networkRequired := options.networkFactory != nil
	var networkHandle *primaryServiceHandle
	multiplexerHealth := fuseportal.SharedMultiplexerHealth
	if err := multiplexerHealth(); err != nil {
		return fmt.Errorf("initialize shared FUSE multiplexer health: %w", err)
	}
	serviceHealthy := func() bool {
		return ctx.Err() == nil && multiplexerHealth() == nil
	}
	serviceReady := func() bool {
		return serviceHealthy() && portalManager.RecoveryError() == nil &&
			(registrationServer == nil || registrationServer.Registered()) &&
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
	containerdRuntime := buildContainerdRuntime()
	defer containerdRuntime.Close()
	runtimeMetricsHandle := startCtldRuntimeMetrics(ctx, ctldCfg, containerdRuntime, podCache, obsProvider, zapLogger)
	httpServer := newHTTPServer(httpAddr, combinedController{
		Controller:  probeController,
		Portal:      portalManager,
		RootFS:      buildRootFSController(ctx, storageCfg, objectStoreRequestMeter, containerdRuntime, dbPool, ctldMetricsRegistry),
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
	go monitorPrimaryServiceHealth(ctx, time.Second, multiplexerHealth, serviceErrors)

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
	case err := <-csiErrors:
		runErr = fmt.Errorf("ctld volume portal CSI server: %w", err)
		log.Printf("ctld primary service failed: %v", runErr)
	case err := <-registrationErrors:
		runErr = fmt.Errorf("ctld kubelet CSI registration server: %w", err)
		log.Printf("ctld primary service failed: %v", runErr)
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
	if registrationServer != nil {
		registrationServer.Stop()
	}
	csiServer.Stop()
	portalShutdownCtx, portalShutdownCancel := context.WithTimeout(context.Background(), portalShutdownTimeout)
	if err := portalManager.Shutdown(portalShutdownCtx); err != nil {
		log.Printf("ctld volume portal shutdown completed with errors: %v", err)
	}
	portalShutdownCancel()
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

func activePodUIDLister(k8sClient kubernetes.Interface, nodeName string) ctldportal.ActivePodUIDLister {
	nodeName = strings.TrimSpace(nodeName)
	if k8sClient == nil || nodeName == "" {
		return nil
	}
	return func(ctx context.Context) (map[string]struct{}, error) {
		pods, err := k8sClient.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
			FieldSelector: fields.OneTermEqualSelector("spec.nodeName", nodeName).String(),
		})
		if err != nil {
			return nil, err
		}
		active := make(map[string]struct{}, len(pods.Items))
		for i := range pods.Items {
			pod := &pods.Items[i]
			if podTerminalForMountCleanup(pod) || pod.UID == "" {
				continue
			}
			active[string(pod.UID)] = struct{}{}
		}
		return active, nil
	}
}

func podTerminalForMountCleanup(pod *corev1.Pod) bool {
	if pod == nil {
		return true
	}
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

func buildRootFSController(
	ctx context.Context,
	storageCfg *apiconfig.StorageProxyConfig,
	requestObserver objectstore.RequestObserver,
	runtime *ctldrootfs.ContainerdRuntime,
	dbPool *pgxpool.Pool,
	metricsRegistry prometheus.Registerer,
) rootFSHandler {
	store, err := buildRootFSObjectStore(storageCfg, requestObserver)
	if err != nil {
		log.Printf("ctld rootfs object store disabled: %v", err)
	}
	return ctldrootfs.NewController(ctldrootfs.Config{
		Context:         ctx,
		Runtime:         runtime,
		Store:           store,
		WatchFenceRoot:  filepath.Join(portalRoot, "rootfs-watch-fences", strings.TrimSpace(haSlot)),
		CaptureLeases:   rootfslease.NewRepository(dbPool),
		MetricsRegistry: metricsRegistry,
	})
}

func buildContainerdRuntime() *ctldrootfs.ContainerdRuntime {
	return ctldrootfs.NewContainerdRuntime(ctldrootfs.ContainerdRuntimeConfig{
		CRIEndpoint:            criEndpoint,
		ContainerdEndpoint:     containerdEndpoint,
		ContainerdDataRoot:     containerdDataRoot,
		ContainerdHostDataRoot: containerdHostDataRoot,
		Namespace:              containerdNamespace,
	})
}

func buildRootFSObjectStore(cfg *apiconfig.StorageProxyConfig, requestObserver objectstore.RequestObserver) (objectstore.Store, error) {
	return ctldrootfsstore.NewObjectStore(cfg, requestObserver)
}

func initPortalDatabase(ctx context.Context, cfg *apiconfig.StorageProxyConfig, obsProvider *observability.Provider) (*pgxpool.Pool, error) {
	if cfg == nil || cfg.DatabaseURL == "" {
		return nil, nil
	}
	schema := cfg.DatabaseSchema
	if schema == "" {
		schema = "storage_proxy"
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
		Schema:          schema,
		ConfigModifier:  modifier,
	})
}

func newPortalStorageObserver(
	cfg *apiconfig.StorageProxyConfig,
	repo *storagedb.Repository,
	pool *pgxpool.Pool,
) storagevolume.StorageObserver {
	if cfg == nil || !cfg.Metering.Enabled || repo == nil || pool == nil {
		return nil
	}
	return storagevolume.NewVolumeStorageObserverWithRecorder(
		repo,
		meteringoutbox.NewRepository(pool),
		cfg.RegionID,
		naming.ClusterIDOrDefault(&cfg.DefaultClusterId),
	)
}

type combinedController struct {
	ctldserver.Controller
	Portal      volumePortalHandler
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

func monitorPrimaryServiceHealth(ctx context.Context, interval time.Duration, check func() error, failures chan<- error) {
	if check == nil || failures == nil {
		return
	}
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := check(); err != nil {
			select {
			case failures <- fmt.Errorf("ctld primary health: %w", err):
			case <-ctx.Done():
			}
			return
		}
	}
}

func (c combinedController) BindVolumePortal(r *http.Request, req ctldapi.BindVolumePortalRequest) (ctldapi.BindVolumePortalResponse, int) {
	if c.Portal == nil {
		return ctldapi.BindVolumePortalResponse{Error: "ctld volume portals not implemented"}, http.StatusNotImplemented
	}
	resp, err := c.Portal.Bind(r.Context(), req)
	if err != nil {
		return ctldapi.BindVolumePortalResponse{Error: err.Error()}, volumePortalErrorStatus(err)
	}
	return resp, http.StatusOK
}

func (c combinedController) UnbindVolumePortal(r *http.Request, req ctldapi.UnbindVolumePortalRequest) (ctldapi.UnbindVolumePortalResponse, int) {
	if c.Portal == nil {
		return ctldapi.UnbindVolumePortalResponse{Error: "ctld volume portals not implemented"}, http.StatusNotImplemented
	}
	resp, err := c.Portal.Unbind(r.Context(), req)
	if err != nil {
		return ctldapi.UnbindVolumePortalResponse{Error: err.Error()}, volumePortalErrorStatus(err)
	}
	return resp, http.StatusOK
}

func (c combinedController) CheckVolumePortals(r *http.Request, req ctldapi.CheckVolumePortalsRequest) (ctldapi.CheckVolumePortalsResponse, int) {
	if c.Portal == nil {
		return ctldapi.CheckVolumePortalsResponse{Error: "ctld volume portals not implemented"}, http.StatusNotImplemented
	}
	resp, err := c.Portal.CheckPublished(r.Context(), req)
	if err != nil {
		return ctldapi.CheckVolumePortalsResponse{Error: err.Error()}, volumePortalErrorStatus(err)
	}
	return resp, http.StatusOK
}

func (c combinedController) AttachVolumeOwner(r *http.Request, req ctldapi.AttachVolumeOwnerRequest) (ctldapi.AttachVolumeOwnerResponse, int) {
	if c.Portal == nil {
		return ctldapi.AttachVolumeOwnerResponse{Error: "ctld volume owners not implemented"}, http.StatusNotImplemented
	}
	resp, err := c.Portal.AttachOwner(r.Context(), req)
	if err != nil {
		return ctldapi.AttachVolumeOwnerResponse{Error: err.Error()}, volumePortalErrorStatus(err)
	}
	return resp, http.StatusOK
}

func (c combinedController) ReleaseVolumeOwner(r *http.Request, req ctldapi.ReleaseVolumeOwnerRequest) (ctldapi.ReleaseVolumeOwnerResponse, int) {
	if c.Portal == nil {
		return ctldapi.ReleaseVolumeOwnerResponse{Error: "ctld volume owners not implemented"}, http.StatusNotImplemented
	}
	resp, err := c.Portal.ReleaseOwner(r.Context(), req)
	if err != nil {
		resp.Error = err.Error()
		return resp, volumePortalErrorStatus(err)
	}
	return resp, http.StatusOK
}

func (c combinedController) PrepareVolumeSnapshotCheckpoint(r *http.Request, req ctldapi.PrepareVolumeSnapshotCheckpointRequest) (ctldapi.PrepareVolumeSnapshotCheckpointResponse, int) {
	if c.Portal == nil {
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{Error: "ctld volume snapshot checkpoint not implemented"}, http.StatusNotImplemented
	}
	resp, err := c.Portal.PrepareSnapshotCheckpoint(r.Context(), req)
	if err != nil {
		return ctldapi.PrepareVolumeSnapshotCheckpointResponse{Error: err.Error()}, volumePortalErrorStatus(err)
	}
	return resp, http.StatusOK
}

func (c combinedController) CompleteVolumeSnapshotCheckpoint(r *http.Request, req ctldapi.CompleteVolumeSnapshotCheckpointRequest) (ctldapi.CompleteVolumeSnapshotCheckpointResponse, int) {
	if c.Portal == nil {
		return ctldapi.CompleteVolumeSnapshotCheckpointResponse{Error: "ctld volume snapshot checkpoint not implemented"}, http.StatusNotImplemented
	}
	resp, err := c.Portal.CompleteSnapshotCheckpoint(r.Context(), req)
	if err != nil {
		return ctldapi.CompleteVolumeSnapshotCheckpointResponse{Error: err.Error()}, volumePortalErrorStatus(err)
	}
	return resp, http.StatusOK
}

func (c combinedController) AbortVolumeSnapshotCheckpoint(r *http.Request, req ctldapi.AbortVolumeSnapshotCheckpointRequest) (ctldapi.AbortVolumeSnapshotCheckpointResponse, int) {
	if c.Portal == nil {
		return ctldapi.AbortVolumeSnapshotCheckpointResponse{Error: "ctld volume snapshot checkpoint not implemented"}, http.StatusNotImplemented
	}
	resp, err := c.Portal.AbortSnapshotCheckpoint(r.Context(), req)
	if err != nil {
		return ctldapi.AbortVolumeSnapshotCheckpointResponse{Error: err.Error()}, volumePortalErrorStatus(err)
	}
	return resp, http.StatusOK
}

func volumePortalErrorStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(message, "already has an active owner"),
		strings.Contains(message, "actively bound to a portal"),
		strings.Contains(message, "still has active file requests"),
		strings.Contains(message, "already bound to"),
		strings.Contains(message, "snapshot checkpoint already in progress"):
		return http.StatusConflict
	default:
		return http.StatusBadRequest
	}
}

func (c combinedController) MountedVolumeHandler() http.Handler {
	if c.Portal == nil {
		return nil
	}
	return c.Portal.MountedVolumeHandler()
}

func (c combinedController) Probe(r *http.Request, sandboxID string, kind sandboxprobe.Kind) (sandboxprobe.Response, int) {
	return c.Controller.Probe(r, sandboxID, kind)
}

func (c combinedController) BindRootFSSync(r *http.Request, req ctldapi.BindRootFSSyncRequest) (ctldapi.BindRootFSSyncResponse, int) {
	if c.RootFS == nil {
		return ctldapi.BindRootFSSyncResponse{Error: "ctld rootfs sync not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.BindRootFSSync(r, req)
}

func (c combinedController) GetRootFSSyncStatus(r *http.Request, req ctldapi.GetRootFSSyncStatusRequest) (ctldapi.GetRootFSSyncStatusResponse, int) {
	if c.RootFS == nil {
		return ctldapi.GetRootFSSyncStatusResponse{Error: "ctld rootfs sync not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.GetRootFSSyncStatus(r, req)
}

func (c combinedController) SealRootFSHead(r *http.Request, req ctldapi.SealRootFSHeadRequest) (ctldapi.SealRootFSHeadResponse, int) {
	if c.RootFS == nil {
		return ctldapi.SealRootFSHeadResponse{Error: "ctld rootfs sync not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.SealRootFSHead(r, req)
}

func (c combinedController) AcknowledgeRootFSHead(r *http.Request, req ctldapi.AcknowledgeRootFSHeadRequest) (ctldapi.AcknowledgeRootFSHeadResponse, int) {
	if c.RootFS == nil {
		return ctldapi.AcknowledgeRootFSHeadResponse{Error: "ctld rootfs sync not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.AcknowledgeRootFSHead(r, req)
}

func (c combinedController) MaterializeRootFSHead(r *http.Request, req ctldapi.MaterializeRootFSHeadRequest) (ctldapi.MaterializeRootFSHeadResponse, int) {
	if c.RootFS == nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: "ctld rootfs sync not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.MaterializeRootFSHead(r, req)
}

type rootFSHandler interface {
	ctldserver.RootFSSyncController
}

type volumePortalHandler interface {
	Bind(ctx context.Context, req ctldapi.BindVolumePortalRequest) (ctldapi.BindVolumePortalResponse, error)
	Unbind(ctx context.Context, req ctldapi.UnbindVolumePortalRequest) (ctldapi.UnbindVolumePortalResponse, error)
	CheckPublished(ctx context.Context, req ctldapi.CheckVolumePortalsRequest) (ctldapi.CheckVolumePortalsResponse, error)
	AttachOwner(ctx context.Context, req ctldapi.AttachVolumeOwnerRequest) (ctldapi.AttachVolumeOwnerResponse, error)
	ReleaseOwner(ctx context.Context, req ctldapi.ReleaseVolumeOwnerRequest) (ctldapi.ReleaseVolumeOwnerResponse, error)
	PrepareSnapshotCheckpoint(ctx context.Context, req ctldapi.PrepareVolumeSnapshotCheckpointRequest) (ctldapi.PrepareVolumeSnapshotCheckpointResponse, error)
	CompleteSnapshotCheckpoint(ctx context.Context, req ctldapi.CompleteVolumeSnapshotCheckpointRequest) (ctldapi.CompleteVolumeSnapshotCheckpointResponse, error)
	AbortSnapshotCheckpoint(ctx context.Context, req ctldapi.AbortVolumeSnapshotCheckpointRequest) (ctldapi.AbortVolumeSnapshotCheckpointResponse, error)
	MountedVolumeHandler() http.Handler
}
