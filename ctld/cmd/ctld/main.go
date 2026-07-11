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
	ctldha "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/ha"
	ctldportal "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
	ctldpower "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/power"
	ctldrootfs "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfs"
	ctldserver "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/server"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/k8s"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	storagedb "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
)

var (
	httpAddr                       = ":8095"
	kubeconfig                     = ""
	criEndpoint                    = "/host-run/containerd/containerd.sock"
	containerdEndpoint             = "/host-run/containerd/containerd.sock"
	containerdRoot                 = "/host-run/containerd"
	containerdHostRoot             = "/run/containerd"
	containerdDataRoot             = "/host-var-lib/containerd"
	containerdHostDataRoot         = "/var/lib/containerd"
	containerdNamespace            = "k8s.io"
	nodeName                       = os.Getenv("NODE_NAME")
	portalRoot                     = "/var/lib/sandbox0/ctld"
	kubeletPodsRoot                = "/var/lib/kubelet/pods"
	csiSocket                      = "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock"
	rootFSObjectCacheMaxBytes      = "20Gi"
	rootFSObjectCacheMinFreeBytes  = "0"
	rootFSObjectCacheMaxAge        time.Duration
	rootFSObjectCacheSweepInterval = time.Minute
	podName                        = os.Getenv("POD_NAME")
	podNamespace                   = os.Getenv("POD_NAMESPACE")
	haEnabled                      bool
	haSlot                         = os.Getenv("CTLD_HA_SLOT")
	haProbe                        string
	haProbeSocket                  = "/run/sandbox0/ctld-ha.sock"
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
	flag.StringVar(&kubeconfig, "kubeconfig", "", "optional kubeconfig path used by ctld")
	flag.StringVar(&criEndpoint, "cri-endpoint", "/host-run/containerd/containerd.sock", "host CRI socket used to read pod sandbox stats")
	flag.StringVar(&containerdEndpoint, "containerd-endpoint", "/host-run/containerd/containerd.sock", "host containerd socket used for rootfs diff/apply")
	flag.StringVar(&containerdRoot, "containerd-root", "/host-run/containerd", "host containerd runtime root mounted into ctld")
	flag.StringVar(&containerdHostRoot, "containerd-host-root", "/run/containerd", "host containerd runtime root path used in containerd mount requests")
	flag.StringVar(&containerdDataRoot, "containerd-data-root", "/host-var-lib/containerd", "host containerd data root mounted into ctld")
	flag.StringVar(&containerdHostDataRoot, "containerd-host-data-root", "/var/lib/containerd", "host containerd data root path returned by containerd snapshotters")
	flag.StringVar(&containerdNamespace, "containerd-namespace", "k8s.io", "containerd namespace used by Kubernetes")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "current node name used to validate local sandbox ownership")
	flag.StringVar(&portalRoot, "volume-portal-root", "/var/lib/sandbox0/ctld", "host-local root for ctld volume portal WAL and cache")
	flag.StringVar(&kubeletPodsRoot, "kubelet-pods-root", "/var/lib/kubelet/pods", "host kubelet pod directory used to recover stale sandbox0 CSI mounts")
	flag.StringVar(&csiSocket, "csi-socket", "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock", "CSI endpoint socket for sandbox volume portals")
	flag.StringVar(&rootFSObjectCacheMaxBytes, "rootfs-object-cache-max-bytes", "20Gi", "maximum node-local rootfs object cache size; set to 0 to disable")
	flag.StringVar(&rootFSObjectCacheMinFreeBytes, "rootfs-object-cache-min-free-bytes", "0", "minimum free bytes to preserve on the rootfs object cache filesystem")
	flag.DurationVar(&rootFSObjectCacheMaxAge, "rootfs-object-cache-max-age", 0, "maximum age for node-local rootfs cache objects; 0 disables age-based eviction")
	flag.DurationVar(&rootFSObjectCacheSweepInterval, "rootfs-object-cache-sweep-interval", time.Minute, "interval for node-local rootfs object cache garbage collection")
	flag.BoolVar(&haEnabled, "ha-enabled", false, "enable node-local primary/standby ctld coordination")
	flag.StringVar(&haSlot, "ha-slot", os.Getenv("CTLD_HA_SLOT"), "stable ctld HA deployment slot")
	flag.StringVar(&haProbe, "ha-probe", "", "run one ctld HA probe (live or ready) and exit")
	flag.StringVar(&haProbeSocket, "ha-probe-socket", "/run/sandbox0/ctld-ha.sock", "container-local ctld HA probe socket")
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
	if !haEnabled {
		return runPrimary(ctx, primaryRunOptions{})
	}
	coordinator, err := ctldha.NewCoordinator(ctldha.Config{RootDir: portalRoot, Slot: haSlot})
	if err != nil {
		return err
	}
	probeServer, err := ctldha.StartProbeServer(ctx, haProbeSocket, coordinator)
	if err != nil {
		return err
	}
	defer probeServer.Close()
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
	return runPrimary(ctx, primaryRunOptions{
		replicator:     lease.Replicator,
		requireStandby: true,
		ownerID:        ownerID,
		recovery:       lease.Recovery,
		setReady:       probeServer.SetServiceReady,
	})
}

type primaryRunOptions struct {
	replicator     *ctldha.Replicator
	requireStandby bool
	ownerID        string
	recovery       []ctldha.RecoveredPortal
	setReady       func(bool)
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

	podUIDLister := activePodUIDLister(k8sClient, nodeName)
	portalManager := ctldportal.NewManager(ctldportal.Config{
		NodeName:           nodeName,
		RootDir:            portalRoot,
		KubeletPodsRoot:    kubeletPodsRoot,
		Logger:             zapLogger,
		StorageConfig:      storageCfg,
		Repository:         repo,
		PodName:            podName,
		PodNamespace:       podNamespace,
		OwnerID:            options.ownerID,
		ActivePodUIDLister: podUIDLister,
		Replicator:         options.replicator,
		RequireStandby:     options.requireStandby,
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
	if options.replicator != nil {
		options.replicator.SetSnapshotProvider(func(ctx context.Context, target ctldportal.PortalReplicator) error {
			return portalManager.SyncTo(ctx, target)
		})
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 30*time.Second)
	if err := portalManager.CleanupStaleCSIMounts(cleanupCtx); err != nil && cleanupCtx.Err() == nil {
		log.Printf("ctld stale CSI mount cleanup completed with errors: %v", err)
	}
	cleanupCancel()
	go portalManager.Run(ctx)
	csiServer := ctldportal.NewCSIServer(nodeName, portalManager)
	serviceErrors := make(chan error, 2)
	go func() {
		if err := csiServer.Serve(csiSocket); err != nil && ctx.Err() == nil {
			serviceErrors <- fmt.Errorf("ctld volume portal CSI server: %w", err)
		}
	}()
	defer csiServer.Stop()

	podCache := buildNodePodCache(ctx, k8sClient)
	probeController := buildProbeController(k8sClient, obsProvider, podCache)
	containerdRuntime := buildContainerdRuntime()
	defer containerdRuntime.Close()
	runtimeMetricsHandle := startCtldRuntimeMetrics(ctx, ctldCfg, containerdRuntime, podCache, obsProvider, zapLogger)
	httpServer := newHTTPServer(httpAddr, combinedController{
		Controller: probeController,
		Portal:     portalManager,
		RootFS:     buildRootFSController(ctx, storageCfg, portalManager, containerdRuntime),
	})
	if obsProvider != nil {
		httpServer.Handler = httpobs.ServerMiddleware(obsProvider.HTTPServerConfig(zapLogger))(httpServer.Handler)
		httpServer.ConnState = httpobs.NewConnStateTracker(obsProvider.HTTPServerConfig(nil)).Wrap(httpServer.ConnState)
	}
	httpListener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("listen for ctld HTTP server: %w", err)
	}
	go func() {
		if err := httpServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
			serviceErrors <- fmt.Errorf("ctld HTTP server: %w", err)
		}
	}()

	if options.setReady != nil {
		options.setReady(portalManager.RecoveryError() == nil)
		defer options.setReady(false)
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					options.setReady(portalManager.RecoveryError() == nil)
				}
			}
		}()
	}
	var runErr error
	select {
	case <-parent.Done():
		log.Printf("ctld primary shutting down: %v", parent.Err())
	case runErr = <-serviceErrors:
		log.Printf("ctld primary service failed: %v", runErr)
	}
	cancel()
	httpShutdownCtx, httpShutdownCancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	_ = httpServer.Shutdown(httpShutdownCtx)
	httpShutdownCancel()
	runtimeMetricsShutdownCtx, runtimeMetricsShutdownCancel := context.WithTimeout(context.Background(), runtimeMetricsShutdownTimeout)
	if err := runtimeMetricsHandle.Shutdown(runtimeMetricsShutdownCtx); err != nil {
		log.Printf("ctld runtime metric producer shutdown completed with errors: %v", err)
	}
	runtimeMetricsShutdownCancel()
	csiServer.Stop()
	portalShutdownCtx, portalShutdownCancel := context.WithTimeout(context.Background(), portalShutdownTimeout)
	if err := portalManager.Shutdown(portalShutdownCtx); err != nil {
		log.Printf("ctld volume portal shutdown completed with errors: %v", err)
	}
	portalShutdownCancel()
	return runErr
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

func buildNodePodCache(ctx context.Context, k8sClient kubernetes.Interface) *ctldpower.PodCache {
	if k8sClient == nil {
		return nil
	}
	podCache, err := ctldpower.NewNodePodCache(k8sClient, nodeName, 0)
	if err != nil {
		log.Printf("ctld pod cache disabled: %v", err)
		return nil
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

func buildRootFSController(ctx context.Context, storageCfg *apiconfig.StorageProxyConfig, portalResolver ctldrootfs.PortalResolver, runtime *ctldrootfs.ContainerdRuntime) rootFSHandler {
	store, err := buildRootFSObjectStore(storageCfg)
	if err != nil {
		log.Printf("ctld rootfs object store disabled: %v", err)
	}
	objectCache := buildRootFSObjectCache(ctx)
	return ctldrootfs.NewController(ctldrootfs.Config{
		Runtime:        runtime,
		Store:          store,
		PortalResolver: portalResolver,
		SnapshotDir:    filepath.Join(portalRoot, "rootfs", "prepared"),
		ObjectCache:    objectCache,
	})
}

func buildContainerdRuntime() *ctldrootfs.ContainerdRuntime {
	return ctldrootfs.NewContainerdRuntime(ctldrootfs.ContainerdRuntimeConfig{
		CRIEndpoint:            criEndpoint,
		ContainerdEndpoint:     containerdEndpoint,
		ContainerdRoot:         containerdRoot,
		ContainerdHostRoot:     containerdHostRoot,
		ContainerdDataRoot:     containerdDataRoot,
		ContainerdHostDataRoot: containerdHostDataRoot,
		RootFSCacheDir:         filepath.Join(portalRoot, "rootfs"),
		Namespace:              containerdNamespace,
	})
}

func buildRootFSObjectCache(ctx context.Context) *ctldrootfs.ObjectCache {
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
		Dir:           filepath.Join(portalRoot, "rootfs", "objects"),
		MaxBytes:      maxBytes,
		MinFreeBytes:  minFreeBytes,
		MaxAge:        rootFSObjectCacheMaxAge,
		SweepInterval: rootFSObjectCacheSweepInterval,
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

func buildRootFSObjectStore(cfg *apiconfig.StorageProxyConfig) (objectstore.Store, error) {
	if cfg == nil {
		return nil, fmt.Errorf("storage config is not configured")
	}
	store, err := objectstore.Create(objectstore.Config{
		Type:         cfg.ObjectStorageType,
		Bucket:       cfg.S3Bucket,
		Region:       cfg.S3Region,
		Endpoint:     cfg.S3Endpoint,
		AccessKey:    cfg.S3AccessKey,
		SecretKey:    cfg.S3SecretKey,
		SessionToken: cfg.S3SessionToken,
	})
	if err != nil {
		return nil, err
	}
	if cfg.ObjectEncryptionEnabled {
		keyPEM, err := objectstore.LoadEncryptionKey(cfg.ObjectEncryptionKeyPath)
		if err != nil {
			return nil, err
		}
		keyEncryptor, err := objectstore.NewKeyEncryptor(keyPEM, cfg.ObjectEncryptionPassphrase)
		if err != nil {
			return nil, err
		}
		store = objectstore.Encrypting(store, objectstore.EncryptionConfig{
			Enabled:      true,
			Algorithm:    cfg.ObjectEncryptionAlgo,
			KeyEncryptor: keyEncryptor,
		})
	}
	return store, nil
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

type combinedController struct {
	ctldserver.Controller
	Portal volumePortalHandler
	RootFS rootFSHandler
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
