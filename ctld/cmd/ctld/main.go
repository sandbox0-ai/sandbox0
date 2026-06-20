package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
)

var (
	httpAddr               = ":8095"
	kubeconfig             = ""
	criEndpoint            = "/host-run/containerd/containerd.sock"
	containerdEndpoint     = "/host-run/containerd/containerd.sock"
	containerdRoot         = "/host-run/containerd"
	containerdHostRoot     = "/run/containerd"
	containerdDataRoot     = "/host-var-lib/containerd"
	containerdHostDataRoot = "/var/lib/containerd"
	containerdNamespace    = "k8s.io"
	nodeName               = os.Getenv("NODE_NAME")
	portalRoot             = "/var/lib/sandbox0/ctld"
	csiSocket              = "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock"
	podName                = os.Getenv("POD_NAME")
	podNamespace           = os.Getenv("POD_NAMESPACE")
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
	flag.StringVar(&csiSocket, "csi-socket", "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock", "CSI endpoint socket for sandbox volume portals")
	flag.Parse()

	log.Println("Starting ctld")
	defer func() { log.Println("Stopped ctld") }()

	ctx, cancel := context.WithCancel(context.Background())
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

	storageCfg := apiconfig.LoadStorageProxyConfig()
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

	portalManager := ctldportal.NewManager(ctldportal.Config{
		NodeName:      nodeName,
		RootDir:       portalRoot,
		Logger:        zapLogger,
		StorageConfig: storageCfg,
		Repository:    repo,
		PodName:       podName,
		PodNamespace:  podNamespace,
	})
	go portalManager.Run(ctx)
	csiServer := ctldportal.NewCSIServer(nodeName, portalManager)
	go func() {
		if err := csiServer.Serve(csiSocket); err != nil && ctx.Err() == nil {
			log.Fatalf("ctld volume portal CSI server failed: %v", err)
		}
	}()
	defer csiServer.Stop()

	probeController := buildProbeController(ctx, obsProvider)
	httpServer := newHTTPServer(httpAddr, combinedController{
		Controller: probeController,
		Portal:     portalManager,
		RootFS:     buildRootFSController(storageCfg, portalManager),
	})
	if obsProvider != nil {
		httpServer.Handler = httpobs.ServerMiddleware(obsProvider.HTTPServerConfig(zapLogger))(httpServer.Handler)
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("ctld http server failed: %v", err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	s := <-sigs
	log.Printf("Received signal \"%v\", shutting down.", s)
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = httpServer.Shutdown(shutdownCtx)
	shutdownCancel()
}

func newHTTPServer(addr string, controller ctldserver.Controller) *http.Server {
	return &http.Server{Addr: addr, Handler: ctldserver.NewMux(controller)}
}

func buildProbeController(ctx context.Context, obsProvider *observability.Provider) ctldserver.Controller {
	if ctx == nil {
		ctx = context.Background()
	}
	k8sClient, err := k8s.NewClientWithObservability(kubeconfig, obsProvider)
	if err != nil {
		log.Printf("ctld probe control disabled: build kubernetes client: %v", err)
		return ctldserver.NotImplementedController{}
	}
	resolver := ctldpower.NewPodResolver(k8sClient, nodeName)
	controller := ctldpower.NewController(resolver)
	if obsProvider != nil {
		controller.HTTPClient = obsProvider.HTTP.NewClient(httpobs.Config{Timeout: 2 * time.Second})
	}

	if podCache, err := ctldpower.NewNodePodCache(k8sClient, nodeName, 0); err != nil {
		log.Printf("ctld pod cache disabled: %v", err)
	} else {
		podCache.Start(ctx)
		resolver.SetPodCache(podCache.PodLister(), podCache.PodIndexer())
		go func() {
			syncCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if !podCache.WaitForSync(syncCtx) && ctx.Err() == nil {
				log.Printf("ctld pod cache did not sync before timeout; live kubernetes lookups remain enabled")
			}
		}()
	}
	return controller
}

func buildRootFSController(storageCfg *apiconfig.StorageProxyConfig, portalResolver ctldrootfs.PortalResolver) ctldserver.RootFSController {
	store, err := buildRootFSObjectStore(storageCfg)
	if err != nil {
		log.Printf("ctld rootfs object store disabled: %v", err)
	}
	return ctldrootfs.NewController(ctldrootfs.Config{
		Runtime: ctldrootfs.NewContainerdRuntime(ctldrootfs.ContainerdRuntimeConfig{
			CRIEndpoint:            criEndpoint,
			ContainerdEndpoint:     containerdEndpoint,
			ContainerdRoot:         containerdRoot,
			ContainerdHostRoot:     containerdHostRoot,
			ContainerdDataRoot:     containerdDataRoot,
			ContainerdHostDataRoot: containerdHostDataRoot,
			RootFSCacheDir:         filepath.Join(portalRoot, "rootfs"),
			Namespace:              containerdNamespace,
		}),
		Store:          store,
		PortalResolver: portalResolver,
	})
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
	RootFS ctldserver.RootFSController
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

func (c combinedController) ApplyRootFS(r *http.Request, req ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int) {
	if c.RootFS == nil {
		return ctldapi.ApplyRootFSResponse{Error: "ctld rootfs apply not implemented"}, http.StatusNotImplemented
	}
	return c.RootFS.ApplyRootFS(r, req)
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
