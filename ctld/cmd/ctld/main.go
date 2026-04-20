package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	ctldportal "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
	ctldpower "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/power"
	ctldserver "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/server"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/k8s"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	storagedb "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"go.uber.org/zap"
)

var (
	httpAddr               = ":8095"
	kubeconfig             = ""
	cgroupRoot             = "/host-sys/fs/cgroup"
	criEndpoint            = "/host-run/containerd/containerd.sock"
	procRoot               = "/proc"
	nodeName               = os.Getenv("NODE_NAME")
	pauseMinMemoryRequest  = "10Mi"
	pauseMinMemoryLimit    = "32Mi"
	pauseMemoryBufferRatio = "1.1"
	pauseMinCPU            = "10m"
	defaultSandboxTTL      time.Duration
	portalRoot             = "/var/lib/sandbox0/ctld"
	csiSocket              = "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock"
	podName                = os.Getenv("POD_NAME")
	podNamespace           = os.Getenv("POD_NAMESPACE")
)

func main() {
	flag.StringVar(&httpAddr, "http-addr", ":8095", "HTTP listen address for ctld health and control endpoints")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "optional kubeconfig path used by ctld")
	flag.StringVar(&cgroupRoot, "cgroup-root", "/host-sys/fs/cgroup", "host cgroup root mounted into ctld")
	flag.StringVar(&criEndpoint, "cri-endpoint", "/host-run/containerd/containerd.sock", "host CRI socket used to read pod sandbox stats")
	flag.StringVar(&procRoot, "proc-root", "/proc", "host proc root used to inspect sandbox processes")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "current node name used to validate local sandbox ownership")
	flag.StringVar(&pauseMinMemoryRequest, "pause-min-memory-request", "10Mi", "minimum memory request to apply to paused sandbox pods")
	flag.StringVar(&pauseMinMemoryLimit, "pause-min-memory-limit", "32Mi", "minimum memory limit to apply to paused sandbox pods")
	flag.StringVar(&pauseMemoryBufferRatio, "pause-memory-buffer-ratio", "1.1", "memory limit multiplier applied to paused sandbox working set")
	flag.StringVar(&pauseMinCPU, "pause-min-cpu", "10m", "minimum CPU request and limit to apply to paused sandbox pods")
	flag.DurationVar(&defaultSandboxTTL, "default-sandbox-ttl", 0, "default sandbox TTL restored on resume when no original TTL is recorded")
	flag.StringVar(&portalRoot, "volume-portal-root", "/var/lib/sandbox0/ctld", "host-local root for ctld volume portal WAL and cache")
	flag.StringVar(&csiSocket, "csi-socket", "/var/lib/kubelet/plugins/volume.sandbox0.ai/csi.sock", "CSI endpoint socket for sandbox volume portals")
	flag.Parse()

	log.Println("Starting ctld")
	defer func() { log.Println("Stopped ctld") }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	zapLogger, err := zap.NewProduction()
	if err != nil {
		log.Printf("ctld observability disabled: create zap logger: %v", err)
	}
	var obsProvider *observability.Provider
	if zapLogger != nil {
		defer zapLogger.Sync()
		obsProvider, err = observability.New(observability.Config{
			ServiceName: "ctld",
			Logger:      zapLogger,
			TraceExporter: observability.TraceExporterConfig{
				Type:     os.Getenv("OTEL_EXPORTER_TYPE"),
				Endpoint: os.Getenv("OTEL_EXPORTER_ENDPOINT"),
			},
		})
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
	csiServer := ctldportal.NewCSIServer(nodeName, portalManager)
	go func() {
		if err := csiServer.Serve(csiSocket); err != nil && ctx.Err() == nil {
			log.Fatalf("ctld volume portal CSI server failed: %v", err)
		}
	}()
	defer csiServer.Stop()

	httpServer := newHTTPServer(httpAddr, combinedController{
		Controller: buildPowerController(ctx, obsProvider),
		Portal:     portalManager,
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

func buildPowerController(ctx context.Context, obsProvider *observability.Provider) ctldserver.Controller {
	if ctx == nil {
		ctx = context.Background()
	}
	k8sClient, err := k8s.NewClientWithObservability(kubeconfig, obsProvider)
	if err != nil {
		log.Printf("ctld power control disabled: build kubernetes client: %v", err)
		return ctldserver.NotImplementedController{}
	}
	resolver := ctldpower.NewPodResolver(k8sClient, nodeName, cgroupRoot)
	resolver.ProcRoot = procRoot
	controller := ctldpower.NewController(resolver, nil)
	if obsProvider != nil {
		controller.HTTPClient = obsProvider.HTTP.NewClient(httpobs.Config{Timeout: 2 * time.Second})
	}
	controller.StatsProvider = ctldpower.NewCRIStatsProvider(criEndpoint)

	if podCache, err := ctldpower.NewNodePodCache(k8sClient, nodeName, 0); err != nil {
		log.Printf("ctld pod cache disabled: %v", err)
	} else {
		ratio, err := strconv.ParseFloat(pauseMemoryBufferRatio, 64)
		if err != nil || ratio <= 0 {
			log.Printf("invalid pause memory buffer ratio %q, using default 1.1", pauseMemoryBufferRatio)
			ratio = 1.1
		}
		powerReconciler := ctldpower.NewPowerReconciler(k8sClient, podCache.PodLister(), resolver, controller, ctldpower.PowerReconcilerConfig{
			PauseMinMemoryRequest:  pauseMinMemoryRequest,
			PauseMinMemoryLimit:    pauseMinMemoryLimit,
			PauseMemoryBufferRatio: ratio,
			PauseMinCPU:            pauseMinCPU,
			DefaultSandboxTTL:      defaultSandboxTTL,
		})
		if err := podCache.AddEventHandler(powerReconciler.EventHandler()); err != nil {
			log.Printf("ctld power reconciler disabled: add pod handler: %v", err)
		}
		podCache.Start(ctx)
		resolver.SetPodCache(podCache.PodLister(), podCache.PodIndexer())
		go func() {
			syncCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if !podCache.WaitForSync(syncCtx) && ctx.Err() == nil {
				log.Printf("ctld pod cache did not sync before timeout; live kubernetes lookups remain enabled")
			}
			powerReconciler.EnqueueAll()
			powerReconciler.Run(ctx, 1)
		}()
	}
	return controller
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
}

func (c combinedController) BindVolumePortal(r *http.Request, req ctldapi.BindVolumePortalRequest) (ctldapi.BindVolumePortalResponse, int) {
	if c.Portal == nil {
		return ctldapi.BindVolumePortalResponse{Error: "ctld volume portals not implemented"}, http.StatusNotImplemented
	}
	resp, err := c.Portal.Bind(r.Context(), req)
	if err != nil {
		return ctldapi.BindVolumePortalResponse{Error: err.Error()}, http.StatusBadRequest
	}
	return resp, http.StatusOK
}

func (c combinedController) UnbindVolumePortal(r *http.Request, req ctldapi.UnbindVolumePortalRequest) (ctldapi.UnbindVolumePortalResponse, int) {
	if c.Portal == nil {
		return ctldapi.UnbindVolumePortalResponse{Error: "ctld volume portals not implemented"}, http.StatusNotImplemented
	}
	resp, err := c.Portal.Unbind(r.Context(), req)
	if err != nil {
		return ctldapi.UnbindVolumePortalResponse{Error: err.Error()}, http.StatusBadRequest
	}
	return resp, http.StatusOK
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

type volumePortalHandler interface {
	Bind(ctx context.Context, req ctldapi.BindVolumePortalRequest) (ctldapi.BindVolumePortalResponse, error)
	Unbind(ctx context.Context, req ctldapi.UnbindVolumePortalRequest) (ctldapi.UnbindVolumePortalResponse, error)
	MountedVolumeHandler() http.Handler
}
