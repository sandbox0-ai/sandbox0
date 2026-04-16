package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	ctldfuseplugin "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/fuseplugin"
	ctldpower "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/power"
	ctldserver "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/server"
	"github.com/sandbox0-ai/sandbox0/pkg/k8s"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	"go.uber.org/zap"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

var (
	mountsAllowed          = 5
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
)

func main() {
	flag.IntVar(&mountsAllowed, "mounts-allowed", 100, "maximum times the fuse device can be mounted")
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

	httpServer := newHTTPServer(httpAddr, buildPowerController(ctx, obsProvider))
	if obsProvider != nil {
		httpServer.Handler = httpobs.ServerMiddleware(obsProvider.HTTPServerConfig(zapLogger))(httpServer.Handler)
	}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("ctld http server failed: %v", err)
		}
	}()

	log.Println("Starting FS watcher.")
	watcher, err := ctldfuseplugin.NewFSWatcher(pluginapi.DevicePluginPath)
	if err != nil {
		log.Println("Failed to created FS watcher.")
		os.Exit(1)
	}
	defer watcher.Close()

	log.Println("Starting OS watcher.")
	sigs := ctldfuseplugin.NewOSWatcher(syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	restart := true
	var devicePlugin *ctldfuseplugin.DevicePlugin

L:
	for {
		if restart {
			if devicePlugin != nil {
				devicePlugin.Stop()
			}

			devicePlugin = ctldfuseplugin.NewDevicePlugin(mountsAllowed)
			if err := devicePlugin.Serve(); err != nil {
				log.Println("Could not contact Kubelet, retrying. Did you enable the device plugin feature gate?")
			} else {
				restart = false
			}
		}

		select {
		case event := <-watcher.Events:
			if event.Name == pluginapi.KubeletSocket && event.Op&fsnotify.Create == fsnotify.Create {
				log.Printf("inotify: %s created, restarting.", pluginapi.KubeletSocket)
				restart = true
			}

		case err := <-watcher.Errors:
			log.Printf("inotify: %s", err)

		case s := <-sigs:
			switch s {
			case syscall.SIGHUP:
				log.Println("Received SIGHUP, restarting.")
				restart = true
			default:
				log.Printf("Received signal \"%v\", shutting down.", s)
				cancel()
				if devicePlugin != nil {
					devicePlugin.Stop()
				}
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = httpServer.Shutdown(shutdownCtx)
				cancel()
				break L
			}
		}
	}
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
