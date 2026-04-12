package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	ctldfuseplugin "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/fuseplugin"
	ctldpower "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/power"
	ctldserver "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/server"
	"github.com/sandbox0-ai/sandbox0/pkg/k8s"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

var (
	mountsAllowed = 5
	httpAddr      = ":8095"
	kubeconfig    = ""
	cgroupRoot    = "/host-sys/fs/cgroup"
	criEndpoint   = "/host-run/containerd/containerd.sock"
	procRoot      = "/proc"
	nodeName      = os.Getenv("NODE_NAME")
)

func main() {
	flag.IntVar(&mountsAllowed, "mounts-allowed", 100, "maximum times the fuse device can be mounted")
	flag.StringVar(&httpAddr, "http-addr", ":8095", "HTTP listen address for ctld health and control endpoints")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "optional kubeconfig path used by ctld")
	flag.StringVar(&cgroupRoot, "cgroup-root", "/host-sys/fs/cgroup", "host cgroup root mounted into ctld")
	flag.StringVar(&criEndpoint, "cri-endpoint", "/host-run/containerd/containerd.sock", "host CRI socket used to read pod sandbox stats")
	flag.StringVar(&procRoot, "proc-root", "/proc", "host proc root used to inspect sandbox processes")
	flag.StringVar(&nodeName, "node-name", os.Getenv("NODE_NAME"), "current node name used to validate local sandbox ownership")
	flag.Parse()

	log.Println("Starting ctld")
	defer func() { log.Println("Stopped ctld") }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	httpServer := newHTTPServer(httpAddr, buildPowerController(ctx))
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

func buildPowerController(ctx context.Context) ctldserver.Controller {
	if ctx == nil {
		ctx = context.Background()
	}
	k8sClient, err := k8s.NewClient(kubeconfig)
	if err != nil {
		log.Printf("ctld power control disabled: build kubernetes client: %v", err)
		return ctldserver.NotImplementedController{}
	}
	resolver := ctldpower.NewPodResolver(k8sClient, nodeName, cgroupRoot)
	resolver.ProcRoot = procRoot
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
	controller := ctldpower.NewController(resolver, nil)
	controller.StatsProvider = ctldpower.NewCRIStatsProvider(criEndpoint)
	return controller
}
