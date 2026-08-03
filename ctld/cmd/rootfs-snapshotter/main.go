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
	"strings"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	ctldobjectmetering "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/objectrequestmetering"
	ctldrootfssnapshotter "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfssnapshotter"
	ctldrootfsstore "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"go.uber.org/zap"
)

const (
	shutdownTimeout          = 10 * time.Second
	registrationProbeTimeout = 2 * time.Second
	registrationProbeKey     = "__sandbox0_rootfs_snapshotter_readiness__"
)

var (
	root              = "/var/lib/containerd/sandbox0-rootfs-snapshotter"
	socketPath        = "/run/sandbox0-rootfs-snapshotter/snapshotter.sock"
	namespace         = "k8s.io"
	healthAddr        = ":8096"
	containerdAddress = "/host-run/containerd/containerd.sock"
)

func main() {
	flag.StringVar(&root, "root", root, "persistent root for private overlay snapshot metadata")
	flag.StringVar(&socketPath, "address", socketPath, "external containerd snapshotter Unix socket")
	flag.StringVar(&namespace, "namespace", namespace, "containerd namespace")
	flag.StringVar(&healthAddr, "health-address", healthAddr, "health and readiness HTTP listen address")
	flag.StringVar(&containerdAddress, "containerd-address", containerdAddress, "main containerd Unix socket used to verify proxy registration")
	flag.Parse()

	if err := run(); err != nil {
		log.Fatalf("rootfs snapshotter stopped with error: %v", err)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()
	ctldCfg := apiconfig.LoadCtldConfig()
	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("create rootfs snapshotter logger: %w", err)
	}
	defer logger.Sync()
	meteringPool, err := newSnapshotterMeteringPool(ctx, &ctldCfg.StorageProxyConfig)
	if err != nil {
		log.Printf("rootfs snapshotter object request metering disabled: %v", err)
	}
	if meteringPool != nil {
		defer meteringPool.Close()
	}
	meteringInstance := strings.TrimSpace(os.Getenv("NODE_NAME"))
	if meteringInstance == "" {
		meteringInstance = strings.TrimSpace(os.Getenv("POD_NAME"))
	}
	if meteringInstance == "" {
		meteringInstance = "rootfs-snapshotter"
	} else {
		meteringInstance += "-rootfs-snapshotter"
	}
	requestMeter := ctldobjectmetering.Start(ctx, &ctldCfg.StorageProxyConfig, meteringPool, meteringInstance, logger)
	defer ctldobjectmetering.Flush(requestMeter, logger)
	store, err := ctldrootfsstore.New(&ctldCfg.StorageProxyConfig, requestMeter)
	if err != nil {
		return fmt.Errorf("create rootfs object store: %w", err)
	}
	containerdClient, err := containerd.New(
		containerdAddress,
		containerd.WithDefaultNamespace(namespace),
		containerd.WithTimeout(registrationProbeTimeout),
	)
	if err != nil {
		return fmt.Errorf("create containerd registration client: %w", err)
	}
	defer containerdClient.Close()
	service, err := ctldrootfssnapshotter.NewService(ctldrootfssnapshotter.ServiceConfig{
		Root:       root,
		SocketPath: socketPath,
		Namespace:  namespace,
		Store:      store,
		Observer:   ctldrootfssnapshotter.NewObserver(prometheus.DefaultRegisterer, logger),
	})
	if err != nil {
		return err
	}
	defer service.Close()
	ready := combinedReadiness{
		service: service,
		registration: containerdRegistrationProbe{
			client:      containerdClient,
			snapshotter: rootfshead.SnapshotterName,
			timeout:     registrationProbeTimeout,
		},
	}

	healthServer := &http.Server{
		Addr:              healthAddr,
		Handler:           healthHandler(ready),
		ReadHeaderTimeout: 5 * time.Second,
	}
	snapshotterErrors := make(chan error, 1)
	httpErrors := make(chan error, 1)
	go func() { snapshotterErrors <- service.Run(ctx) }()
	go func() {
		err := healthServer.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			httpErrors <- err
		}
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-snapshotterErrors:
		runErr = err
		cancel()
	case err := <-httpErrors:
		runErr = fmt.Errorf("serve health endpoint: %w", err)
		cancel()
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, err)
	}
	return runErr
}

func newSnapshotterMeteringPool(ctx context.Context, cfg *apiconfig.StorageProxyConfig) (*pgxpool.Pool, error) {
	if cfg == nil || !cfg.Metering.Enabled || strings.TrimSpace(cfg.DatabaseURL) == "" {
		return nil, nil
	}
	schema := strings.TrimSpace(cfg.DatabaseSchema)
	if schema == "" {
		schema = "storage_proxy"
	}
	return dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     cfg.DatabaseURL,
		MaxConns:        int32(cfg.DatabaseMaxConns),
		MinConns:        int32(cfg.DatabaseMinConns),
		DefaultMaxConns: 5,
		DefaultMinConns: 1,
		Schema:          schema,
	})
}

type serviceReadiness interface {
	Ready() bool
}

type readiness interface {
	Ready(context.Context) error
}

type registrationReadiness interface {
	Registered(context.Context) error
}

type combinedReadiness struct {
	service      serviceReadiness
	registration registrationReadiness
}

func (r combinedReadiness) Ready(ctx context.Context) error {
	if r.service == nil || !r.service.Ready() {
		return fmt.Errorf("snapshotter service is not ready")
	}
	if r.registration == nil {
		return fmt.Errorf("containerd registration probe is not configured")
	}
	return r.registration.Registered(ctx)
}

type containerdRegistrationProbe struct {
	client      *containerd.Client
	snapshotter string
	timeout     time.Duration
}

func (p containerdRegistrationProbe) Registered(ctx context.Context) error {
	if p.client == nil {
		return fmt.Errorf("containerd client is not configured")
	}
	timeout := p.timeout
	if timeout <= 0 {
		timeout = registrationProbeTimeout
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := p.client.SnapshotService(p.snapshotter).Stat(probeCtx, registrationProbeKey)
	if err == nil || errdefs.IsNotFound(err) {
		return nil
	}
	return fmt.Errorf("containerd snapshotter %q is not registered: %w", p.snapshotter, err)
}

func healthHandler(ready readiness) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, request *http.Request) {
		if ready == nil || ready.Ready(request.Context()) != nil {
			http.Error(w, "snapshotter is not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	return mux
}
