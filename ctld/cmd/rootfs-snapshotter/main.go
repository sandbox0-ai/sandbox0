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

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	ctldobjectmetering "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/objectrequestmetering"
	ctldrootfs "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfs"
	ctldrootfssnapshotter "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfssnapshotter"
	ctldrootfsstore "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	registrationProbeKey = "__sandbox0_rootfs_snapshotter_readiness__"
	shutdownTimeout      = 10 * time.Second
)

var (
	root                  = "/var/lib/containerd/sandbox0-rootfs-snapshotter"
	socketPath            = "/run/sandbox0-rootfs-snapshotter/snapshotter.sock"
	containerdAddress     = "/host-run/containerd/containerd.sock"
	namespace             = "k8s.io"
	healthAddress         = ":8096"
	objectCacheMaxBytes   = "20Gi"
	objectCacheMinFree    = "1Gi"
	objectCacheMaxAge     time.Duration
	objectCacheSweep      = time.Minute
	metadataCacheMaxBytes = "128Mi"
)

func main() {
	flag.StringVar(&root, "root", root, "persistent root for delegated overlay metadata and object cache")
	flag.StringVar(&socketPath, "address", socketPath, "external snapshotter Unix socket")
	flag.StringVar(&containerdAddress, "containerd-address", containerdAddress, "containerd Unix socket used for registration readiness")
	flag.StringVar(&namespace, "namespace", namespace, "containerd namespace")
	flag.StringVar(&healthAddress, "health-address", healthAddress, "health, readiness, and metrics listen address")
	flag.StringVar(&objectCacheMaxBytes, "object-cache-max-bytes", objectCacheMaxBytes, "verified node-local CAS cache limit")
	flag.StringVar(&objectCacheMinFree, "object-cache-min-free-bytes", objectCacheMinFree, "minimum free bytes retained on the cache filesystem")
	flag.DurationVar(&objectCacheMaxAge, "object-cache-max-age", objectCacheMaxAge, "maximum cached object age; zero disables age eviction")
	flag.DurationVar(&objectCacheSweep, "object-cache-sweep-interval", objectCacheSweep, "object cache sweep interval")
	flag.StringVar(&metadataCacheMaxBytes, "metadata-cache-max-bytes", metadataCacheMaxBytes, "shared in-memory metadata cache limit")
	flag.Parse()
	if err := run(); err != nil {
		log.Fatalf("rootfs snapshotter stopped with error: %v", err)
	}
}

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}
	defer logger.Sync()
	cfg := apiconfig.LoadCtldConfig()
	pool, err := meteringPool(ctx, &cfg.StorageProxyConfig)
	if err != nil {
		logger.Warn("Rootfs snapshotter request metering disabled", zap.Error(err))
	}
	if pool != nil {
		defer pool.Close()
	}
	instance := strings.TrimSpace(os.Getenv("NODE_NAME"))
	if instance == "" {
		instance = strings.TrimSpace(os.Getenv("POD_NAME"))
	}
	if instance == "" {
		instance = "rootfs-snapshotter"
	} else {
		instance += "-rootfs-snapshotter"
	}
	requestMeter := ctldobjectmetering.Start(ctx, &cfg.StorageProxyConfig, pool, instance, logger)
	defer ctldobjectmetering.Flush(requestMeter, logger)
	store, err := ctldrootfsstore.NewObjectStore(&cfg.StorageProxyConfig, requestMeter)
	if err != nil {
		return fmt.Errorf("create rootfs object store: %w", err)
	}
	cacheMax, err := parseBytes(objectCacheMaxBytes)
	if err != nil {
		return err
	}
	cacheMinFree, err := parseBytes(objectCacheMinFree)
	if err != nil {
		return err
	}
	metadataMax, err := parseBytes(metadataCacheMaxBytes)
	if err != nil {
		return err
	}
	objectCache := ctldrootfs.NewObjectCache(ctldrootfs.ObjectCacheConfig{
		Dir:           filepath.Join(root, "objects"),
		MaxBytes:      cacheMax,
		MinFreeBytes:  cacheMinFree,
		MaxAge:        objectCacheMaxAge,
		SweepInterval: objectCacheSweep,
		Observer:      ctldrootfs.NewObserver(prometheus.DefaultRegisterer, logger),
	})
	objectCache.Start(ctx)
	service, err := ctldrootfssnapshotter.NewService(ctldrootfssnapshotter.ServiceConfig{
		Root:          root,
		SocketPath:    socketPath,
		Namespace:     namespace,
		Store:         store,
		ObjectCache:   objectCache,
		MetadataBytes: metadataMax,
	})
	if err != nil {
		return err
	}
	defer service.Close()
	containerdClient, err := containerd.New(
		containerdAddress,
		containerd.WithDefaultNamespace(namespace),
		containerd.WithTimeout(2*time.Second),
	)
	if err != nil {
		return fmt.Errorf("create containerd readiness client: %w", err)
	}
	defer containerdClient.Close()
	ready := &readiness{
		service:     service,
		client:      containerdClient,
		store:       store,
		snapshotter: rootfshead.SnapshotterName,
	}
	healthServer := &http.Server{
		Addr:              healthAddress,
		Handler:           healthHandler(ready),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serviceErrors := make(chan error, 1)
	healthErrors := make(chan error, 1)
	go func() { serviceErrors <- service.Run(ctx) }()
	go func() {
		err := healthServer.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			healthErrors <- err
		}
	}()
	var result error
	select {
	case <-ctx.Done():
	case err := <-serviceErrors:
		result = err
		cancel()
	case err := <-healthErrors:
		result = fmt.Errorf("serve rootfs snapshotter health endpoint: %w", err)
		cancel()
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()
	return errors.Join(result, healthServer.Shutdown(shutdownCtx))
}

type readyService interface {
	Ready() error
}

type readiness struct {
	service     readyService
	client      *containerd.Client
	store       objectstore.Store
	snapshotter string
}

func (r *readiness) Ready(ctx context.Context) error {
	if r == nil || r.service == nil || r.client == nil || r.store == nil {
		return fmt.Errorf("rootfs snapshotter readiness is not configured")
	}
	if err := r.service.Ready(); err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := r.client.SnapshotService(r.snapshotter).Stat(probeCtx, registrationProbeKey)
	if err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("containerd snapshotter %q is not registered: %w", r.snapshotter, err)
	}
	_, err = r.store.Head("sandbox-rootfs/cow-v3/readiness")
	if err != nil && !objectstore.IsNotFound(err) {
		return fmt.Errorf("rootfs object store is unavailable: %w", err)
	}
	return nil
}

func healthHandler(ready interface{ Ready(context.Context) error }) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(response http.ResponseWriter, request *http.Request) {
		if ready == nil || ready.Ready(request.Context()) != nil {
			http.Error(response, "rootfs snapshotter is not ready", http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ready\n"))
	})
	return mux
}

func meteringPool(ctx context.Context, cfg *apiconfig.StorageProxyConfig) (*pgxpool.Pool, error) {
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

func parseBytes(raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	switch strings.ToLower(value) {
	case "", "0", "off", "disabled", "false":
		return 0, nil
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, fmt.Errorf("parse byte quantity %q: %w", raw, err)
	}
	if quantity.Value() < 0 {
		return 0, fmt.Errorf("byte quantity %q must be non-negative", raw)
	}
	return quantity.Value(), nil
}
