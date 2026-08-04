package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	storageproxyruntime "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/runtime"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
)

type managerInternalAuth struct {
	generator             *internalauth.Generator
	procdTokenGenerator   service.TokenGenerator
	storageTokenGenerator service.TokenGenerator
}

func buildManagerInternalAuth(logger *zap.Logger) managerInternalAuth {
	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(internalauth.DefaultInternalJWTPrivateKeyPath)
	if err != nil {
		logger.Warn("Failed to load internal auth private key, procd and manager storage calls will not work",
			zap.String("path", internalauth.DefaultInternalJWTPrivateKeyPath),
			zap.Error(err),
		)
		return managerInternalAuth{}
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "manager",
		PrivateKey: privateKey,
		TTL:        30 * time.Second,
	})
	logger.Info("Internal auth generators initialized for procd and manager storage communication")
	return managerInternalAuth{
		generator:             generator,
		procdTokenGenerator:   runtimeauth.NewInternalTokenGenerator(generator),
		storageTokenGenerator: runtimeauth.NewManagerStorageAdminTokenGenerator(generator),
	}
}

type managerStorageComponents struct {
	runtime *storageproxyruntime.Runtime
	config  *config.StorageProxyConfig
}

// startManagerStorageRuntime starts the storage subsystem embedded in manager.
// Its independent config file is retained because the two schemas contain
// overlapping keys such as http_port and database_schema.
func startManagerStorageRuntime(
	ctx context.Context,
	cancel context.CancelFunc,
	obsProvider *observability.Provider,
	k8sClient kubernetes.Interface,
	requestObserver objectstore.RequestObserver,
	logger *zap.Logger,
) (*managerStorageComponents, error) {
	configPath := strings.TrimSpace(os.Getenv("STORAGE_RUNTIME_CONFIG_PATH"))
	if configPath == "" {
		logger.Info("Manager storage runtime disabled; STORAGE_RUNTIME_CONFIG_PATH is not set")
		return &managerStorageComponents{}, nil
	}
	storageConfig, err := config.ReadStorageProxyConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("load manager storage config %q: %w", configPath, err)
	}

	logrusLogger := logrus.New()
	logrusLogger.SetFormatter(&logrus.JSONFormatter{})
	logrusLogger.SetOutput(os.Stdout)
	logLevel, err := logrus.ParseLevel(storageConfig.LogLevel)
	if err != nil {
		logLevel = logrus.InfoLevel
	}
	logrusLogger.SetLevel(logLevel)

	runtime, err := storageproxyruntime.New(ctx, storageproxyruntime.Options{
		Config:                     storageConfig,
		Logger:                     logger,
		LogrusLogger:               logrusLogger,
		Observability:              obsProvider,
		K8sClient:                  k8sClient,
		ObjectStoreRequestObserver: requestObserver,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize manager storage runtime: %w", err)
	}
	if err := runtime.Start(ctx); err != nil {
		return nil, fmt.Errorf("start manager storage runtime: %w", err)
	}
	go func() {
		select {
		case runtimeErr := <-runtime.Errors():
			logger.Error("Manager storage runtime failed", zap.Error(runtimeErr))
			cancel()
		case <-ctx.Done():
		}
	}()
	logger.Info("Manager storage runtime started",
		zap.String("configPath", configPath),
		zap.String("address", runtime.Address()),
	)
	return &managerStorageComponents{runtime: runtime, config: storageConfig}, nil
}
