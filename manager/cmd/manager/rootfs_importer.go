package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsimportdiscovery"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsimportworker"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ocirootfs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
)

func configureRootFSImportDiscovery(
	cfg *config.ManagerConfig,
	sources templatestore.ImageSourceStore,
	store *sandboxstore.PGSandboxStore,
	platforms []sandboxstore.RootFSArtifactPlatform,
) (*rootfsimportdiscovery.Worker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("manager config is required")
	}
	worker, err := rootfsimportdiscovery.New(rootfsimportdiscovery.Config{
		Sources: sources, Imports: store, Platforms: platforms,
		FormatGeneration: rootfsblock.DescriptorVersion,
		ProcdProtocol:    cfg.RootFSImporter.ProcdProtocol,
		ProcdDigest:      cfg.RootFSImporter.ProcdDigest,
		Interval:         cfg.RootFSImporter.DiscoveryInterval.Duration,
		PageSize:         cfg.RootFSImporter.DiscoveryPageSize,
	})
	if err != nil {
		return nil, fmt.Errorf("create template RootFS import discovery: %w", err)
	}
	return worker, nil
}

func configureRootFSImportWorker(
	cfg *config.ManagerConfig,
	store *sandboxstore.PGSandboxStore,
	objects objectstore.Store,
) (*rootfsimportworker.Worker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("manager config is required")
	}
	if cfg.RootFSImporter.Disabled {
		return nil, fmt.Errorf("Nomad sandbox runtime requires the durable RootFS importer")
	}
	if store == nil {
		return nil, fmt.Errorf("Nomad RootFS importer requires PostgreSQL")
	}
	conditional, ok := objects.(objectstore.ContextConditionalStore)
	if !ok || !objectstore.SupportsContextConditionalCreate(objects) {
		return nil, fmt.Errorf("Nomad RootFS importer requires contextual conditional object storage")
	}
	procdDigest, err := digest.Parse(cfg.RootFSImporter.ProcdDigest)
	if err != nil {
		return nil, fmt.Errorf("parse RootFS importer procd digest: %w", err)
	}
	if err := ocirootfs.ValidateLocalImportEnvironment(
		cfg.RootFSImporter.WorkRoot,
		cfg.RootFSImporter.ProcdPath,
		procdDigest,
	); err != nil {
		return nil, fmt.Errorf("validate RootFS importer local environment: %w", err)
	}
	resolver, err := ocirootfs.NewDockerResolver(ocirootfs.DockerResolverConfig{
		CredentialsFile: cfg.Registry.PullCredentialsFile,
		PlainHTTPHosts:  cfg.RootFSImporter.PlainHTTPHosts,
	})
	if err != nil {
		return nil, fmt.Errorf("configure RootFS importer registry resolver: %w", err)
	}
	unpacker, err := ocirootfs.NewImporter(resolver, ocirootfs.Limits{})
	if err != nil {
		return nil, fmt.Errorf("configure OCI RootFS importer: %w", err)
	}
	builder, err := rootfsimportworker.NewDurableBuilder(rootfsimportworker.DurableBuilderConfig{
		Store: store, Unpacker: unpacker, Filesystem: rootfsartifact.XFSBuilder{},
		Publisher: rootfsblock.ObjectStorePublisher{Store: conditional},
		WorkRoot:  cfg.RootFSImporter.WorkRoot, ProcdPath: cfg.RootFSImporter.ProcdPath,
	})
	if err != nil {
		return nil, err
	}
	workerID := cfg.RootFSImporter.WorkerID
	if workerID == "" {
		workerID, err = newRootFSImportWorkerID()
		if err != nil {
			return nil, err
		}
	}
	worker, err := rootfsimportworker.New(rootfsimportworker.Config{
		Store: store, Builder: builder, WorkerID: workerID,
		Interval: cfg.RootFSImporter.Interval.Duration, BuildTimeout: cfg.RootFSImporter.BuildTimeout.Duration,
		LeaseTTL: cfg.RootFSImporter.LeaseTTL.Duration, LeaseRenewal: cfg.RootFSImporter.LeaseRenewal.Duration,
		MaxAttempts:       cfg.RootFSImporter.MaxAttempts,
		GarbageInterval:   cfg.RootFSImporter.GarbageInterval.Duration,
		TerminalRetention: cfg.RootFSImporter.TerminalRetention.Duration,
		GarbageLimit:      cfg.RootFSImporter.GarbageLimit,
		ProcdProtocol:     cfg.RootFSImporter.ProcdProtocol, ProcdDigest: cfg.RootFSImporter.ProcdDigest,
	})
	if err != nil {
		return nil, fmt.Errorf("create durable RootFS import worker: %w", err)
	}
	return worker, nil
}

func newRootFSImportWorkerID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate RootFS import worker identity: %w", err)
	}
	return "manager.rootfs.import." + hex.EncodeToString(random[:]), nil
}

func logRootFSImportWorkerPass(logger *zap.Logger, result rootfsimportworker.Result, err error) {
	if logger == nil {
		return
	}
	fields := []zap.Field{
		zap.String("operationID", result.OperationID), zap.String("failureCategory", result.FailureCategory),
		zap.Int("leased", result.Leased), zap.Int("ready", result.Ready),
		zap.Int("released", result.Released), zap.Int("abandoned", result.Abandoned),
		zap.Int("leaseUncertain", result.LeaseUncertain), zap.Int("failed", result.Failed),
		zap.Int("recoveredLeases", result.RecoveredLeases), zap.Int("purgedReady", result.PurgedReady),
		zap.Int("purgedAbandoned", result.PurgedAbandoned), zap.Int("enqueuedObjects", result.EnqueuedObjects),
	}
	if err != nil {
		logger.Warn("Rootfs import worker pass failed", append(fields, zap.Error(err))...)
		return
	}
	if result.Ready > 0 || result.RecoveredLeases > 0 || result.PurgedReady > 0 ||
		result.PurgedAbandoned > 0 || result.EnqueuedObjects > 0 {
		logger.Info("Rootfs import worker pass completed", fields...)
	}
}

func logRootFSImportDiscoveryPass(logger *zap.Logger, result rootfsimportdiscovery.Result, err error) {
	if logger == nil {
		return
	}
	fields := []zap.Field{
		zap.Int("templates", result.Templates),
		zap.Int("requirements", result.Requirements),
		zap.Int("ready", result.Ready),
		zap.Int("ensured", result.Ensured),
		zap.Int("failed", result.Failed),
		zap.Bool("wrapped", result.Wrapped),
	}
	if err != nil {
		logger.Warn("Template Rootfs import discovery pass completed with failures", append(fields, zap.Error(err))...)
		return
	}
	if result.Ensured > 0 {
		logger.Info("Template Rootfs import discovery pass completed", fields...)
	}
}
