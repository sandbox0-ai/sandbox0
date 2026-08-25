package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"gopkg.in/yaml.v3"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/legacyackmigration"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ocirootfs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsobjectstore"
)

func readSourceCatalog(
	ctx context.Context,
	opts options,
	getenv func(string) string,
) (*legacyackmigration.Catalog, error) {
	dsn, err := loadSourceDSN(opts.sourceDSNFile, strings.TrimSpace(getenv("SANDBOX0_LEGACY_SOURCE_DSN")))
	if err != nil {
		return nil, err
	}
	pool, err := openDatabase(ctx, dsn, "legacy source")
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	return legacyackmigration.ReadCatalog(ctx, pool)
}

func readSourceDrain(
	ctx context.Context,
	opts options,
	getenv func(string) string,
) (*sourceDrainSummary, error) {
	dsn, err := loadSourceDSN(opts.sourceDSNFile, strings.TrimSpace(getenv("SANDBOX0_LEGACY_SOURCE_DSN")))
	if err != nil {
		return nil, err
	}
	pool, err := openDatabase(ctx, dsn, "legacy source drain")
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	objectDeletions, deletionWebhooks, meteringOperations, err := readRetirementQueueCounts(ctx, pool)
	if err != nil {
		return nil, err
	}
	return &sourceDrainSummary{
		PendingObjectDeletions: objectDeletions, PendingDeletionWebhooks: deletionWebhooks,
		PendingMeteringOperations: meteringOperations,
	}, nil
}

func requireSourceDrain(drain *sourceDrainSummary) error {
	if drain == nil {
		return fmt.Errorf("source drain status is required")
	}
	if drain.PendingObjectDeletions != 0 || drain.PendingDeletionWebhooks != 0 ||
		drain.PendingMeteringOperations != 0 {
		return fmt.Errorf(
			"source durable queues are not drained: RootFS deletions=%d, deletion webhooks=%d, metering operations=%d",
			drain.PendingObjectDeletions, drain.PendingDeletionWebhooks, drain.PendingMeteringOperations,
		)
	}
	return nil
}

func captureSourceCatalog(
	ctx context.Context,
	opts options,
	getenv func(string) string,
	catalog *legacyackmigration.Catalog,
) (*legacyackmigration.CapturedCatalog, error) {
	dsn, err := loadTargetDSN(opts.targetDSNFile, strings.TrimSpace(getenv("SANDBOX0_MIGRATION_TARGET_DSN")))
	if err != nil {
		return nil, err
	}
	pool, err := openDatabase(ctx, dsn, "migration target")
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	store, err := legacyackmigration.NewCaptureStore(pool)
	if err != nil {
		return nil, err
	}
	return store.CaptureCatalog(ctx, opts.sessionID, opts.targetClusterID, catalog)
}

type targetContext struct {
	pool       *pgxpool.Pool
	store      *legacyackmigration.TargetStore
	capture    *legacyackmigration.CapturedCatalog
	normalized *legacyackmigration.NormalizedCatalog
}

func (t *targetContext) Close() {
	if t != nil && t.pool != nil {
		t.pool.Close()
	}
}

func loadTargetContext(
	ctx context.Context,
	opts options,
	getenv func(string) string,
	normalizeOptions legacyackmigration.NormalizeOptions,
) (*targetContext, error) {
	dsn, err := loadTargetDSN(opts.targetDSNFile, strings.TrimSpace(getenv("SANDBOX0_MIGRATION_TARGET_DSN")))
	if err != nil {
		return nil, err
	}
	pool, err := openDatabase(ctx, dsn, "migration target")
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			pool.Close()
		}
	}()
	captures, err := legacyackmigration.NewCaptureStore(pool)
	if err != nil {
		return nil, err
	}
	captured, err := captures.LoadCapturedCatalog(ctx, opts.sessionID)
	if err != nil {
		return nil, err
	}
	if captured.TargetClusterID != opts.targetClusterID {
		return nil, fmt.Errorf("%w: capture session targets cluster %s, not %s",
			legacyackmigration.ErrTargetMigrationConflict, captured.TargetClusterID, opts.targetClusterID)
	}
	normalized, err := captured.Catalog.Normalize(normalizeOptions)
	if err != nil {
		return nil, fmt.Errorf("normalize captured legacy ACK catalog: %w", err)
	}
	store, err := legacyackmigration.NewTargetStore(pool)
	if err != nil {
		return nil, err
	}
	failed = false
	return &targetContext{pool: pool, store: store, capture: captured, normalized: normalized}, nil
}

func openDatabase(ctx context.Context, dsn, role string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("configure %s database: %w", role, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to %s database: %w", role, err)
	}
	return pool, nil
}

func prepareTargetCatalog(
	ctx context.Context,
	opts options,
	target *targetContext,
) (*legacyackmigration.TargetPreparationResult, error) {
	cfg, err := loadManagerRuntimeConfig(opts.targetManagerConfigFile)
	if err != nil {
		return nil, err
	}
	contract, err := targetContract(cfg)
	if err != nil {
		return nil, err
	}
	return target.store.PrepareCatalog(
		ctx, opts.sessionID, target.capture.SourceCatalogDigest, opts.targetClusterID,
		target.normalized, contract, sandboxstore.NewPGSandboxStore(target.pool),
	)
}

func buildTargetCatalog(
	ctx context.Context,
	opts options,
	target *targetContext,
) (*buildSummary, error) {
	if runtime.GOOS != "linux" {
		return nil, fmt.Errorf("materialized migration builds require Linux")
	}
	targetConfig, err := loadManagerRuntimeConfig(opts.targetManagerConfigFile)
	if err != nil {
		return nil, err
	}
	sourceConfig, err := loadManagerRuntimeConfig(opts.sourceManagerConfigFile)
	if err != nil {
		return nil, err
	}
	contract, err := targetContract(targetConfig)
	if err != nil {
		return nil, err
	}
	if err := target.store.EnsureSchema(ctx); err != nil {
		return nil, err
	}
	if err := target.store.EnsureSession(
		ctx, opts.sessionID, target.capture.SourceCatalogDigest, opts.targetClusterID,
	); err != nil {
		return nil, err
	}
	sourceObjects, err := rootfsobjectstore.Create(sourceConfig.RootFSObjectStorage, nil)
	if err != nil {
		return nil, fmt.Errorf("configure legacy source object store: %w", err)
	}
	targetObjects, err := rootfsobjectstore.Create(targetConfig.RootFSObjectStorage, nil)
	if err != nil {
		return nil, fmt.Errorf("configure target object store: %w", err)
	}
	resolver, err := ocirootfs.NewDockerResolver(ocirootfs.DockerResolverConfig{
		CredentialsFile: targetConfig.Registry.PullCredentialsFile,
		PlainHTTPHosts:  targetConfig.RootFSImporter.PlainHTTPHosts,
	})
	if err != nil {
		return nil, fmt.Errorf("configure migration registry resolver: %w", err)
	}
	unpacker, err := ocirootfs.NewImporter(resolver, ocirootfs.Limits{})
	if err != nil {
		return nil, fmt.Errorf("configure migration OCI importer: %w", err)
	}
	executor, err := legacyackmigration.NewMaterializedBuildExecutor(
		legacyackmigration.MaterializedBuildExecutorConfig{
			Store: target.store, SourceObjects: sourceObjects, TargetObjects: targetObjects,
			Unpacker: unpacker, Filesystem: rootfsartifact.XFSBuilder{},
			WorkRoot: targetConfig.RootFSImporter.WorkRoot, ProcdPath: targetConfig.RootFSImporter.ProcdPath,
			WorkerID: opts.workerID, LeaseTTL: opts.buildLeaseTTL, LeaseRenewal: opts.buildLeaseRenewal,
		},
	)
	if err != nil {
		return nil, err
	}
	baseArtifacts := sandboxstore.NewPGSandboxStore(target.pool)
	result := &buildSummary{Builds: len(target.normalized.MaterializedBuilds)}
	for _, build := range target.normalized.MaterializedBuilds {
		if _, err := target.store.GetBuild(ctx, build.ID); err != nil {
			return nil, fmt.Errorf("materialized build %s was not prepared: %w", build.ID, err)
		}
		artifact, err := baseArtifacts.GetReadyRootFSBaseArtifact(
			ctx, build.SourceOCIDigest,
			sandboxstore.RootFSArtifactPlatform{
				OS: build.Platform.OS, Architecture: build.Platform.Architecture, Variant: build.Platform.Variant,
			},
			sandboxstore.ReadyRootFSArtifactRequirements{
				FormatGeneration: contract.FormatGeneration, LogicalSizeBytes: build.LogicalSizeBytes,
				ProcdProtocol: contract.ProcdProtocol, ProcdDigest: contract.ProcdDigest,
			},
		)
		if errors.Is(err, sandboxstore.ErrRootFSBaseArtifactNotFound) {
			return nil, fmt.Errorf("base artifact for build %s is not ready; keep the target importer running and repeat prepare", build.ID)
		}
		if err != nil {
			return nil, fmt.Errorf("resolve Base artifact for build %s: %w", build.ID, err)
		}
		operation, err := executor.Build(ctx, opts.sessionID, build, contract, artifact.ArtifactDigest)
		if err != nil {
			return nil, fmt.Errorf("execute materialized build %s: %w", build.ID, err)
		}
		if operation == nil || operation.Result == nil {
			return nil, fmt.Errorf("materialized build %s did not publish an exact ready result", build.ID)
		}
		result.Ready++
		result.Objects += operation.Result.Objects
		result.Bytes += operation.Result.Bytes
	}
	return result, nil
}

func targetContract(cfg *config.ManagerConfig) (legacyackmigration.TargetContract, error) {
	if cfg == nil {
		return legacyackmigration.TargetContract{}, fmt.Errorf("target manager config is required")
	}
	protocol := strings.TrimSpace(cfg.RootFSImporter.ProcdProtocol)
	if err := rootfsimporter.ValidateProcdProtocol(protocol); err != nil {
		return legacyackmigration.TargetContract{}, fmt.Errorf("target procd protocol: %w", err)
	}
	procdDigest := strings.TrimSpace(cfg.RootFSImporter.ProcdDigest)
	parsed, err := digest.Parse(procdDigest)
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != procdDigest {
		return legacyackmigration.TargetContract{}, fmt.Errorf("target procd digest must be canonical SHA-256")
	}
	return legacyackmigration.TargetContract{
		FormatGeneration: rootfsblock.DescriptorVersion,
		ProcdProtocol:    protocol, ProcdDigest: procdDigest,
	}, nil
}

func loadManagerRuntimeConfig(path string) (*config.ManagerConfig, error) {
	payload, err := readOwnerOnlyFile(path, maxManagerConfigBytes, "manager config")
	if err != nil {
		return nil, err
	}
	cfg := &config.ManagerConfig{}
	if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(payload))), cfg); err != nil {
		return nil, fmt.Errorf("decode manager config: %w", err)
	}
	return cfg, nil
}
