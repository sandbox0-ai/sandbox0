// Command rootfs-materializer-soak runs the opt-in, wall-clock acceptance
// gate for PostgreSQL-backed RootFS materialization against a real RustFS.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaterializer"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const (
	defaultSoakDuration        = 24 * time.Hour
	defaultGenerationCount     = 10_000
	defaultBurstCount          = 20
	defaultSampleInterval      = time.Minute
	defaultProxyListen         = "172.16.100.2:19001"
	defaultPhysicalByteLimit   = int64(512 << 20)
	defaultPhysicalFileLimit   = int64(4_096)
	defaultDatabaseGrowthLimit = int64(512 << 20)
)

type options struct {
	databaseURL         string
	rustFSEndpoint      string
	rustFSBucket        string
	accessKey           string
	secretKey           string
	proxyListen         string
	rustFSDataDir       string
	outputPath          string
	duration            time.Duration
	generations         int
	burstCount          int
	workerInterval      time.Duration
	sampleInterval      time.Duration
	minPackBytes        int64
	maxDelay            time.Duration
	physicalByteLimit   int64
	physicalFileLimit   int64
	databaseGrowthLimit int64
}

type runtimeState struct {
	pool        *pgxpool.Pool
	store       *sandboxstore.PGSandboxStore
	objects     objectstore.Store
	conditional objectstore.ConditionalStore
	worker      *rootfsmaterializer.Worker
}

type fixture struct {
	runID      string
	teamID     string
	filesystem *sandboxstore.RootFSFilesystem
	initial    *sandboxstore.RootFSGeneration
	base       rootfsblock.Descriptor
}

type counters struct {
	Generated            int `json:"generated"`
	Materialized         int `json:"materialized"`
	Batches              int `json:"batches"`
	ExpectedWorkerErrors int `json:"expected_worker_errors"`
}

type databaseSnapshot struct {
	CompositeGenerations    int64 `json:"composite_generations"`
	MaterializedGenerations int64 `json:"materialized_generations"`
	UploadingBatches        int64 `json:"uploading_batches"`
	PublishedBatches        int64 `json:"published_batches"`
	AbandonedBatches        int64 `json:"abandoned_batches"`
	Members                 int64 `json:"members"`
	CatalogObjects          int64 `json:"catalog_objects"`
	CatalogBytes            int64 `json:"catalog_bytes"`
	BatchObjects            int64 `json:"batch_objects"`
	GenerationObjects       int64 `json:"generation_objects"`
	MemberObjects           int64 `json:"member_objects"`
	DeletionQueue           int64 `json:"deletion_queue"`
	DatabaseBytes           int64 `json:"database_bytes"`
}

type objectSnapshot struct {
	Objects int64 `json:"objects"`
	Bytes   int64 `json:"bytes"`
}

type directorySnapshot struct {
	Files int64 `json:"files"`
	Bytes int64 `json:"bytes"`
}

type peakSnapshot struct {
	Database databaseSnapshot  `json:"database"`
	Objects  objectSnapshot    `json:"objects"`
	Physical directorySnapshot `json:"physical"`
}

type reportEvent struct {
	Type           string    `json:"type"`
	At             time.Time `json:"at"`
	ElapsedSeconds float64   `json:"elapsed_seconds"`
	Data           any       `json:"data,omitempty"`
}

type eventWriter struct {
	file    *os.File
	encoder *json.Encoder
	start   time.Time
}

type migrationLogger struct{}

func (migrationLogger) Printf(string, ...any)             {}
func (migrationLogger) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	opts, err := parseOptions()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	writer, err := newEventWriter(opts.outputPath)
	if err != nil {
		return err
	}
	defer writer.Close()
	start := time.Now().UTC()
	writer.start = start
	if err := writer.Write("configuration", map[string]any{
		"duration": opts.duration.String(), "generations": opts.generations,
		"burst_count": opts.burstCount, "worker_interval": opts.workerInterval.String(),
		"sample_interval": opts.sampleInterval.String(), "min_pack_bytes": opts.minPackBytes,
		"max_delay": opts.maxDelay.String(), "terminal_retention": rootfsmaterializer.DefaultTerminalRetention.String(),
		"uploading_stale":  rootfsmaterializer.DefaultUploadingStale.String(),
		"garbage_interval": rootfsmaterializer.DefaultGarbageInterval.String(),
		"proxy_listen":     opts.proxyListen,
	}); err != nil {
		return err
	}

	proxy, err := startOutageProxy(opts.proxyListen, opts.rustFSEndpoint)
	if err != nil {
		return fmt.Errorf("start RustFS fault proxy: %w", err)
	}
	defer proxy.Close(context.Background())
	proxyEndpoint := "http://" + opts.proxyListen

	runtime, err := openRuntime(ctx, opts, proxyEndpoint)
	if err != nil {
		return err
	}
	defer func() {
		if runtime != nil && runtime.pool != nil {
			runtime.pool.Close()
		}
	}()
	if err := sandboxstore.RunSandboxStoreMigrations(ctx, runtime.pool, migrationLogger{}); err != nil {
		return err
	}
	if err := requireEmptyDatabase(ctx, runtime.pool); err != nil {
		return err
	}
	if err := createAndRequireEmptyBucket(runtime.objects); err != nil {
		return err
	}
	baselineDB, err := snapshotDatabase(ctx, runtime.pool)
	if err != nil {
		return err
	}
	baselinePhysical, err := snapshotDirectory(opts.rustFSDataDir)
	if err != nil {
		return err
	}

	seeded, err := seedFixture(ctx, runtime, start)
	if err != nil {
		return err
	}
	schedule := buildGenerationSchedule(opts.generations, opts.duration, opts.maxDelay, opts.burstCount)
	if len(schedule) != opts.generations {
		return fmt.Errorf("generation schedule has %d entries, want %d", len(schedule), opts.generations)
	}
	if err := writer.Write("seeded", map[string]any{
		"run_id": seeded.runID, "team_id": seeded.teamID,
		"filesystem_id": seeded.filesystem.ID, "initial_generation_id": seeded.initial.ID,
		"database_baseline": baselineDB, "physical_baseline": baselinePhysical,
	}); err != nil {
		return err
	}

	state := counters{}
	peaks := peakSnapshot{Physical: directorySnapshot{Files: -1, Bytes: -1}}
	nextGeneration := 0
	nextWorker := start.Add(opts.workerInterval)
	nextSample := start.Add(opts.sampleInterval)
	faultAt := start.Add(opts.duration / 3)
	faultArmed := false
	faultComplete := false
	armObjects := objectSnapshot{}
	deadline := start.Add(opts.duration)
	drainDeadline := deadline.Add(opts.maxDelay + time.Minute)

	for {
		if err := ctx.Err(); err != nil {
			_ = writer.Write("interrupted", map[string]any{"error": err.Error(), "counters": state})
			return err
		}
		now := time.Now().UTC()
		elapsed := now.Sub(start)

		if nextGeneration < len(schedule) && schedule[nextGeneration].offset <= elapsed {
			end := nextGeneration + 1
			for end < len(schedule) && schedule[end].offset <= elapsed {
				end++
			}
			if err := insertScheduledGenerations(ctx, runtime.pool, seeded, schedule[nextGeneration:end], now); err != nil {
				return fmt.Errorf("insert scheduled generations: %w", err)
			}
			state.Generated += end - nextGeneration
			nextGeneration = end
		}

		if !faultArmed && now.Before(deadline) && !now.Before(faultAt) {
			armObjects, err = snapshotObjects(runtime.objects)
			if err != nil {
				return err
			}
			proxy.ArmAfterNextPut()
			faultArmed = true
			if err := writer.Write("rustfs_outage_armed", map[string]any{"objects": armObjects}); err != nil {
				return err
			}
		}

		if !now.Before(nextWorker) {
			result, runErr := runtime.worker.RunOnce(ctx)
			for !nextWorker.After(now) {
				nextWorker = nextWorker.Add(opts.workerInterval)
			}
			if runErr != nil {
				if !faultArmed || faultComplete || !proxy.Tripped() {
					return fmt.Errorf("unexpected materializer failure: %w", runErr)
				}
				// The failed pass durably created this exact uploading batch. Its
				// recovery retries must not count it again, but physical object
				// bounds must include it once.
				state.Batches += result.Batches
				state.ExpectedWorkerErrors++
				if err := writer.Write("rustfs_outage_observed", map[string]any{
					"result": result, "error": runErr.Error(), "proxy": proxy.Snapshot(),
				}); err != nil {
					return err
				}
				var recoveryResult rootfsmaterializer.Result
				runtime, recoveryResult, err = restartAndRecoverExactBatch(
					ctx, opts, proxyEndpoint, runtime, proxy, armObjects, writer,
				)
				if err != nil {
					return err
				}
				state.ExpectedWorkerErrors++
				state.Materialized += recoveryResult.Materialized
				faultComplete = true
				nextWorker = time.Now().UTC().Add(opts.workerInterval)
			} else {
				state.Materialized += result.Materialized
				state.Batches += result.Batches
			}
		}

		if !now.Before(nextSample) {
			db, objects, physical, sampleErr := collectSnapshots(ctx, runtime, opts.rustFSDataDir)
			if sampleErr != nil {
				return sampleErr
			}
			updatePeaks(&peaks, db, objects, physical)
			if err := writer.Write("sample", map[string]any{
				"counters": state, "database": db, "objects": objects,
				"physical": physical, "proxy": proxy.Snapshot(),
			}); err != nil {
				return err
			}
			for !nextSample.After(now) {
				nextSample = nextSample.Add(opts.sampleInterval)
			}
		}

		if !now.Before(deadline) {
			if nextGeneration != len(schedule) {
				return fmt.Errorf("wall-clock deadline reached with only %d of %d generations inserted",
					nextGeneration, len(schedule))
			}
			db, snapshotErr := snapshotDatabase(ctx, runtime.pool)
			if snapshotErr != nil {
				return snapshotErr
			}
			if db.CompositeGenerations == 0 && db.UploadingBatches == 0 {
				break
			}
			if now.After(drainDeadline) {
				return fmt.Errorf("materializer did not drain before %s: %+v", drainDeadline, db)
			}
		}

		wake := time.Now().Add(250 * time.Millisecond)
		for _, candidate := range []time.Time{nextWorker, nextSample, deadline} {
			if candidate.Before(wake) {
				wake = candidate
			}
		}
		if nextGeneration < len(schedule) {
			candidate := start.Add(schedule[nextGeneration].offset)
			if candidate.Before(wake) {
				wake = candidate
			}
		}
		timer := time.NewTimer(maxDuration(time.Until(wake), time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}

	finalDB, finalObjects, finalPhysical, err := collectSnapshots(ctx, runtime, opts.rustFSDataDir)
	if err != nil {
		return err
	}
	updatePeaks(&peaks, finalDB, finalObjects, finalPhysical)
	if err := verifySampleGenerations(ctx, runtime, seeded, opts.generations); err != nil {
		return err
	}
	violations := evaluateFinalBounds(opts, start, state, faultComplete, baselineDB,
		baselinePhysical, finalDB, finalObjects, finalPhysical)
	final := map[string]any{
		"passed": len(violations) == 0, "violations": violations,
		"counters": state, "database": finalDB, "objects": finalObjects,
		"physical": finalPhysical, "peaks": peaks, "proxy": proxy.Snapshot(),
		"database_growth_bytes": finalDB.DatabaseBytes - baselineDB.DatabaseBytes,
		"physical_growth_files": differenceIfKnown(finalPhysical.Files, baselinePhysical.Files),
		"physical_growth_bytes": differenceIfKnown(finalPhysical.Bytes, baselinePhysical.Bytes),
	}
	if err := writer.Write("final", final); err != nil {
		return err
	}
	if len(violations) > 0 {
		return fmt.Errorf("RootFS materializer soak failed: %s", strings.Join(violations, "; "))
	}
	return nil
}

func parseOptions() (options, error) {
	var opts options
	flag.StringVar(&opts.databaseURL, "database-url", os.Getenv("SANDBOX0_SOAK_DATABASE_URL"), "dedicated empty PostgreSQL database URL")
	flag.StringVar(&opts.rustFSEndpoint, "rustfs-endpoint", os.Getenv("SANDBOX0_RUSTFS_ENDPOINT"), "real RustFS S3 endpoint")
	flag.StringVar(&opts.rustFSBucket, "rustfs-bucket", envOr("SANDBOX0_RUSTFS_BUCKET", "sandbox0-materializer-soak"), "dedicated empty RustFS bucket")
	flag.StringVar(&opts.accessKey, "rustfs-access-key", os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"), "RustFS access key")
	flag.StringVar(&opts.secretKey, "rustfs-secret-key", os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY"), "RustFS secret key")
	flag.StringVar(&opts.proxyListen, "proxy-listen", defaultProxyListen, "fault proxy listen address")
	flag.StringVar(&opts.rustFSDataDir, "rustfs-data-dir", os.Getenv("SANDBOX0_RUSTFS_DATA_DIR"), "optional RustFS data directory for physical growth checks")
	flag.StringVar(&opts.outputPath, "output", "", "exclusive JSONL evidence output path")
	flag.DurationVar(&opts.duration, "duration", defaultSoakDuration, "actual wall-clock soak duration")
	flag.IntVar(&opts.generations, "generations", defaultGenerationCount, "generation lifecycle count")
	flag.IntVar(&opts.burstCount, "burst-count", defaultBurstCount, "number of deterministic write bursts")
	flag.DurationVar(&opts.workerInterval, "worker-interval", rootfsmaterializer.DefaultInterval, "materializer interval")
	flag.DurationVar(&opts.sampleInterval, "sample-interval", defaultSampleInterval, "evidence sample interval")
	flag.Int64Var(&opts.minPackBytes, "min-pack-bytes", rootfsmaterializer.DefaultMinPackBytes, "materializer minimum pack bytes")
	flag.DurationVar(&opts.maxDelay, "max-delay", rootfsmaterializer.DefaultMaxDelay, "materializer forced flush delay")
	flag.Int64Var(&opts.physicalByteLimit, "physical-byte-limit", defaultPhysicalByteLimit, "maximum RustFS physical byte growth")
	flag.Int64Var(&opts.physicalFileLimit, "physical-file-limit", defaultPhysicalFileLimit, "maximum RustFS physical file growth")
	flag.Int64Var(&opts.databaseGrowthLimit, "database-growth-limit", defaultDatabaseGrowthLimit, "maximum PostgreSQL database byte growth")
	flag.Parse()

	opts.databaseURL = strings.TrimSpace(opts.databaseURL)
	opts.rustFSEndpoint = strings.TrimRight(strings.TrimSpace(opts.rustFSEndpoint), "/")
	opts.rustFSBucket = strings.TrimSpace(opts.rustFSBucket)
	opts.proxyListen = strings.TrimSpace(opts.proxyListen)
	opts.outputPath = strings.TrimSpace(opts.outputPath)
	if opts.databaseURL == "" || opts.rustFSEndpoint == "" || opts.rustFSBucket == "" ||
		opts.proxyListen == "" || opts.outputPath == "" {
		return options{}, fmt.Errorf("database URL, RustFS endpoint/bucket, proxy listen address, and output are required")
	}
	if opts.duration < 10*time.Second || opts.duration > 7*24*time.Hour {
		return options{}, fmt.Errorf("duration must be between 10s and 7d")
	}
	if opts.generations < 10 || opts.generations > 100_000 {
		return options{}, fmt.Errorf("generations must be between 10 and 100000")
	}
	if opts.burstCount < 1 || opts.burstCount > opts.generations/2 {
		return options{}, fmt.Errorf("burst count must be between 1 and half the generation count")
	}
	if opts.workerInterval < 10*time.Millisecond || opts.sampleInterval < time.Second {
		return options{}, fmt.Errorf("worker interval must be at least 10ms and sample interval at least 1s")
	}
	if opts.maxDelay < time.Second || opts.maxDelay >= opts.duration {
		return options{}, fmt.Errorf("max delay must be at least 1s and shorter than the soak duration")
	}
	if opts.minPackBytes < rootfsblock.LogicalBlockSize || opts.minPackBytes > rootfsblock.DefaultPackBytes {
		return options{}, fmt.Errorf("minimum pack bytes must be between one block and one pack")
	}
	if opts.physicalByteLimit <= 0 || opts.physicalFileLimit <= 0 || opts.databaseGrowthLimit <= 0 {
		return options{}, fmt.Errorf("growth limits must be positive")
	}
	if _, err := url.ParseRequestURI(opts.rustFSEndpoint); err != nil {
		return options{}, fmt.Errorf("parse RustFS endpoint: %w", err)
	}
	return opts, nil
}

func openRuntime(ctx context.Context, opts options, endpoint string) (*runtimeState, error) {
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: opts.databaseURL, DefaultMaxConns: 8, DefaultMinConns: 1,
		RequirePrimary: true, MaxConnLifetime: 30 * time.Minute, MaxConnIdleTime: 5 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("open primary PostgreSQL pool: %w", err)
	}
	objects, err := objectstore.Create(objectstore.Config{
		Type: objectstore.TypeS3, Bucket: opts.rustFSBucket, Region: "us-east-1",
		Endpoint: endpoint, AccessKey: opts.accessKey, SecretKey: opts.secretKey,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("create RustFS client: %w", err)
	}
	conditional, ok := objects.(objectstore.ConditionalStore)
	if !ok || !objectstore.SupportsConditionalCreate(objects) {
		pool.Close()
		return nil, fmt.Errorf("RustFS client lacks conditional create")
	}
	store := sandboxstore.NewPGSandboxStore(pool)
	worker, err := rootfsmaterializer.New(rootfsmaterializer.Config{
		Store: store, Source: conditional,
		Publisher: rootfsblock.ObjectStorePublisher{Store: conditional},
		ScanLimit: rootfsmaterializer.DefaultScanLimit, Interval: opts.workerInterval,
		MinPackBytes: opts.minPackBytes, MaxDelay: opts.maxDelay,
		ForcedFlushesPerRun: rootfsmaterializer.DefaultForcedFlushes,
		GarbageInterval:     rootfsmaterializer.DefaultGarbageInterval,
		UploadingStale:      rootfsmaterializer.DefaultUploadingStale,
		TerminalRetention:   rootfsmaterializer.DefaultTerminalRetention,
	})
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &runtimeState{pool: pool, store: store, objects: objects, conditional: conditional, worker: worker}, nil
}

func requireEmptyDatabase(ctx context.Context, pool *pgxpool.Pool) error {
	var rows int64
	if err := pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM manager.sandboxes) +
			(SELECT COUNT(*) FROM manager.rootfs_generations) +
			(SELECT COUNT(*) FROM manager.rootfs_materialization_batches)
	`).Scan(&rows); err != nil {
		return fmt.Errorf("check dedicated soak database: %w", err)
	}
	if rows != 0 {
		return fmt.Errorf("dedicated soak database is not empty (%d durable rows)", rows)
	}
	return nil
}

func createAndRequireEmptyBucket(store objectstore.Store) error {
	if err := store.Create(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") {
		return fmt.Errorf("create dedicated RustFS bucket: %w", err)
	}
	snapshot, err := snapshotObjects(store)
	if err != nil {
		return err
	}
	if snapshot.Objects != 0 {
		return fmt.Errorf("dedicated RustFS bucket is not empty (%d objects)", snapshot.Objects)
	}
	return nil
}

func seedFixture(ctx context.Context, runtime *runtimeState, startedAt time.Time) (*fixture, error) {
	runDigest := digest.FromString(startedAt.Format(time.RFC3339Nano)).Encoded()[:16]
	result := &fixture{runID: "soak-" + runDigest, teamID: "soak-team-" + runDigest}
	publisher := rootfsblock.ObjectStorePublisher{Store: runtime.conditional}
	base, err := rootfsblock.BuildMaterializedGeneration(
		ctx, bytes.NewReader(make([]byte, rootfsblock.LogicalBlockSize)),
		rootfsblock.LogicalBlockSize, publisher,
		rootfsblock.BuildOptions{ObjectPrefix: "rootfs/soak/" + result.runID + "/base"},
	)
	if err != nil {
		return nil, fmt.Errorf("publish soak Base: %w", err)
	}
	result.base = base.Descriptor
	sandboxID := result.runID + "-sandbox"
	if err := runtime.store.UpsertSandbox(ctx, &sandboxstore.SandboxRecord{
		ID: sandboxID, TeamID: result.teamID, UserID: "soak-user",
		TemplateID: "soak-template", TemplateName: "soak-template",
		TemplateNamespace: "soak", RuntimeBackend: sandboxstore.SandboxRuntimeBackendNomad,
		DesiredState: sandboxstore.SandboxDesiredStatePaused, CreatedAt: startedAt,
	}); err != nil {
		return nil, err
	}
	artifactDigest := digest.FromBytes(base.Payload).String()
	sourceDigest := digest.FromString(result.runID + "-source").String()
	sourceRef := "registry.invalid/sandbox0-soak@" + sourceDigest
	artifact, err := runtime.store.PutReadyRootFSBaseArtifact(ctx, &sandboxstore.PutReadyRootFSBaseArtifactRequest{
		ArtifactDigest: artifactDigest, SourceOCIRef: sourceRef, SourceOCIDigest: sourceDigest,
		BaseBlockRoot: base.Descriptor.MappingRoot.RootDigest, FormatGeneration: 1,
		Platform:   sandboxstore.RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"},
		Descriptor: base.Payload,
	})
	if err != nil {
		return nil, err
	}
	filesystem, initial, err := runtime.store.EnsureInitialRootFSGeneration(ctx,
		&sandboxstore.EnsureInitialRootFSGenerationRequest{
			SandboxID: sandboxID, TeamID: result.teamID, SourceOCIRef: sourceRef,
			SourceOCIDigest: sourceDigest, BaseArtifactDigest: artifact.ArtifactDigest,
		})
	if err != nil {
		return nil, err
	}
	result.filesystem, result.initial = filesystem, initial
	return result, nil
}

type scheduledGeneration struct {
	offset time.Duration
	index  int
}

func buildGenerationSchedule(total int, duration, maxDelay time.Duration, burstCount int) []scheduledGeneration {
	window := duration - maxDelay - time.Second
	if window < duration/2 {
		window = duration * 3 / 4
	}
	burstTotal := total / 2
	baselineTotal := total - burstTotal
	result := make([]scheduledGeneration, 0, total)
	nextIndex := 0
	for index := 0; index < baselineTotal; index++ {
		offset := time.Duration(0)
		if baselineTotal > 1 {
			offset = time.Duration(int64(window) * int64(index) / int64(baselineTotal-1))
		}
		result = append(result, scheduledGeneration{offset: offset, index: nextIndex})
		nextIndex++
	}
	remaining := burstTotal
	for burst := 0; burst < burstCount; burst++ {
		size := remaining / (burstCount - burst)
		remaining -= size
		offset := time.Duration(int64(window) * int64(burst+1) / int64(burstCount+1))
		for range size {
			result = append(result, scheduledGeneration{offset: offset, index: nextIndex})
			nextIndex++
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].offset == result[right].offset {
			return result[left].index < result[right].index
		}
		return result[left].offset < result[right].offset
	})
	return result
}

func insertScheduledGenerations(
	ctx context.Context,
	pool *pgxpool.Pool,
	fixture *fixture,
	scheduled []scheduledGeneration,
	createdAt time.Time,
) error {
	rows := make([][]any, 0, len(scheduled))
	for _, item := range scheduled {
		data := bytes.Repeat([]byte{byte(item.index%251 + 1)}, rootfsblock.LogicalBlockSize)
		sealed, payload, err := rootfsblock.BuildCompositeGeneration(fixture.base,
			[]rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: data}})
		if err != nil {
			return err
		}
		rows = append(rows, []any{
			generationID(fixture.runID, item.index), fixture.filesystem.ID, fixture.initial.ID,
			fixture.initial.SourceOCIDigest, fixture.initial.BaseArtifactDigest,
			fixture.initial.BaseBlockRoot, sealed.MappingRoot.RootDigest, int64(item.index + 1),
			fixture.initial.FormatGeneration, sandboxstore.RootFSGenerationStateCompositeDurable,
			fixture.initial.LocatorVersion + 1, payload, createdAt,
		})
	}
	count, err := pool.CopyFrom(ctx, pgx.Identifier{"manager", "rootfs_generations"}, []string{
		"generation_id", "filesystem_id", "parent_generation_id", "source_oci_digest",
		"base_artifact_digest", "base_block_root", "current_block_head", "writer_epoch",
		"format_generation", "durability_state", "locator_version", "descriptor", "created_at",
	}, pgx.CopyFromRows(rows))
	if err != nil {
		return err
	}
	if count != int64(len(rows)) {
		return fmt.Errorf("inserted %d generations, want %d", count, len(rows))
	}
	return nil
}

func generationID(runID string, index int) string {
	return fmt.Sprintf("%s-generation-%05d", runID, index)
}

func restartAndRecoverExactBatch(
	ctx context.Context,
	opts options,
	proxyEndpoint string,
	runtime *runtimeState,
	proxy *outageProxy,
	objectsAtArm objectSnapshot,
	writer *eventWriter,
) (*runtimeState, rootfsmaterializer.Result, error) {
	pendingBefore, err := runtime.store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
	if err != nil || pendingBefore == nil {
		return runtime, rootfsmaterializer.Result{}, fmt.Errorf("read exact outage batch: %w", err)
	}
	runtime.pool.Close()
	restarted, err := openRuntime(ctx, opts, proxyEndpoint)
	if err != nil {
		return runtime, rootfsmaterializer.Result{}, fmt.Errorf("reopen materializer after PostgreSQL connection interruption: %w", err)
	}
	runtime = restarted
	retryResult, retryErr := runtime.worker.RunOnce(ctx)
	if retryErr == nil {
		return runtime, rootfsmaterializer.Result{}, fmt.Errorf("exact batch unexpectedly succeeded while RustFS remained unavailable")
	}
	pendingAfter, err := runtime.store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
	if err != nil || pendingAfter == nil || pendingAfter.BatchID != pendingBefore.BatchID {
		return runtime, rootfsmaterializer.Result{}, fmt.Errorf("restart changed exact uploading batch %s", pendingBefore.BatchID)
	}
	if err := writer.Write("materializer_restarted_during_outage", map[string]any{
		"batch_id": pendingBefore.BatchID, "members": len(pendingBefore.Members),
		"retry_result": retryResult, "retry_error": retryErr.Error(),
	}); err != nil {
		return runtime, rootfsmaterializer.Result{}, err
	}
	proxy.Recover()
	recoveryResult, err := runtime.worker.RunOnce(ctx)
	if err != nil {
		return runtime, rootfsmaterializer.Result{}, fmt.Errorf("resume exact batch after RustFS recovery: %w", err)
	}
	pendingFinal, err := runtime.store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
	if err != nil || pendingFinal != nil {
		return runtime, rootfsmaterializer.Result{}, fmt.Errorf("exact batch remained uploading after recovery: %w", err)
	}
	objectsAfter, err := snapshotObjects(runtime.objects)
	if err != nil {
		return runtime, rootfsmaterializer.Result{}, err
	}
	if growth := objectsAfter.Objects - objectsAtArm.Objects; growth < 1 || growth > 2 {
		return runtime, rootfsmaterializer.Result{}, fmt.Errorf("exact outage batch created %d object keys, want 1..2", growth)
	}
	if err := writer.Write("rustfs_outage_recovered", map[string]any{
		"batch_id": pendingBefore.BatchID, "result": recoveryResult,
		"objects_before": objectsAtArm, "objects_after": objectsAfter,
		"proxy": proxy.Snapshot(),
	}); err != nil {
		return runtime, rootfsmaterializer.Result{}, err
	}
	return runtime, recoveryResult, nil
}

func snapshotDatabase(ctx context.Context, pool *pgxpool.Pool) (databaseSnapshot, error) {
	var result databaseSnapshot
	err := pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM manager.rootfs_generations WHERE durability_state = 'composite_durable'),
			(SELECT COUNT(*) FROM manager.rootfs_generations WHERE durability_state = 's3_materialized'),
			(SELECT COUNT(*) FROM manager.rootfs_materialization_batches WHERE state = 'uploading'),
			(SELECT COUNT(*) FROM manager.rootfs_materialization_batches WHERE state = 'published'),
			(SELECT COUNT(*) FROM manager.rootfs_materialization_batches WHERE state = 'abandoned'),
			(SELECT COUNT(*) FROM manager.rootfs_materialization_members),
			(SELECT COUNT(*) FROM manager.rootfs_materialization_objects),
			(SELECT COALESCE(SUM(object_size), 0) FROM manager.rootfs_materialization_objects),
			(SELECT COUNT(*) FROM manager.rootfs_materialization_batch_objects),
			(SELECT COUNT(*) FROM manager.rootfs_generation_materialization_objects),
			(SELECT COUNT(*) FROM manager.rootfs_materialization_member_objects),
			(SELECT COUNT(*) FROM manager.rootfs_object_deletions),
			pg_database_size(current_database())
	`).Scan(
		&result.CompositeGenerations, &result.MaterializedGenerations,
		&result.UploadingBatches, &result.PublishedBatches, &result.AbandonedBatches,
		&result.Members, &result.CatalogObjects, &result.CatalogBytes,
		&result.BatchObjects, &result.GenerationObjects, &result.MemberObjects,
		&result.DeletionQueue, &result.DatabaseBytes,
	)
	if err != nil {
		return databaseSnapshot{}, fmt.Errorf("snapshot soak PostgreSQL state: %w", err)
	}
	return result, nil
}

func snapshotObjects(store objectstore.Store) (objectSnapshot, error) {
	var result objectSnapshot
	var token string
	for {
		items, truncated, next, err := store.List("", "", token, "", 1_000)
		if err != nil {
			return objectSnapshot{}, fmt.Errorf("list soak RustFS objects: %w", err)
		}
		for _, item := range items {
			if !item.IsPrefix {
				result.Objects++
				result.Bytes += item.Size
			}
		}
		if !truncated {
			return result, nil
		}
		if next == "" {
			return objectSnapshot{}, fmt.Errorf("RustFS truncated listing has no continuation token")
		}
		token = next
	}
}

func snapshotDirectory(root string) (directorySnapshot, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return directorySnapshot{Files: -1, Bytes: -1}, nil
	}
	var result directorySnapshot
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		result.Files++
		result.Bytes += info.Size()
		return nil
	})
	if err != nil {
		return directorySnapshot{}, fmt.Errorf("measure RustFS data directory: %w", err)
	}
	return result, nil
}

func collectSnapshots(
	ctx context.Context,
	runtime *runtimeState,
	dataDir string,
) (databaseSnapshot, objectSnapshot, directorySnapshot, error) {
	db, err := snapshotDatabase(ctx, runtime.pool)
	if err != nil {
		return databaseSnapshot{}, objectSnapshot{}, directorySnapshot{}, err
	}
	objects, err := snapshotObjects(runtime.objects)
	if err != nil {
		return databaseSnapshot{}, objectSnapshot{}, directorySnapshot{}, err
	}
	physical, err := snapshotDirectory(dataDir)
	return db, objects, physical, err
}

func updatePeaks(peaks *peakSnapshot, db databaseSnapshot, objects objectSnapshot, physical directorySnapshot) {
	peaks.Database.CompositeGenerations = max(peaks.Database.CompositeGenerations, db.CompositeGenerations)
	peaks.Database.MaterializedGenerations = max(peaks.Database.MaterializedGenerations, db.MaterializedGenerations)
	peaks.Database.UploadingBatches = max(peaks.Database.UploadingBatches, db.UploadingBatches)
	peaks.Database.PublishedBatches = max(peaks.Database.PublishedBatches, db.PublishedBatches)
	peaks.Database.AbandonedBatches = max(peaks.Database.AbandonedBatches, db.AbandonedBatches)
	peaks.Database.Members = max(peaks.Database.Members, db.Members)
	peaks.Database.CatalogObjects = max(peaks.Database.CatalogObjects, db.CatalogObjects)
	peaks.Database.CatalogBytes = max(peaks.Database.CatalogBytes, db.CatalogBytes)
	peaks.Database.BatchObjects = max(peaks.Database.BatchObjects, db.BatchObjects)
	peaks.Database.GenerationObjects = max(peaks.Database.GenerationObjects, db.GenerationObjects)
	peaks.Database.MemberObjects = max(peaks.Database.MemberObjects, db.MemberObjects)
	peaks.Database.DeletionQueue = max(peaks.Database.DeletionQueue, db.DeletionQueue)
	peaks.Database.DatabaseBytes = max(peaks.Database.DatabaseBytes, db.DatabaseBytes)
	peaks.Objects.Objects = max(peaks.Objects.Objects, objects.Objects)
	peaks.Objects.Bytes = max(peaks.Objects.Bytes, objects.Bytes)
	if physical.Files >= 0 {
		peaks.Physical.Files = max(peaks.Physical.Files, physical.Files)
		peaks.Physical.Bytes = max(peaks.Physical.Bytes, physical.Bytes)
	}
}

func verifySampleGenerations(
	ctx context.Context,
	runtime *runtimeState,
	fixture *fixture,
	total int,
) error {
	for _, index := range []int{0, total / 2, total - 1} {
		generation, err := runtime.store.GetRootFSGeneration(ctx, generationID(fixture.runID, index))
		if err != nil {
			return fmt.Errorf("load sample generation %d: %w", index, err)
		}
		if generation.DurabilityState != sandboxstore.RootFSGenerationStateS3Materialized {
			return fmt.Errorf("sample generation %d is %s", index, generation.DurabilityState)
		}
		descriptor, err := rootfsblock.DecodeDescriptor(generation.Descriptor)
		if err != nil {
			return err
		}
		reader, err := rootfsblock.NewReader(runtime.conditional, descriptor, rootfsblock.DefaultReadCacheBytes)
		if err != nil {
			return err
		}
		actual := make([]byte, rootfsblock.LogicalBlockSize)
		if _, err := reader.ReadAt(actual, 0); err != nil {
			return fmt.Errorf("read sample generation %d: %w", index, err)
		}
		expected := bytes.Repeat([]byte{byte(index%251 + 1)}, rootfsblock.LogicalBlockSize)
		if !bytes.Equal(actual, expected) {
			return fmt.Errorf("sample generation %d has incorrect bytes", index)
		}
	}
	return nil
}

func evaluateFinalBounds(
	opts options,
	startedAt time.Time,
	state counters,
	faultComplete bool,
	baselineDB databaseSnapshot,
	baselinePhysical directorySnapshot,
	finalDB databaseSnapshot,
	finalObjects objectSnapshot,
	finalPhysical directorySnapshot,
) []string {
	var result []string
	if time.Since(startedAt) < opts.duration {
		result = append(result, "actual wall-clock duration was shorter than configured")
	}
	if state.Generated != opts.generations || state.Materialized != opts.generations {
		result = append(result, fmt.Sprintf("generated/materialized=%d/%d, want %d/%d",
			state.Generated, state.Materialized, opts.generations, opts.generations))
	}
	if !faultComplete || state.ExpectedWorkerErrors != 2 {
		result = append(result, fmt.Sprintf("fault recovery complete/errors=%t/%d, want true/2",
			faultComplete, state.ExpectedWorkerErrors))
	}
	if finalDB.CompositeGenerations != 0 || finalDB.UploadingBatches != 0 || finalDB.AbandonedBatches != 0 {
		result = append(result, "PostgreSQL retains nonterminal materialization state")
	}
	if finalDB.MaterializedGenerations != int64(opts.generations+1) {
		result = append(result, fmt.Sprintf("materialized generation rows=%d, want %d",
			finalDB.MaterializedGenerations, opts.generations+1))
	}
	if finalDB.DeletionQueue != 0 {
		result = append(result, fmt.Sprintf("unexpected object deletion queue rows=%d", finalDB.DeletionQueue))
	}
	if finalDB.CatalogObjects != finalObjects.Objects-1 {
		result = append(result, fmt.Sprintf("catalog/RustFS objects=%d/%d do not differ by the one Base object",
			finalDB.CatalogObjects, finalObjects.Objects))
	}
	if finalObjects.Objects > int64(1+2*state.Batches) {
		result = append(result, fmt.Sprintf("RustFS object count=%d exceeds batch bound=%d",
			finalObjects.Objects, 1+2*state.Batches))
	}
	batchBound := int(opts.duration/opts.maxDelay) + 3
	if state.Batches > batchBound {
		result = append(result, fmt.Sprintf("materialization batches=%d exceed forced-flush bound=%d",
			state.Batches, batchBound))
	}
	if growth := finalDB.DatabaseBytes - baselineDB.DatabaseBytes; growth > opts.databaseGrowthLimit {
		result = append(result, fmt.Sprintf("PostgreSQL growth=%d exceeds %d", growth, opts.databaseGrowthLimit))
	}
	if finalPhysical.Files >= 0 && baselinePhysical.Files >= 0 {
		if growth := finalPhysical.Files - baselinePhysical.Files; growth > opts.physicalFileLimit {
			result = append(result, fmt.Sprintf("RustFS physical file growth=%d exceeds %d", growth, opts.physicalFileLimit))
		}
		if growth := finalPhysical.Bytes - baselinePhysical.Bytes; growth > opts.physicalByteLimit {
			result = append(result, fmt.Sprintf("RustFS physical byte growth=%d exceeds %d", growth, opts.physicalByteLimit))
		}
	}
	return result
}

func differenceIfKnown(value, baseline int64) int64 {
	if value < 0 || baseline < 0 {
		return -1
	}
	return value - baseline
}

func newEventWriter(path string) (*eventWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create exclusive soak evidence file: %w", err)
	}
	return &eventWriter{file: file, encoder: json.NewEncoder(file)}, nil
}

func (w *eventWriter) Write(eventType string, data any) error {
	event := reportEvent{Type: eventType, At: time.Now().UTC(), Data: data}
	if !w.start.IsZero() {
		event.ElapsedSeconds = time.Since(w.start).Seconds()
	}
	if err := w.encoder.Encode(event); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *eventWriter) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	return w.file.Close()
}

type proxySnapshot struct {
	Armed         bool             `json:"armed"`
	Tripped       bool             `json:"tripped"`
	ForwardedPUTs int64            `json:"forwarded_puts"`
	Methods       map[string]int64 `json:"methods"`
	Statuses      map[int]int64    `json:"statuses"`
}

type outageProxy struct {
	server        *http.Server
	listener      net.Listener
	upstream      *httputil.ReverseProxy
	mu            sync.Mutex
	armed         bool
	tripped       bool
	forwardedPUTs int64
	methods       map[string]int64
	statuses      map[int]int64
}

func startOutageProxy(listenAddress, upstreamEndpoint string) (*outageProxy, error) {
	target, err := url.Parse(upstreamEndpoint)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return nil, err
	}
	result := &outageProxy{
		listener: listener, upstream: httputil.NewSingleHostReverseProxy(target),
		methods: make(map[string]int64), statuses: make(map[int]int64),
	}
	result.server = &http.Server{Handler: result, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = result.server.Serve(listener) }()
	return result, nil
}

func (p *outageProxy) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	p.mu.Lock()
	p.methods[request.Method]++
	tripped := p.tripped
	armedPut := p.armed && request.Method == http.MethodPut
	p.mu.Unlock()
	if tripped {
		p.recordStatus(http.StatusServiceUnavailable)
		response.Header().Set("Retry-After", "0")
		http.Error(response, "injected RustFS outage", http.StatusServiceUnavailable)
		return
	}
	recorder := &statusRecorder{ResponseWriter: response, status: http.StatusOK}
	p.upstream.ServeHTTP(recorder, request)
	p.recordStatus(recorder.status)
	if armedPut {
		p.mu.Lock()
		if p.armed && !p.tripped {
			p.forwardedPUTs++
			p.tripped = true
		}
		p.mu.Unlock()
	}
}

func (p *outageProxy) recordStatus(status int) {
	p.mu.Lock()
	p.statuses[status]++
	p.mu.Unlock()
}

func (p *outageProxy) ArmAfterNextPut() {
	p.mu.Lock()
	p.armed = true
	p.tripped = false
	p.mu.Unlock()
}

func (p *outageProxy) Tripped() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tripped
}

func (p *outageProxy) Recover() {
	p.mu.Lock()
	p.armed = false
	p.tripped = false
	p.mu.Unlock()
}

func (p *outageProxy) Snapshot() proxySnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := proxySnapshot{
		Armed: p.armed, Tripped: p.tripped, ForwardedPUTs: p.forwardedPUTs,
		Methods: make(map[string]int64, len(p.methods)), Statuses: make(map[int]int64, len(p.statuses)),
	}
	for key, value := range p.methods {
		result.Methods[key] = value
	}
	for key, value := range p.statuses {
		result.Statuses[key] = value
	}
	return result
}

func (p *outageProxy) Close(ctx context.Context) error {
	if p == nil || p.server == nil {
		return nil
	}
	return p.server.Shutdown(ctx)
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func maxDuration(left, right time.Duration) time.Duration {
	if left > right {
		return left
	}
	return right
}
