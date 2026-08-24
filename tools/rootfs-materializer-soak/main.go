// Command rootfs-materializer-soak runs the opt-in, active-time acceptance
// gate for PostgreSQL-backed RootFS materialization against a real RustFS.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/internal/soakstate"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/rootfsmaterializer"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	templatemigrations "github.com/sandbox0-ai/sandbox0/pkg/template/migrations"
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
	mode                soakstate.Mode
}

type runtimeState struct {
	pool        *pgxpool.Pool
	store       *sandboxstore.PGSandboxStore
	objects     objectstore.Store
	conditional objectstore.ContextConditionalStore
	worker      *rootfsmaterializer.Worker
}

type zeroReaderAt struct {
	size int64
}

func (r zeroReaderAt) ReadAt(payload []byte, offset int64) (int, error) {
	if offset < 0 || offset >= r.size {
		return 0, io.EOF
	}
	remaining := r.size - offset
	count := len(payload)
	if int64(count) > remaining {
		count = int(remaining)
	}
	clear(payload[:count])
	if count != len(payload) {
		return count, io.EOF
	}
	return count, nil
}

type fixture struct {
	runID      string
	teamID     string
	filesystem *sandboxstore.RootFSFilesystem
	initial    *sandboxstore.RootFSGeneration
	base       rootfsblock.Descriptor
}

type fixtureCheckpoint struct {
	RunID      string                        `json:"run_id"`
	TeamID     string                        `json:"team_id"`
	Filesystem sandboxstore.RootFSFilesystem `json:"filesystem"`
	Initial    sandboxstore.RootFSGeneration `json:"initial"`
	Base       rootfsblock.Descriptor        `json:"base"`
}

type counters struct {
	Generated            int `json:"generated"`
	Materialized         int `json:"materialized"`
	RetainedBatches      int `json:"retained_batches"`
	ExpectedWorkerErrors int `json:"expected_worker_errors"`
}

type acceptanceBounds struct {
	MaxBatches int   `json:"max_batches"`
	MaxObjects int64 `json:"max_objects"`
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

const materializerSoakStateVersion = 1

const (
	materializerSoakPhasePreflight = "preflight"
	materializerSoakPhaseSeeding   = "seeding"
	materializerSoakPhaseActive    = "active"
	materializerSoakPhasePassed    = "passed"
	materializerSoakPhaseFailed    = "failed"
)

const (
	materializerFaultPending       = "pending"
	materializerFaultArmed         = "armed"
	materializerFaultTripped       = "tripped"
	materializerFaultRetryObserved = "retry_observed"
	materializerFaultRecovered     = "recovered"
)

type soakConfiguration struct {
	Duration            string `json:"duration"`
	Generations         int    `json:"generations"`
	BurstCount          int    `json:"burst_count"`
	WorkerInterval      string `json:"worker_interval"`
	SampleInterval      string `json:"sample_interval"`
	MinPackBytes        int64  `json:"min_pack_bytes"`
	MaxDelay            string `json:"max_delay"`
	PhysicalByteLimit   int64  `json:"physical_byte_limit"`
	PhysicalFileLimit   int64  `json:"physical_file_limit"`
	DatabaseGrowthLimit int64  `json:"database_growth_limit"`
	TerminalRetention   string `json:"terminal_retention"`
	UploadingStale      string `json:"uploading_stale"`
	GarbageInterval     string `json:"garbage_interval"`
	RustFSEndpoint      string `json:"rustfs_endpoint"`
	RustFSBucket        string `json:"rustfs_bucket"`
	RustFSDataDir       string `json:"rustfs_data_dir,omitempty"`
	ProxyListen         string `json:"proxy_listen"`
}

type soakCheckpoint struct {
	Version              int                `json:"version"`
	Phase                string             `json:"phase"`
	ActiveElapsedNS      int64              `json:"active_elapsed_ns"`
	Fixture              *fixtureCheckpoint `json:"fixture,omitempty"`
	DatabaseBaseline     databaseSnapshot   `json:"database_baseline"`
	PhysicalBaseline     directorySnapshot  `json:"physical_baseline"`
	Peaks                peakSnapshot       `json:"peaks"`
	NextGeneration       int                `json:"next_generation"`
	FaultPhase           string             `json:"fault_phase"`
	FaultObjects         objectSnapshot     `json:"fault_objects"`
	FaultBatchID         string             `json:"fault_batch_id,omitempty"`
	ExpectedWorkerErrors int                `json:"expected_worker_errors"`
	FinalViolations      []string           `json:"final_violations,omitempty"`
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
	configuration := materializerSoakConfiguration(opts)
	initial := soakCheckpoint{
		Version: materializerSoakStateVersion, Phase: materializerSoakPhasePreflight,
		FaultPhase:       materializerFaultPending,
		Peaks:            peakSnapshot{Physical: directorySnapshot{Files: -1, Bytes: -1}},
		PhysicalBaseline: directorySnapshot{Files: -1, Bytes: -1},
	}
	writer, err := soakstate.Open(soakstate.OpenOptions{
		Path: opts.outputPath, Mode: opts.mode, Config: configuration, Initial: initial,
	})
	if err != nil {
		return err
	}
	defer writer.Close()
	state := initial
	if writer.ResumeInfo().Resumed {
		if err := writer.DecodeCheckpoint(&state); err != nil {
			return err
		}
	}
	if err := validateMaterializerSoakCheckpoint(state, opts); err != nil {
		return err
	}
	if state.Phase == materializerSoakPhasePassed {
		return nil
	}
	if state.Phase == materializerSoakPhaseFailed {
		return fmt.Errorf("RootFS materializer soak previously failed: %s", strings.Join(state.FinalViolations, "; "))
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
	if err := migrate.Up(ctx, runtime.pool, ".",
		migrate.WithBaseFS(templatemigrations.FS),
		migrate.WithLogger(migrationLogger{}),
		migrate.WithSchema("scheduler"),
	); err != nil {
		return fmt.Errorf("run scheduler template migrations: %w", err)
	}
	if err := egressauthstore.RunMigrations(ctx, runtime.pool, migrationLogger{}); err != nil {
		return err
	}
	if err := sandboxstore.RunSandboxStoreMigrations(ctx, runtime.pool, migrationLogger{}); err != nil {
		return err
	}
	if state.Phase == materializerSoakPhasePreflight {
		if err := requireEmptyDatabase(ctx, runtime.pool); err != nil {
			return err
		}
		if err := ensureMaterializerDatabaseIdentity(
			ctx, runtime.pool, writer.RunID(), writer.ConfigSHA256(), true,
		); err != nil {
			return err
		}
		if err := ensureMaterializerObjectIdentity(
			ctx, runtime.objects, writer.RunID(), writer.ConfigSHA256(), writer.ExecutableSHA256(), true,
		); err != nil {
			return err
		}
		state.DatabaseBaseline, err = snapshotDatabase(ctx, runtime.pool)
		if err != nil {
			return err
		}
		state.PhysicalBaseline, err = snapshotDirectory(opts.rustFSDataDir)
		if err != nil {
			return err
		}
		state.Phase = materializerSoakPhaseSeeding
		if err := writer.Commit("preflight_complete", 0, map[string]any{
			"database_baseline": state.DatabaseBaseline,
			"physical_baseline": state.PhysicalBaseline,
		}, state); err != nil {
			return err
		}
	}
	if err := ensureMaterializerDatabaseIdentity(
		ctx, runtime.pool, writer.RunID(), writer.ConfigSHA256(), false,
	); err != nil {
		return err
	}
	if err := ensureMaterializerObjectIdentity(
		ctx, runtime.objects, writer.RunID(), writer.ConfigSHA256(), writer.ExecutableSHA256(),
		false,
	); err != nil {
		return err
	}
	if state.Phase == materializerSoakPhaseSeeding {
		seeded, seedErr := seedFixture(ctx, runtime, writer.RunID(), time.Now().UTC())
		if seedErr != nil {
			return seedErr
		}
		state.Fixture = checkpointFixture(seeded)
		state.Phase = materializerSoakPhaseActive
		if err := writer.Commit("seeded", 0, map[string]any{
			"run_id": seeded.runID, "team_id": seeded.teamID,
			"filesystem_id": seeded.filesystem.ID, "initial_generation_id": seeded.initial.ID,
		}, state); err != nil {
			return err
		}
	}
	seeded, err := restoreAndVerifyFixture(ctx, runtime, writer.RunID(), state.Fixture)
	if err != nil {
		return err
	}
	schedule := buildGenerationSchedule(opts.generations, opts.duration, opts.maxDelay, opts.burstCount)
	if len(schedule) != opts.generations {
		return fmt.Errorf("generation schedule has %d entries, want %d", len(schedule), opts.generations)
	}

	segmentStarted := time.Now()
	activeElapsed := func() time.Duration {
		return time.Duration(state.ActiveElapsedNS) + time.Since(segmentStarted)
	}
	commit := func(eventType string, data any) error {
		now := time.Now()
		state.ActiveElapsedNS += now.Sub(segmentStarted).Nanoseconds()
		segmentStarted = now
		return writer.Commit(eventType, time.Duration(state.ActiveElapsedNS), data, state)
	}
	if writer.ResumeInfo().Resumed {
		if err := commit("resumed", writer.ResumeInfo()); err != nil {
			return err
		}
	}
	nextWorker := nextSoakBoundary(activeElapsed(), opts.workerInterval)
	nextSample := nextSoakBoundary(activeElapsed(), opts.sampleInterval)
	checkpointInterval := materializerSoakCheckpointInterval(opts.duration)
	nextCheckpoint := nextSoakBoundary(activeElapsed(), checkpointInterval)

	for {
		if err := ctx.Err(); err != nil {
			_ = commit("interrupted", map[string]any{"error": err.Error()})
			return err
		}
		now := time.Now().UTC()
		elapsed := activeElapsed()

		if state.NextGeneration < len(schedule) && schedule[state.NextGeneration].offset <= elapsed {
			end := state.NextGeneration + 1
			for end < len(schedule) && schedule[end].offset <= elapsed {
				end++
			}
			if err := insertScheduledGenerations(ctx, runtime.pool, seeded, schedule[state.NextGeneration:end], now); err != nil {
				return fmt.Errorf("insert scheduled generations: %w", err)
			}
			state.NextGeneration = end
			if err := commit("generated", map[string]any{"next_generation": end}); err != nil {
				return err
			}
		}

		if state.FaultPhase == materializerFaultPending && elapsed < opts.duration && elapsed >= opts.duration/3 {
			state.FaultObjects, err = snapshotObjects(runtime.objects)
			if err != nil {
				return err
			}
			state.FaultPhase = materializerFaultArmed
			if err := commit("rustfs_outage_armed", map[string]any{"objects": state.FaultObjects}); err != nil {
				return err
			}
			proxy.ArmAfterNextPut()
		}

		handled, faultErr := progressMaterializerFault(
			ctx, &runtime, opts, proxyEndpoint, proxy, &state, commit,
		)
		if faultErr != nil {
			return faultErr
		}
		if handled {
			continue
		}

		if elapsed >= nextWorker {
			result, runErr := runtime.worker.RunOnce(ctx)
			for elapsed >= nextWorker {
				nextWorker += opts.workerInterval
			}
			if runErr != nil {
				if state.FaultPhase != materializerFaultArmed || !proxy.Tripped() {
					return fmt.Errorf("unexpected materializer failure: %w", runErr)
				}
				pending, pendingErr := runtime.store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
				if pendingErr != nil || pending == nil {
					return fmt.Errorf("read exact outage batch after failure: %w", pendingErr)
				}
				state.FaultBatchID = pending.BatchID
				state.FaultPhase = materializerFaultTripped
				state.ExpectedWorkerErrors = 1
				if err := commit("rustfs_outage_observed", map[string]any{
					"result": result, "error": runErr.Error(), "proxy": proxy.Snapshot(),
					"batch_id": pending.BatchID, "members": len(pending.Members),
				}); err != nil {
					return err
				}
			} else {
				if err := commit("worker", result); err != nil {
					return err
				}
			}
		}

		if elapsed >= nextSample {
			db, objects, physical, sampleErr := collectSnapshots(ctx, runtime, opts.rustFSDataDir)
			if sampleErr != nil {
				return sampleErr
			}
			updatePeaks(&state.Peaks, db, objects, physical)
			if err := commit("sample", map[string]any{
				"counters": materializerCounters(state, db), "database": db, "objects": objects,
				"physical": physical, "proxy": proxy.Snapshot(),
			}); err != nil {
				return err
			}
			for elapsed >= nextSample {
				nextSample += opts.sampleInterval
			}
		}

		if elapsed >= nextCheckpoint {
			if err := commit("checkpoint", map[string]any{"proxy": proxy.Snapshot()}); err != nil {
				return err
			}
			for elapsed >= nextCheckpoint {
				nextCheckpoint += checkpointInterval
			}
		}

		if elapsed >= opts.duration {
			if state.NextGeneration != len(schedule) {
				return fmt.Errorf("active-time deadline reached with only %d of %d generations inserted",
					state.NextGeneration, len(schedule))
			}
			db, snapshotErr := snapshotDatabase(ctx, runtime.pool)
			if snapshotErr != nil {
				return snapshotErr
			}
			if db.CompositeGenerations == 0 && db.UploadingBatches == 0 {
				break
			}
			if elapsed > opts.duration+opts.maxDelay+time.Minute {
				return fmt.Errorf("materializer did not drain within the active deadline: %+v", db)
			}
		}

		timer := time.NewTimer(100 * time.Millisecond)
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
	updatePeaks(&state.Peaks, finalDB, finalObjects, finalPhysical)
	if err := verifySampleGenerations(ctx, runtime, seeded, opts.generations); err != nil {
		return err
	}
	finalCounters := materializerCounters(state, finalDB)
	violations := evaluateFinalBounds(opts, activeElapsed(), finalCounters,
		state.FaultPhase == materializerFaultRecovered, state.DatabaseBaseline,
		state.PhysicalBaseline, finalDB, finalObjects, finalPhysical)
	final := map[string]any{
		"passed": len(violations) == 0, "violations": violations,
		"counters": finalCounters, "database": finalDB, "objects": finalObjects,
		"bounds":   materializerAcceptanceBounds(opts),
		"physical": finalPhysical, "peaks": state.Peaks, "proxy": proxy.Snapshot(),
		"database_growth_bytes": finalDB.DatabaseBytes - state.DatabaseBaseline.DatabaseBytes,
		"physical_growth_files": differenceIfKnown(finalPhysical.Files, state.PhysicalBaseline.Files),
		"physical_growth_bytes": differenceIfKnown(finalPhysical.Bytes, state.PhysicalBaseline.Bytes),
	}
	state.FinalViolations = violations
	if len(violations) == 0 {
		state.Phase = materializerSoakPhasePassed
	} else {
		state.Phase = materializerSoakPhaseFailed
	}
	if err := commit("final", final); err != nil {
		return err
	}
	if len(violations) > 0 {
		return fmt.Errorf("RootFS materializer soak failed: %s", strings.Join(violations, "; "))
	}
	return nil
}

func parseOptions() (options, error) {
	var opts options
	var rawMode string
	flag.StringVar(&opts.databaseURL, "database-url", os.Getenv("SANDBOX0_SOAK_DATABASE_URL"), "dedicated empty PostgreSQL database URL")
	flag.StringVar(&opts.rustFSEndpoint, "rustfs-endpoint", os.Getenv("SANDBOX0_RUSTFS_ENDPOINT"), "real RustFS S3 endpoint")
	flag.StringVar(&opts.rustFSBucket, "rustfs-bucket", envOr("SANDBOX0_RUSTFS_BUCKET", "sandbox0-materializer-soak"), "dedicated empty RustFS bucket")
	flag.StringVar(&opts.accessKey, "rustfs-access-key", os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"), "RustFS access key")
	flag.StringVar(&opts.secretKey, "rustfs-secret-key", os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY"), "RustFS secret key")
	flag.StringVar(&opts.proxyListen, "proxy-listen", defaultProxyListen, "fault proxy listen address")
	flag.StringVar(&opts.rustFSDataDir, "rustfs-data-dir", os.Getenv("SANDBOX0_RUSTFS_DATA_DIR"), "dedicated RustFS data directory for physical growth checks")
	flag.StringVar(&opts.outputPath, "output", "", "durable JSONL evidence and checkpoint path")
	flag.StringVar(&rawMode, "mode", envOr("SANDBOX0_SOAK_MODE", string(soakstate.ModeCreate)), "state mode: create, resume, or auto")
	flag.DurationVar(&opts.duration, "duration", defaultSoakDuration, "required active soak duration")
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
	opts.accessKey = strings.TrimSpace(opts.accessKey)
	opts.secretKey = strings.TrimSpace(opts.secretKey)
	opts.proxyListen = strings.TrimSpace(opts.proxyListen)
	rawRustFSDataDir := strings.TrimSpace(opts.rustFSDataDir)
	opts.rustFSDataDir = filepath.Clean(rawRustFSDataDir)
	opts.outputPath = strings.TrimSpace(opts.outputPath)
	if opts.databaseURL == "" || opts.rustFSEndpoint == "" || opts.rustFSBucket == "" ||
		opts.accessKey == "" || opts.secretKey == "" || opts.proxyListen == "" ||
		opts.rustFSDataDir == "." || opts.outputPath == "" {
		return options{}, fmt.Errorf("database URL, RustFS endpoint/bucket/credentials/data directory, proxy listen address, and output are required")
	}
	if !filepath.IsAbs(opts.rustFSDataDir) || opts.rustFSDataDir == string(filepath.Separator) ||
		rawRustFSDataDir != opts.rustFSDataDir {
		return options{}, fmt.Errorf("RustFS data directory must be a canonical non-root absolute path")
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
	mode, err := soakstate.ParseMode(rawMode)
	if err != nil {
		return options{}, err
	}
	opts.mode = mode
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
	directObjects, err := objectstore.Create(objectstore.Config{
		Type: objectstore.TypeS3, Bucket: opts.rustFSBucket, Region: "us-east-1",
		Endpoint: opts.rustFSEndpoint, AccessKey: opts.accessKey, SecretKey: opts.secretKey,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("create direct RustFS observation client: %w", err)
	}
	workerObjects, err := objectstore.Create(objectstore.Config{
		Type: objectstore.TypeS3, Bucket: opts.rustFSBucket, Region: "us-east-1",
		Endpoint: endpoint, AccessKey: opts.accessKey, SecretKey: opts.secretKey,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("create proxied RustFS worker client: %w", err)
	}
	conditional, ok := workerObjects.(objectstore.ContextConditionalStore)
	if !ok || !objectstore.SupportsContextConditionalCreate(workerObjects) {
		pool.Close()
		return nil, fmt.Errorf("RustFS client lacks contextual conditional access")
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
	return &runtimeState{
		pool: pool, store: store, objects: directObjects,
		conditional: conditional, worker: worker,
	}, nil
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

func seedFixture(
	ctx context.Context,
	runtime *runtimeState,
	runID string,
	startedAt time.Time,
) (*fixture, error) {
	result := &fixture{runID: runID, teamID: runID + "-team"}
	sourceDigest := digest.FromString(result.runID + "-source")
	sourceRef := "registry.invalid/sandbox0-soak@" + sourceDigest.String()
	procdDigest := digest.FromString("sandbox0-procd-soak")
	blockOptions := rootfsblock.BuildOptions{ObjectPrefix: "rootfs/soak/" + result.runID + "/base"}
	operationID := result.runID + ":base-import"
	platform := rootfsimporter.ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"}
	operation, err := runtime.store.BeginRootFSImport(ctx, &sandboxstore.BeginRootFSImportRequest{
		OperationID: operationID,
		Spec: rootfsimporter.OperationSpec{
			SourceOCIRef:     sourceRef,
			Platform:         platform,
			FormatGeneration: 1,
			ProcdProtocol:    "sandbox0.procd.soak.v1",
			ProcdDigest:      procdDigest.String(),
			LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes,
			BlockOptions:     blockOptions,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("begin soak Base import: %w", err)
	}
	artifactPlatform := sandboxstore.RootFSArtifactPlatform{
		OS: platform.OS, Architecture: platform.Architecture, Variant: platform.Variant,
	}
	requirements := sandboxstore.ReadyRootFSArtifactRequirements{
		FormatGeneration: 1,
		LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes,
		ProcdProtocol:    "sandbox0.procd.soak.v1",
		ProcdDigest:      procdDigest.String(),
	}
	var artifact *sandboxstore.RootFSBaseArtifact
	if operation.State == sandboxstore.RootFSImportStateReady {
		artifact, err = runtime.store.GetReadyRootFSBaseArtifactByDigest(
			ctx, operation.ArtifactDigest, artifactPlatform, requirements,
		)
		if err != nil {
			return nil, fmt.Errorf("read ready soak Base import %q: %w", operationID, err)
		}
		result.base, err = rootfsblock.DecodeDescriptor(artifact.Descriptor)
		if err != nil {
			return nil, fmt.Errorf("decode ready soak Base import %q: %w", operationID, err)
		}
	} else {
		operation, err = runtime.store.LeaseNextRootFSImport(
			ctx, "materializer-soak-seed", 2*time.Minute,
		)
		if err != nil {
			return nil, fmt.Errorf("lease soak Base import %q: %w", operationID, err)
		}
		if operation == nil || operation.ID != operationID {
			return nil, fmt.Errorf("lease soak Base import %q returned operation %#v", operationID, operation)
		}
		lease, leaseErr := operation.Lease()
		if leaseErr != nil {
			return nil, leaseErr
		}
		journal, journalErr := sandboxstore.NewRootFSImportPublicationJournal(runtime.store, lease)
		if journalErr != nil {
			return nil, journalErr
		}
		publisher := rootfsimporter.JournaledPublisher{
			OperationID: operationID,
			Journal:     journal,
			Publisher:   rootfsblock.ObjectStorePublisher{Store: runtime.conditional},
		}
		base, buildErr := rootfsblock.BuildMaterializedGeneration(
			ctx, zeroReaderAt{size: rootfsartifact.MinimumLogicalSizeBytes},
			rootfsartifact.MinimumLogicalSizeBytes, publisher,
			blockOptions,
		)
		if buildErr != nil {
			return nil, fmt.Errorf("publish soak Base: %w", buildErr)
		}
		result.base = base.Descriptor
		baseBlockRoot, parseErr := digest.Parse(base.Descriptor.MappingRoot.RootDigest)
		if parseErr != nil {
			return nil, fmt.Errorf("parse soak Base block root: %w", parseErr)
		}
		artifact, err = runtime.store.PublishReadyRootFSImport(ctx, &sandboxstore.PublishReadyRootFSImportRequest{
			Lease: lease,
			Result: rootfsimporter.BuildResult{
				SourceOCIRef: sourceRef, SourceOCIDigest: sourceDigest,
				ManifestDigest:   digest.FromString(result.runID + "-manifest"),
				ConfigDigest:     digest.FromString(result.runID + "-config"),
				Platform:         ocispec.Platform{OS: "linux", Architecture: "amd64"},
				LayerDigests:     []digest.Digest{digest.FromString(result.runID + "-layer")},
				DiffIDs:          []digest.Digest{digest.FromString(result.runID + "-diff")},
				ProcdDigest:      procdDigest,
				LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes,
				DescriptorDigest: digest.FromBytes(base.Payload),
				BaseBlockRoot:    baseBlockRoot,
				Descriptor:       base.Descriptor,
				DescriptorBytes:  base.Payload,
				Objects:          base.Objects, Bytes: base.Bytes, References: base.References,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("publish ready soak Base import: %w", err)
		}
	}
	sandboxID := result.runID + "-sandbox"
	if err := runtime.store.UpsertSandbox(ctx, &sandboxstore.SandboxRecord{
		ID: sandboxID, TeamID: result.teamID, UserID: "soak-user",
		TemplateID: "soak-template", TemplateName: "soak-template",
		TemplateNamespace: "soak", DesiredState: sandboxstore.SandboxDesiredStatePaused,
		ResourceMillicpu: 1000, ResourceMemoryMiB: 1024, CreatedAt: startedAt,
	}); err != nil {
		return nil, err
	}
	filesystem, initial, err := runtime.store.EnsureInitialRootFSGeneration(ctx,
		&sandboxstore.EnsureInitialRootFSGenerationRequest{
			SandboxID: sandboxID, TeamID: result.teamID, SourceOCIRef: sourceRef,
			SourceOCIDigest: sourceDigest.String(), BaseArtifactDigest: artifact.ArtifactDigest,
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
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, item := range scheduled {
		data := bytes.Repeat([]byte{byte(item.index%251 + 1)}, rootfsblock.LogicalBlockSize)
		sealed, payload, err := rootfsblock.BuildCompositeGeneration(fixture.base,
			[]rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: data}})
		if err != nil {
			return err
		}
		args := []any{
			generationID(fixture.runID, item.index), fixture.filesystem.ID, fixture.initial.ID,
			fixture.initial.SourceOCIDigest, fixture.initial.BaseArtifactDigest,
			fixture.initial.BaseBlockRoot, sealed.MappingRoot.RootDigest, int64(item.index + 1),
			fixture.initial.FormatGeneration, sandboxstore.RootFSGenerationStateCompositeDurable,
			fixture.initial.LocatorVersion + 1, payload, createdAt,
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO manager.rootfs_generations (
				generation_id, filesystem_id, parent_generation_id, source_oci_digest,
				base_artifact_digest, base_block_root, current_block_head, writer_epoch,
				format_generation, durability_state, locator_version, descriptor, created_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (generation_id) DO NOTHING
		`, args...)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			continue
		}
		var matches bool
		if err := tx.QueryRow(ctx, `
			SELECT filesystem_id = $2 AND parent_generation_id = $3
				AND source_oci_digest = $4 AND base_artifact_digest = $5
				AND base_block_root = $6 AND current_block_head = $7
				AND writer_epoch = $8 AND format_generation = $9
				AND durability_state IN ($10, 's3_materialized')
				AND locator_version >= $11 AND descriptor = $12
			FROM manager.rootfs_generations
			WHERE generation_id = $1
		`, args[:12]...).Scan(&matches); err != nil {
			return err
		}
		if !matches {
			return fmt.Errorf("scheduled generation %s changed across resume", args[0])
		}
	}
	return tx.Commit(ctx)
}

func generationID(runID string, index int) string {
	return fmt.Sprintf("%s-generation-%05d", runID, index)
}

func materializerSoakConfiguration(opts options) soakConfiguration {
	return soakConfiguration{
		Duration: opts.duration.String(), Generations: opts.generations, BurstCount: opts.burstCount,
		WorkerInterval: opts.workerInterval.String(), SampleInterval: opts.sampleInterval.String(),
		MinPackBytes: opts.minPackBytes, MaxDelay: opts.maxDelay.String(),
		PhysicalByteLimit: opts.physicalByteLimit, PhysicalFileLimit: opts.physicalFileLimit,
		DatabaseGrowthLimit: opts.databaseGrowthLimit,
		TerminalRetention:   rootfsmaterializer.DefaultTerminalRetention.String(),
		UploadingStale:      rootfsmaterializer.DefaultUploadingStale.String(),
		GarbageInterval:     rootfsmaterializer.DefaultGarbageInterval.String(),
		RustFSEndpoint:      opts.rustFSEndpoint, RustFSBucket: opts.rustFSBucket,
		RustFSDataDir: opts.rustFSDataDir, ProxyListen: opts.proxyListen,
	}
}

func validateMaterializerSoakCheckpoint(state soakCheckpoint, opts options) error {
	if state.Version != materializerSoakStateVersion || state.ActiveElapsedNS < 0 ||
		state.NextGeneration < 0 || state.NextGeneration > opts.generations {
		return fmt.Errorf("RootFS materializer soak checkpoint identity or progress is invalid")
	}
	validPhase := state.Phase == materializerSoakPhasePreflight ||
		state.Phase == materializerSoakPhaseSeeding || state.Phase == materializerSoakPhaseActive ||
		state.Phase == materializerSoakPhasePassed || state.Phase == materializerSoakPhaseFailed
	if !validPhase {
		return fmt.Errorf("RootFS materializer soak checkpoint phase is invalid")
	}
	validFault := map[string]int{
		materializerFaultPending: 0, materializerFaultArmed: 0,
		materializerFaultTripped: 1, materializerFaultRetryObserved: 2,
		materializerFaultRecovered: 2,
	}
	wantErrors, found := validFault[state.FaultPhase]
	if !found || state.ExpectedWorkerErrors != wantErrors {
		return fmt.Errorf("RootFS materializer soak fault checkpoint is invalid")
	}
	if state.Phase != materializerSoakPhasePreflight && state.DatabaseBaseline.DatabaseBytes <= 0 {
		return fmt.Errorf("RootFS materializer soak database baseline is absent")
	}
	if state.Phase != materializerSoakPhasePreflight &&
		(state.PhysicalBaseline.Files < 0 || state.PhysicalBaseline.Bytes < 0) {
		return fmt.Errorf("RootFS materializer soak physical baseline is absent")
	}
	if state.Phase != materializerSoakPhasePreflight && state.Phase != materializerSoakPhaseSeeding &&
		state.Fixture == nil {
		return fmt.Errorf("RootFS materializer soak fixture checkpoint is absent")
	}
	if state.FaultPhase == materializerFaultTripped || state.FaultPhase == materializerFaultRetryObserved ||
		state.FaultPhase == materializerFaultRecovered {
		if state.FaultBatchID == "" {
			return fmt.Errorf("RootFS materializer soak fault batch identity is absent")
		}
	}
	if state.Phase == materializerSoakPhasePassed &&
		(state.NextGeneration != opts.generations || time.Duration(state.ActiveElapsedNS) < opts.duration ||
			state.FaultPhase != materializerFaultRecovered || len(state.FinalViolations) != 0) {
		return fmt.Errorf("passed RootFS materializer soak checkpoint is incomplete")
	}
	if state.Phase == materializerSoakPhaseFailed && len(state.FinalViolations) == 0 {
		return fmt.Errorf("failed RootFS materializer soak checkpoint has no violations")
	}
	return nil
}

func checkpointFixture(value *fixture) *fixtureCheckpoint {
	if value == nil || value.filesystem == nil || value.initial == nil {
		return nil
	}
	return &fixtureCheckpoint{
		RunID: value.runID, TeamID: value.teamID, Filesystem: *value.filesystem,
		Initial: *value.initial, Base: value.base,
	}
}

func restoreAndVerifyFixture(
	ctx context.Context,
	runtime *runtimeState,
	runID string,
	expected *fixtureCheckpoint,
) (*fixture, error) {
	if expected == nil || expected.RunID != runID || expected.Initial.CreatedAt.IsZero() {
		return nil, fmt.Errorf("durable RootFS materializer fixture identity is invalid")
	}
	actual, err := seedFixture(ctx, runtime, runID, expected.Initial.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("verify durable RootFS materializer fixture: %w", err)
	}
	wantPayload, err := json.Marshal(expected)
	if err != nil {
		return nil, err
	}
	actualPayload, err := json.Marshal(checkpointFixture(actual))
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(actualPayload, wantPayload) {
		return nil, fmt.Errorf("durable RootFS materializer fixture changed across resume")
	}
	return actual, nil
}

func ensureMaterializerDatabaseIdentity(
	ctx context.Context,
	pool *pgxpool.Pool,
	runID string,
	configSHA256 string,
	allowInitialize bool,
) error {
	if _, err := pool.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS sandbox0_soak;
		CREATE TABLE IF NOT EXISTS sandbox0_soak.rootfs_materializer_run (
			singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
			version INTEGER NOT NULL,
			run_id TEXT NOT NULL,
			config_sha256 TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		return fmt.Errorf("create RootFS materializer soak identity table: %w", err)
	}
	if allowInitialize {
		if _, err := pool.Exec(ctx, `
			INSERT INTO sandbox0_soak.rootfs_materializer_run (
				singleton, version, run_id, config_sha256
			) VALUES (TRUE, $1, $2, $3)
			ON CONFLICT (singleton) DO NOTHING
		`, materializerSoakStateVersion, runID, configSHA256); err != nil {
			return fmt.Errorf("initialize RootFS materializer soak identity: %w", err)
		}
	}
	var version int
	var storedRunID, storedConfigSHA256 string
	if err := pool.QueryRow(ctx, `
		SELECT version, run_id, config_sha256
		FROM sandbox0_soak.rootfs_materializer_run
		WHERE singleton = TRUE
	`).Scan(&version, &storedRunID, &storedConfigSHA256); errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("RootFS materializer soak database identity is absent")
	} else if err != nil {
		return fmt.Errorf("read RootFS materializer soak database identity: %w", err)
	}
	if version != materializerSoakStateVersion || storedRunID != runID || storedConfigSHA256 != configSHA256 {
		return fmt.Errorf("RootFS materializer soak database identity changed")
	}
	return nil
}

func ensureMaterializerObjectIdentity(
	ctx context.Context,
	store objectstore.Store,
	runID string,
	configSHA256 string,
	executableSHA256 string,
	requireOnlyIdentity bool,
) error {
	if err := store.Create(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") {
		return fmt.Errorf("create dedicated RustFS bucket: %w", err)
	}
	conditional, ok := store.(objectstore.ContextConditionalStore)
	if !ok || !objectstore.SupportsContextConditionalCreate(store) {
		return fmt.Errorf("RustFS soak identity requires contextual conditional access")
	}
	payload, err := json.Marshal(map[string]any{
		"version": materializerSoakStateVersion, "run_id": runID,
		"config_sha256": configSHA256, "executable_sha256": executableSHA256,
	})
	if err != nil {
		return err
	}
	key := "soak-control/" + runID + "/identity.json"
	if err := (rootfsblock.ObjectStorePublisher{Store: conditional}).PutImmutable(ctx, key, payload); err != nil {
		return fmt.Errorf("verify RootFS materializer RustFS identity: %w", err)
	}
	if requireOnlyIdentity {
		snapshot, err := snapshotObjects(store)
		if err != nil {
			return err
		}
		if snapshot.Objects != 1 {
			return fmt.Errorf("dedicated RustFS bucket contains %d objects before seeding, want identity only", snapshot.Objects)
		}
	}
	return nil
}

func materializerCounters(state soakCheckpoint, database databaseSnapshot) counters {
	materialized := max(int(database.MaterializedGenerations)-1, 0)
	return counters{
		Generated: state.NextGeneration, Materialized: materialized,
		RetainedBatches:      int(database.UploadingBatches + database.PublishedBatches + database.AbandonedBatches),
		ExpectedWorkerErrors: state.ExpectedWorkerErrors,
	}
}

func materializerAcceptanceBounds(opts options) acceptanceBounds {
	maxBatches := int(opts.duration/opts.maxDelay) + 3
	return acceptanceBounds{MaxBatches: maxBatches, MaxObjects: int64(2 + 2*maxBatches)}
}

func materializerSoakCheckpointInterval(duration time.Duration) time.Duration {
	return maxDuration(min(5*time.Second, duration/20), 100*time.Millisecond)
}

func nextSoakBoundary(elapsed, interval time.Duration) time.Duration {
	return (elapsed/interval + 1) * interval
}

func progressMaterializerFault(
	ctx context.Context,
	runtime **runtimeState,
	opts options,
	proxyEndpoint string,
	proxy *outageProxy,
	state *soakCheckpoint,
	commit func(string, any) error,
) (bool, error) {
	current := *runtime
	switch state.FaultPhase {
	case materializerFaultArmed:
		pending, err := current.store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
		if err != nil {
			return false, err
		}
		if pending == nil {
			if !proxy.Snapshot().Armed {
				proxy.ArmAfterNextPut()
			}
			return false, nil
		}
		proxy.FailAll()
		state.FaultBatchID = pending.BatchID
		state.FaultPhase = materializerFaultTripped
		state.ExpectedWorkerErrors = 1
		return true, commit("rustfs_outage_reconciled", map[string]any{
			"batch_id": pending.BatchID, "members": len(pending.Members),
			"reason": "uploading batch survived before the tripped checkpoint",
		})
	case materializerFaultTripped:
		proxy.FailAll()
		pending, err := current.store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
		if err != nil || pending == nil || pending.BatchID != state.FaultBatchID {
			return false, fmt.Errorf("exact outage batch %s is not uploading before retry: %w", state.FaultBatchID, err)
		}
		current.pool.Close()
		restarted, err := openRuntime(ctx, opts, proxyEndpoint)
		if err != nil {
			return false, fmt.Errorf("reopen materializer while RustFS remains unavailable: %w", err)
		}
		*runtime = restarted
		current = restarted
		result, retryErr := current.worker.RunOnce(ctx)
		if retryErr == nil {
			return false, fmt.Errorf("exact batch unexpectedly succeeded while RustFS remained unavailable")
		}
		pendingAfter, err := current.store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
		if err != nil || pendingAfter == nil || pendingAfter.BatchID != state.FaultBatchID {
			return false, fmt.Errorf("outage retry changed exact uploading batch %s: %w", state.FaultBatchID, err)
		}
		state.FaultPhase = materializerFaultRetryObserved
		state.ExpectedWorkerErrors = 2
		return true, commit("materializer_restarted_during_outage", map[string]any{
			"batch_id": state.FaultBatchID, "members": len(pendingAfter.Members),
			"retry_result": result, "retry_error": retryErr.Error(),
		})
	case materializerFaultRetryObserved:
		pending, err := current.store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
		if err != nil {
			return false, err
		}
		var recoveryResult rootfsmaterializer.Result
		if pending != nil {
			if pending.BatchID != state.FaultBatchID {
				return false, fmt.Errorf("another uploading batch replaced exact outage batch %s", state.FaultBatchID)
			}
			proxy.Recover()
			recoveryResult, err = current.worker.RunOnce(ctx)
			if err != nil {
				return false, fmt.Errorf("resume exact batch after RustFS recovery: %w", err)
			}
		}
		batchState, err := materializerBatchState(ctx, current.pool, state.FaultBatchID)
		if err != nil || batchState != "published" {
			return false, fmt.Errorf("exact outage batch %s is not published after recovery: %w", state.FaultBatchID, err)
		}
		pendingFinal, err := current.store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
		if err != nil || pendingFinal != nil {
			return false, fmt.Errorf("an uploading batch remained after exact recovery: %w", err)
		}
		objectsAfter, err := snapshotObjects(current.objects)
		if err != nil {
			return false, err
		}
		if growth := objectsAfter.Objects - state.FaultObjects.Objects; growth < 1 || growth > 2 {
			return false, fmt.Errorf("exact outage batch created %d object keys, want 1..2", growth)
		}
		state.FaultPhase = materializerFaultRecovered
		return true, commit("rustfs_outage_recovered", map[string]any{
			"batch_id": state.FaultBatchID, "result": recoveryResult,
			"objects_before": state.FaultObjects, "objects_after": objectsAfter,
			"proxy": proxy.Snapshot(),
		})
	default:
		return false, nil
	}
}

func materializerBatchState(ctx context.Context, pool *pgxpool.Pool, batchID string) (string, error) {
	var state string
	if err := pool.QueryRow(ctx, `
		SELECT state
		FROM manager.rootfs_materialization_batches
		WHERE batch_id = $1
	`, batchID).Scan(&state); err != nil {
		return "", err
	}
	return state, nil
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
	activeElapsed time.Duration,
	state counters,
	faultComplete bool,
	baselineDB databaseSnapshot,
	baselinePhysical directorySnapshot,
	finalDB databaseSnapshot,
	finalObjects objectSnapshot,
	finalPhysical directorySnapshot,
) []string {
	var result []string
	if activeElapsed < opts.duration {
		result = append(result, "actual active duration was shorter than configured")
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
	// The importer journals the Base object in the shared catalog. Only the
	// soak-run identity object intentionally remains outside catalog ownership.
	if finalDB.CatalogObjects != finalObjects.Objects-1 {
		result = append(result, fmt.Sprintf("catalog/RustFS objects=%d/%d do not differ by the run identity object",
			finalDB.CatalogObjects, finalObjects.Objects))
	}
	bounds := materializerAcceptanceBounds(opts)
	if state.RetainedBatches <= 0 {
		result = append(result, "no retained materialization batches were observed")
	}
	if finalObjects.Objects > bounds.MaxObjects {
		result = append(result, fmt.Sprintf("RustFS object count=%d exceeds batch bound=%d",
			finalObjects.Objects, bounds.MaxObjects))
	}
	if state.RetainedBatches > bounds.MaxBatches {
		result = append(result, fmt.Sprintf("retained materialization batches=%d exceed forced-flush bound=%d",
			state.RetainedBatches, bounds.MaxBatches))
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

func (p *outageProxy) FailAll() {
	p.mu.Lock()
	p.armed = true
	p.tripped = true
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
