package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	meteringclickhouse "github.com/sandbox0-ai/sandbox0/pkg/metering/clickhouse"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	storagevolume "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

const (
	volumeStorageMeteringReconcileCommand = "reconcile-volume-storage-metering"
	volumeStorageMeteringReconcileLockID  = int64(0x766f6c6d65746572)
)

var errVolumeStorageMeteringHeadChanged = errors.New("volume committed head changed")

type volumeStorageMeteringCandidate struct {
	Volume             db.SandboxVolume
	Head               *s0fs.CommittedHead
	ProjectedSizeBytes *int64
}

type volumeStorageMeteringReconcileResult struct {
	Mode                     string `json:"mode"`
	TotalVolumes             int    `json:"total_volumes"`
	HeadlessVolumes          int    `json:"headless_volumes"`
	MatchedVolumes           int    `json:"matched_volumes"`
	MismatchedVolumes        int    `json:"mismatched_volumes"`
	MissingProjectionVolumes int    `json:"missing_projection_volumes"`
	UpdatedVolumes           int    `json:"updated_volumes"`
	FailedVolumes            int    `json:"failed_volumes"`
	LogicalBytes             int64  `json:"logical_bytes"`
	ProjectedBytesBefore     int64  `json:"projected_bytes_before"`
	OutboxStartSequence      int64  `json:"outbox_start_sequence,omitempty"`
	OutboxEndSequence        int64  `json:"outbox_end_sequence,omitempty"`
	OutboxPending            int64  `json:"outbox_pending,omitempty"`
	ClickHouseCheckedVolumes int    `json:"clickhouse_checked_volumes"`
	ClickHouseMismatches     int    `json:"clickhouse_mismatches"`
}

type volumeStorageMeteringReconcileBackend interface {
	ListCandidates(context.Context) ([]volumeStorageMeteringCandidate, error)
	LoadCurrentState(context.Context, volumeStorageMeteringCandidate) (*s0fs.SnapshotState, *s0fs.Manifest, error)
	RecordCurrentState(context.Context, volumeStorageMeteringCandidate, *s0fs.Manifest, int64) error
}

type postgresVolumeStorageMeteringBackend struct {
	pool             *pgxpool.Pool
	volumeRepo       *db.Repository
	meteringRepo     *meteringoutbox.Repository
	baseStore        objectstore.Store
	encryption       *s0fs.EncryptionConfig
	regionID         string
	defaultClusterID string
}

// runVolumeStorageMeteringReconcile audits every committed S0FS head and,
// when explicitly requested, repairs PostgreSQL through the canonical outbox.
func runVolumeStorageMeteringReconcile(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet(volumeStorageMeteringReconcileCommand, flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", strings.TrimSpace(getenvOrDefault("STORAGE_RUNTIME_CONFIG_PATH", "/config/storage-runtime.yaml")), "manager storage runtime config path")
	apply := flags.Bool("apply", false, "write corrected observations through the PostgreSQL metering outbox")
	timeout := flags.Duration("timeout", 15*time.Minute, "overall reconciliation timeout")
	outboxWait := flags.Duration("outbox-wait", 2*time.Minute, "maximum time to wait for emitted outbox operations")
	verifyClickHouse := flags.Bool("verify-clickhouse", true, "compare current manifests with the ClickHouse storage projection")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if *outboxWait < 0 {
		return fmt.Errorf("outbox-wait must not be negative")
	}

	cfg, err := config.ReadStorageProxyConfig(*configPath)
	if err != nil {
		return err
	}
	if !cfg.Metering.Enabled {
		return fmt.Errorf("storage metering is disabled")
	}
	if strings.TrimSpace(cfg.DatabaseURL) == "" {
		return fmt.Errorf("storage database URL is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     cfg.DatabaseURL,
		MaxConns:        4,
		MinConns:        0,
		DefaultMaxConns: 4,
		Schema:          storageDatabaseSchema(cfg),
	})
	if err != nil {
		return err
	}
	defer pool.Close()

	releaseLock, err := acquireVolumeStorageMeteringReconcileLock(ctx, pool, *apply)
	if err != nil {
		return err
	}
	defer releaseLock()

	backend, err := newPostgresVolumeStorageMeteringBackend(cfg, pool)
	if err != nil {
		return err
	}
	startSequence, err := latestVolumeStorageMeteringOutboxSequence(ctx, pool)
	if err != nil {
		return err
	}
	result, expected, reconcileErr := reconcileVolumeStorageMetering(ctx, backend, *apply, stderr)
	result.Mode = "audit"
	if *apply {
		result.Mode = "apply"
		result.OutboxStartSequence = startSequence
		endSequence, sequenceErr := latestVolumeStorageMeteringOutboxSequence(ctx, pool)
		if sequenceErr != nil {
			reconcileErr = errors.Join(reconcileErr, sequenceErr)
		} else {
			result.OutboxEndSequence = endSequence
			if *outboxWait > 0 && endSequence > startSequence {
				pending, waitErr := waitForVolumeStorageMeteringOutbox(ctx, pool, startSequence, endSequence, *outboxWait)
				result.OutboxPending = pending
				if waitErr != nil {
					reconcileErr = errors.Join(reconcileErr, waitErr)
				}
			}
		}
	}
	if *verifyClickHouse {
		checked, mismatches, verifyErr := verifyVolumeStorageMeteringClickHouse(ctx, cfg, expected)
		result.ClickHouseCheckedVolumes = checked
		result.ClickHouseMismatches = mismatches
		if verifyErr != nil {
			reconcileErr = errors.Join(reconcileErr, verifyErr)
		} else if *apply && mismatches != 0 {
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("ClickHouse contains %d volume storage projection mismatches after repair", mismatches))
		}
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		return errors.Join(reconcileErr, err)
	}
	return reconcileErr
}

func newPostgresVolumeStorageMeteringBackend(cfg *config.StorageProxyConfig, pool *pgxpool.Pool) (*postgresVolumeStorageMeteringBackend, error) {
	baseStore, err := objectstore.Create(objectstore.Config{
		Type:         cfg.ObjectStorageType,
		Bucket:       cfg.S3Bucket,
		Region:       cfg.S3Region,
		Endpoint:     cfg.S3Endpoint,
		AccessKey:    cfg.S3AccessKey,
		SecretKey:    cfg.S3SecretKey,
		SessionToken: cfg.S3SessionToken,
	})
	if err != nil {
		return nil, fmt.Errorf("create volume object store: %w", err)
	}
	encryption, err := storagevolume.S0FSEncryptionConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &postgresVolumeStorageMeteringBackend{
		pool:             pool,
		volumeRepo:       db.NewRepository(pool),
		meteringRepo:     meteringoutbox.NewRepository(pool),
		baseStore:        baseStore,
		encryption:       encryption,
		regionID:         cfg.RegionID,
		defaultClusterID: cfg.DefaultClusterId,
	}, nil
}

func (b *postgresVolumeStorageMeteringBackend) ListCandidates(ctx context.Context) ([]volumeStorageMeteringCandidate, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT
			v.id, v.team_id, v.user_id, v.created_at, v.updated_at,
			h.manifest_seq, h.checkpoint_seq, h.manifest_key, h.updated_at,
			s.size_bytes
		FROM sandbox_volumes AS v
		LEFT JOIN sandbox_volume_s0fs_heads AS h
			ON h.volume_id = v.id
		LEFT JOIN metering.storage_projection_state AS s
			ON s.subject_type = 'volume' AND s.subject_id = v.id
		WHERE COALESCE(NULLIF(v.backend, ''), 's0fs') = 's0fs'
		ORDER BY v.id
	`)
	if err != nil {
		return nil, fmt.Errorf("query S0FS volume metering candidates: %w", err)
	}
	defer rows.Close()

	candidates := make([]volumeStorageMeteringCandidate, 0)
	for rows.Next() {
		var (
			candidate     volumeStorageMeteringCandidate
			manifestSeq   *int64
			checkpointSeq *int64
			manifestKey   *string
			headUpdatedAt *time.Time
			projectedSize *int64
		)
		if err := rows.Scan(
			&candidate.Volume.ID,
			&candidate.Volume.TeamID,
			&candidate.Volume.UserID,
			&candidate.Volume.CreatedAt,
			&candidate.Volume.UpdatedAt,
			&manifestSeq,
			&checkpointSeq,
			&manifestKey,
			&headUpdatedAt,
			&projectedSize,
		); err != nil {
			return nil, fmt.Errorf("scan S0FS volume metering candidate: %w", err)
		}
		candidate.Volume.Backend = storagevolume.BackendS0FS
		candidate.ProjectedSizeBytes = projectedSize
		if manifestSeq != nil && checkpointSeq != nil && manifestKey != nil && headUpdatedAt != nil {
			if *manifestSeq < 0 || *checkpointSeq < 0 {
				return nil, fmt.Errorf("volume %s has a negative committed head sequence", candidate.Volume.ID)
			}
			candidate.Head = &s0fs.CommittedHead{
				VolumeID:      candidate.Volume.ID,
				ManifestSeq:   uint64(*manifestSeq),
				CheckpointSeq: uint64(*checkpointSeq),
				ManifestKey:   *manifestKey,
				UpdatedAt:     *headUpdatedAt,
			}
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate S0FS volume metering candidates: %w", err)
	}
	return candidates, nil
}

func (b *postgresVolumeStorageMeteringBackend) LoadCurrentState(ctx context.Context, candidate volumeStorageMeteringCandidate) (*s0fs.SnapshotState, *s0fs.Manifest, error) {
	prefix, err := naming.S3VolumePrefix(candidate.Volume.TeamID, candidate.Volume.ID)
	if err != nil {
		return nil, nil, err
	}
	store := objectstore.Prefix(b.baseStore, prefix+"/s0fs/")
	materializer := s0fs.NewMaterializer(candidate.Volume.ID, store, db.NewS0FSHeadStore(b.volumeRepo))
	materializer.SetEncryption(b.encryption)
	state, manifest, err := materializer.LoadLatestState(ctx)
	if err != nil {
		return nil, nil, err
	}
	return state, manifest, nil
}

func (b *postgresVolumeStorageMeteringBackend) RecordCurrentState(
	ctx context.Context,
	candidate volumeStorageMeteringCandidate,
	manifest *s0fs.Manifest,
	sizeBytes int64,
) error {
	if manifest == nil {
		return fmt.Errorf("volume %s has no current manifest", candidate.Volume.ID)
	}
	return b.meteringRepo.InTx(ctx, func(tx pgx.Tx) error {
		var currentManifestSeq int64
		err := tx.QueryRow(ctx, `
			SELECT manifest_seq
			FROM sandbox_volume_s0fs_heads
			WHERE volume_id = $1
			FOR SHARE
		`, candidate.Volume.ID).Scan(&currentManifestSeq)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return errVolumeStorageMeteringHeadChanged
			}
			return fmt.Errorf("lock current S0FS head for volume %s: %w", candidate.Volume.ID, err)
		}
		if currentManifestSeq < 0 ||
			uint64(currentManifestSeq) != manifest.ManifestSeq {
			return errVolumeStorageMeteringHeadChanged
		}

		var observedAt time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&observedAt); err != nil {
			return fmt.Errorf("read database clock: %w", err)
		}
		observation := storagevolume.VolumeStorageObservation(
			ctx,
			b.volumeRepo,
			&candidate.Volume,
			b.regionID,
			b.defaultClusterID,
			sizeBytes,
			observedAt,
		)
		if err := b.meteringRepo.RecordStorageObservationTx(ctx, tx, observation); err != nil {
			return err
		}
		return b.meteringRepo.UpsertProducerWatermarkTx(
			ctx,
			tx,
			meteringpkg.ProducerStorage,
			observation.RegionID,
			observation.ObservedAt,
		)
	})
}

// reconcileVolumeStorageMetering compares logical bytes in immutable S0FS
// manifests with producer-side metering state. Headless volumes are reported
// but never guessed because a legacy manifest may still exist in object storage.
func reconcileVolumeStorageMetering(
	ctx context.Context,
	backend volumeStorageMeteringReconcileBackend,
	apply bool,
	stderr io.Writer,
) (volumeStorageMeteringReconcileResult, map[string]int64, error) {
	var result volumeStorageMeteringReconcileResult
	candidates, err := backend.ListCandidates(ctx)
	if err != nil {
		return result, nil, err
	}
	result.TotalVolumes = len(candidates)
	expected := make(map[string]int64, len(candidates))
	var reconcileErr error
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return result, expected, errors.Join(reconcileErr, err)
		}
		if candidate.Head == nil {
			result.HeadlessVolumes++
			continue
		}

		var (
			state    *s0fs.SnapshotState
			manifest *s0fs.Manifest
			loadErr  error
		)
		for attempt := 0; attempt < 3; attempt++ {
			state, manifest, loadErr = backend.LoadCurrentState(ctx, candidate)
			if loadErr == nil {
				break
			}
		}
		if loadErr != nil {
			result.FailedVolumes++
			fmt.Fprintf(stderr, "volume %s: load current S0FS state: %v\n", candidate.Volume.ID, loadErr)
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("load volume %s current state: %w", candidate.Volume.ID, loadErr))
			continue
		}
		sizeBytes := s0fs.StateStorageBytes(state)
		expected[candidate.Volume.ID] = sizeBytes
		result.LogicalBytes += sizeBytes
		if candidate.ProjectedSizeBytes != nil {
			result.ProjectedBytesBefore += *candidate.ProjectedSizeBytes
		}
		if candidate.ProjectedSizeBytes != nil && *candidate.ProjectedSizeBytes == sizeBytes {
			result.MatchedVolumes++
			continue
		}
		result.MismatchedVolumes++
		if candidate.ProjectedSizeBytes == nil {
			result.MissingProjectionVolumes++
		}
		if !apply {
			continue
		}

		var recordErr error
		loadedSizeBytes := sizeBytes
		for attempt := 0; attempt < 3; attempt++ {
			recordErr = backend.RecordCurrentState(ctx, candidate, manifest, sizeBytes)
			if !errors.Is(recordErr, errVolumeStorageMeteringHeadChanged) {
				break
			}
			state, manifest, recordErr = backend.LoadCurrentState(ctx, candidate)
			if recordErr != nil {
				break
			}
			sizeBytes = s0fs.StateStorageBytes(state)
			expected[candidate.Volume.ID] = sizeBytes
		}
		result.LogicalBytes += sizeBytes - loadedSizeBytes
		if recordErr != nil {
			result.FailedVolumes++
			fmt.Fprintf(stderr, "volume %s: record current storage observation: %v\n", candidate.Volume.ID, recordErr)
			reconcileErr = errors.Join(reconcileErr, fmt.Errorf("record volume %s storage observation: %w", candidate.Volume.ID, recordErr))
			continue
		}
		result.UpdatedVolumes++
	}
	return result, expected, reconcileErr
}

func acquireVolumeStorageMeteringReconcileLock(ctx context.Context, pool *pgxpool.Pool, apply bool) (func(), error) {
	if !apply {
		return func() {}, nil
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire volume metering reconciliation lock connection: %w", err)
	}
	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, volumeStorageMeteringReconcileLockID).Scan(&acquired); err != nil {
		conn.Release()
		return nil, fmt.Errorf("acquire volume metering reconciliation lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, fmt.Errorf("another volume storage metering reconciliation is running")
	}
	return func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(releaseCtx, `SELECT pg_advisory_unlock($1)`, volumeStorageMeteringReconcileLockID)
		conn.Release()
	}, nil
}

func latestVolumeStorageMeteringOutboxSequence(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var sequence int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(sequence), 0) FROM metering.projection_outbox`).Scan(&sequence); err != nil {
		return 0, fmt.Errorf("query metering outbox sequence: %w", err)
	}
	return sequence, nil
}

func waitForVolumeStorageMeteringOutbox(
	ctx context.Context,
	pool *pgxpool.Pool,
	startSequence, endSequence int64,
	timeout time.Duration,
) (int64, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		var pending int64
		if err := pool.QueryRow(waitCtx, `
			SELECT COUNT(*)
			FROM metering.projection_outbox
			WHERE sequence > $1 AND sequence <= $2 AND delivered_at IS NULL
		`, startSequence, endSequence).Scan(&pending); err != nil {
			return pending, fmt.Errorf("query pending metering outbox operations: %w", err)
		}
		if pending == 0 {
			return 0, nil
		}
		select {
		case <-waitCtx.Done():
			return pending, fmt.Errorf("wait for metering outbox delivery: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func verifyVolumeStorageMeteringClickHouse(
	ctx context.Context,
	cfg *config.StorageProxyConfig,
	expected map[string]int64,
) (int, int, error) {
	if len(expected) == 0 {
		return 0, 0, nil
	}
	ch := cfg.Metering.ClickHouse
	timeout := ch.ConnectTimeout.Duration
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	database, repo, err := meteringclickhouse.Open(connectCtx, meteringclickhouse.OpenConfig{
		DSN: ch.DSN,
		Schema: meteringclickhouse.Config{
			Database:          ch.Database,
			EventsTable:       ch.EventsTable,
			WindowsTable:      ch.WindowsTable,
			WatermarksTable:   ch.WatermarksTable,
			SandboxStateTable: ch.SandboxStateTable,
			StorageStateTable: ch.StorageStateTable,
		},
		Migrate: false,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("open ClickHouse metering projection: %w", err)
	}
	defer database.Close()
	states, err := repo.ListActiveStorageProjectionStates(ctx)
	if err != nil {
		return 0, 0, err
	}
	actual := make(map[string]int64, len(states))
	for _, state := range states {
		if state.SubjectType == meteringpkg.SubjectTypeVolume {
			actual[state.SubjectID] = state.SizeBytes
		}
	}
	mismatches := 0
	for volumeID, sizeBytes := range expected {
		if actualSize, ok := actual[volumeID]; !ok || actualSize != sizeBytes {
			mismatches++
		}
	}
	return len(expected), mismatches, nil
}

func storageDatabaseSchema(cfg *config.StorageProxyConfig) string {
	if cfg == nil || strings.TrimSpace(cfg.DatabaseSchema) == "" {
		return "storage_proxy"
	}
	return strings.TrimSpace(cfg.DatabaseSchema)
}

func getenvOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
