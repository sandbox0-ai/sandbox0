package rootfsmaterializer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const (
	DefaultScanLimit               = 1_000
	DefaultInterval                = time.Second
	DefaultMinPackBytes      int64 = 32 << 20
	DefaultMaxDelay                = 5 * time.Minute
	DefaultForcedFlushes           = 1
	DefaultGarbageInterval         = time.Minute
	DefaultUploadingStale          = time.Hour
	DefaultTerminalRetention       = 24 * time.Hour
)

type GenerationStore interface {
	ListCompositeRootFSGenerations(context.Context, int) ([]sandboxstore.RootFSGeneration, error)
	GetOldestUploadingRootFSGenerationMaterializationBatch(context.Context) (*sandboxstore.RootFSGenerationMaterializationBatch, error)
	BeginRootFSGenerationMaterializationBatch(context.Context, *sandboxstore.BeginRootFSGenerationMaterializationBatchRequest) (*sandboxstore.RootFSGenerationMaterializationBatch, error)
	RegisterRootFSGenerationMaterializationBatchObject(context.Context, string, rootfsblock.ObjectReference) error
	MarkRootFSGenerationMaterializationBatchObjectUploaded(context.Context, string, string) error
	PublishRootFSGenerationMaterializationBatch(context.Context, *sandboxstore.PublishRootFSGenerationMaterializationBatchRequest) error
	ReconcileRootFSGenerationMaterializationGarbage(context.Context, time.Duration, time.Duration, int) (*sandboxstore.RootFSGenerationMaterializationGarbageResult, error)
}

type Config struct {
	Store               GenerationStore
	Source              rootfsblock.RangeSource
	Publisher           rootfsblock.ImmutableObjectPublisher
	ScanLimit           int
	Interval            time.Duration
	MinPackBytes        int64
	MaxDelay            time.Duration
	ForcedFlushesPerRun int
	GarbageInterval     time.Duration
	UploadingStale      time.Duration
	TerminalRetention   time.Duration
}

// Result describes one bounded materializer pass.
type Result struct {
	Scanned      int
	Materialized int
	Deferred     int
	Batches      int
	Failed       int
	Abandoned    int
	Purged       int
	Enqueued     int
}

// Worker converts region-durable PostgreSQL composite tails into immutable S3
// block mappings. Exact membership is journaled before the first PUT, and all
// generation locators in one shared pack publish atomically.
type Worker struct {
	store               GenerationStore
	source              rootfsblock.RangeSource
	publisher           rootfsblock.ImmutableObjectPublisher
	scanLimit           int
	interval            time.Duration
	minPackBytes        int64
	maxDelay            time.Duration
	forcedFlushesPerRun int
	garbageInterval     time.Duration
	uploadingStale      time.Duration
	terminalRetention   time.Duration
	garbageMu           sync.Mutex
	lastGarbage         time.Time
}

type objectPublicationError struct{ error }
type locatorPublicationError struct{ error }

func (e *objectPublicationError) Unwrap() error  { return e.error }
func (e *locatorPublicationError) Unwrap() error { return e.error }

func New(config Config) (*Worker, error) {
	if config.Store == nil || config.Source == nil || config.Publisher == nil {
		return nil, fmt.Errorf("materializer store, range source, and publisher are required")
	}
	if config.ScanLimit == 0 {
		config.ScanLimit = DefaultScanLimit
	}
	if config.Interval == 0 {
		config.Interval = DefaultInterval
	}
	if config.MinPackBytes == 0 {
		config.MinPackBytes = DefaultMinPackBytes
	}
	if config.MaxDelay == 0 {
		config.MaxDelay = DefaultMaxDelay
	}
	if config.ForcedFlushesPerRun == 0 {
		config.ForcedFlushesPerRun = DefaultForcedFlushes
	}
	if config.GarbageInterval == 0 {
		config.GarbageInterval = DefaultGarbageInterval
	}
	if config.UploadingStale == 0 {
		config.UploadingStale = DefaultUploadingStale
	}
	if config.TerminalRetention == 0 {
		config.TerminalRetention = DefaultTerminalRetention
	}
	if config.ScanLimit < 1 || config.ScanLimit > 10_000 {
		return nil, fmt.Errorf("materializer scan limit must be between 1 and 10000")
	}
	if config.Interval < 10*time.Millisecond {
		return nil, fmt.Errorf("materializer interval must be at least 10ms")
	}
	if config.MinPackBytes < rootfsblock.LogicalBlockSize || config.MinPackBytes > rootfsblock.DefaultPackBytes {
		return nil, fmt.Errorf("materializer minimum pack bytes must be between one logical block and %d", rootfsblock.DefaultPackBytes)
	}
	if config.MaxDelay < time.Second || config.MaxDelay > 24*time.Hour {
		return nil, fmt.Errorf("materializer maximum delay must be between 1s and 24h")
	}
	if config.ForcedFlushesPerRun < 1 || config.ForcedFlushesPerRun > 100 {
		return nil, fmt.Errorf("materializer forced flushes per run must be between 1 and 100")
	}
	if config.GarbageInterval < time.Second || config.GarbageInterval > 24*time.Hour {
		return nil, fmt.Errorf("materializer garbage interval must be between 1s and 24h")
	}
	if config.UploadingStale < time.Minute || config.UploadingStale > 7*24*time.Hour {
		return nil, fmt.Errorf("materializer uploading stale interval must be between 1m and 7d")
	}
	if config.TerminalRetention < time.Minute || config.TerminalRetention > 30*24*time.Hour {
		return nil, fmt.Errorf("materializer terminal retention must be between 1m and 30d")
	}
	return &Worker{
		store: config.Store, source: config.Source, publisher: config.Publisher,
		scanLimit: config.ScanLimit, interval: config.Interval,
		minPackBytes: config.MinPackBytes, maxDelay: config.MaxDelay,
		forcedFlushesPerRun: config.ForcedFlushesPerRun,
		garbageInterval:     config.GarbageInterval, uploadingStale: config.UploadingStale,
		terminalRetention: config.TerminalRetention,
	}, nil
}

// RunOnce first resumes the oldest exact uploading batch. Only when no crash
// recovery exists may it form new tenant/format-isolated membership. Small
// lanes wait for more payload, with a bounded age-triggered flush budget.
func (w *Worker) RunOnce(ctx context.Context) (Result, error) {
	garbage, err := w.reconcileGarbage(ctx)
	if err != nil {
		return Result{}, err
	}
	pending, err := w.store.GetOldestUploadingRootFSGenerationMaterializationBatch(ctx)
	if err != nil {
		return Result{}, err
	}
	if pending != nil {
		result := Result{Scanned: len(pending.Members), Batches: 1}
		applyGarbageResult(&result, garbage)
		if err := w.materializeBatch(ctx, pending); err != nil {
			result.Failed = len(pending.Members)
			return result, fmt.Errorf("resume materialization batch %s: %w", pending.BatchID, err)
		}
		result.Materialized = len(pending.Members)
		return result, nil
	}

	generations, err := w.store.ListCompositeRootFSGenerations(ctx, w.scanLimit)
	if err != nil {
		return Result{}, err
	}
	result := Result{Scanned: len(generations)}
	applyGarbageResult(&result, garbage)
	type candidate struct {
		generation sandboxstore.RootFSGeneration
	}
	type laneGroup struct {
		lane       string
		teamID     string
		format     int
		oldest     time.Time
		dataBytes  int64
		candidates []candidate
	}
	lanes := make(map[string]*laneGroup)
	laneOrder := make([]string, 0)
	var resultErr error
	for index := range generations {
		generation := generations[index]
		dataBytes, err := estimateCompositeFinalDataBytes(generation.Descriptor)
		if err != nil || generation.MaterializationPackLane == "" || generation.MaterializationTeamID == "" ||
			generation.FormatGeneration <= 0 {
			if err == nil {
				err = fmt.Errorf("candidate has no materialization isolation lane")
			}
			result.Failed++
			resultErr = errors.Join(resultErr, fmt.Errorf("prepare generation %s: %w", generation.ID, err))
			continue
		}
		group := lanes[generation.MaterializationPackLane]
		if group == nil {
			group = &laneGroup{
				lane: generation.MaterializationPackLane, teamID: generation.MaterializationTeamID,
				format: generation.FormatGeneration, oldest: generation.CreatedAt,
			}
			lanes[group.lane] = group
			laneOrder = append(laneOrder, group.lane)
		}
		if group.teamID != generation.MaterializationTeamID || group.format != generation.FormatGeneration {
			result.Failed++
			resultErr = errors.Join(resultErr, fmt.Errorf("prepare generation %s: pack lane collision", generation.ID))
			continue
		}
		if group.oldest.IsZero() || !generation.CreatedAt.IsZero() && generation.CreatedAt.Before(group.oldest) {
			group.oldest = generation.CreatedAt
		}
		group.dataBytes += dataBytes
		group.candidates = append(group.candidates, candidate{generation: generation})
	}

	forcedRemaining := w.forcedFlushesPerRun
	now := time.Now().UTC()
	for _, lane := range laneOrder {
		group := lanes[lane]
		mature := group.oldest.IsZero() || !group.oldest.After(now) && now.Sub(group.oldest) >= w.maxDelay
		if group.dataBytes < w.minPackBytes {
			if !mature || forcedRemaining == 0 {
				result.Deferred += len(group.candidates)
				continue
			}
			forcedRemaining--
		}
		members := make([]sandboxstore.RootFSGenerationMaterializationIdentity, len(group.candidates))
		for index, item := range group.candidates {
			members[index] = sandboxstore.RootFSGenerationMaterializationIdentity{
				GenerationID:           item.generation.ID,
				ExpectedLocatorVersion: item.generation.LocatorVersion,
				ExpectedDescriptor:     item.generation.Descriptor,
			}
		}
		batchID, err := sandboxstore.RootFSMaterializationBatchID(group.lane, members)
		if err != nil {
			result.Failed += len(members)
			resultErr = errors.Join(resultErr, fmt.Errorf("identify materialization lane %s: %w", group.lane, err))
			continue
		}
		batch, err := w.store.BeginRootFSGenerationMaterializationBatch(ctx,
			&sandboxstore.BeginRootFSGenerationMaterializationBatchRequest{
				BatchID: batchID, PackLane: group.lane, TeamID: group.teamID,
				FormatGeneration: group.format, Members: members,
			})
		if err != nil {
			if errors.Is(err, sandboxstore.ErrRootFSGenerationConflict) {
				result.Deferred += len(members)
				continue
			}
			result.Failed += len(members)
			resultErr = errors.Join(resultErr, fmt.Errorf("begin materialization batch %s: %w", batchID, err))
			break
		}
		if batch.State == "published" {
			result.Materialized += len(members)
			continue
		}
		result.Batches++
		if err := w.materializeBatch(ctx, batch); err != nil {
			result.Failed += len(members)
			resultErr = errors.Join(resultErr, fmt.Errorf("materialize batch %s: %w", batchID, err))
			var objectFailure *objectPublicationError
			var locatorFailure *locatorPublicationError
			if errors.As(err, &objectFailure) || errors.As(err, &locatorFailure) {
				break
			}
			continue
		}
		result.Materialized += len(members)
	}
	return result, resultErr
}

func (w *Worker) reconcileGarbage(ctx context.Context) (*sandboxstore.RootFSGenerationMaterializationGarbageResult, error) {
	w.garbageMu.Lock()
	defer w.garbageMu.Unlock()
	now := time.Now()
	if !w.lastGarbage.IsZero() && now.Sub(w.lastGarbage) < w.garbageInterval {
		return nil, nil
	}
	result, err := w.store.ReconcileRootFSGenerationMaterializationGarbage(
		ctx, w.uploadingStale, w.terminalRetention, 100,
	)
	if err != nil {
		return nil, fmt.Errorf("reconcile rootfs materialization garbage: %w", err)
	}
	w.lastGarbage = now
	return result, nil
}

func applyGarbageResult(result *Result, garbage *sandboxstore.RootFSGenerationMaterializationGarbageResult) {
	if result == nil || garbage == nil {
		return
	}
	result.Abandoned = garbage.AbandonedBatches
	result.Purged = garbage.PurgedBatches
	result.Enqueued = garbage.EnqueuedObjects
}

// Run continuously retries bounded passes until cancellation. Failures use the
// regular interval rather than an unbounded tight loop during an outage.
func (w *Worker) Run(ctx context.Context, report func(Result, error)) {
	for {
		result, err := w.RunOnce(ctx)
		if report != nil {
			report(result, err)
		}
		timer := time.NewTimer(w.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (w *Worker) materializeBatch(
	ctx context.Context,
	batch *sandboxstore.RootFSGenerationMaterializationBatch,
) error {
	if batch == nil || batch.State != "uploading" || len(batch.Members) == 0 ||
		batch.PackLane == "" || batch.TeamID == "" {
		return fmt.Errorf("materialization batch is incomplete or not uploading")
	}
	inputs := make([]rootfsblock.BatchIncrementalInput, len(batch.Members))
	for index, member := range batch.Members {
		descriptor, err := rootfsblock.DecodeDescriptor(member.ExpectedDescriptor)
		if err != nil || descriptor.CompositeTail == nil {
			return fmt.Errorf("decode composite descriptor for %s: %w", member.GenerationID, err)
		}
		inputs[index] = rootfsblock.BatchIncrementalInput{ID: member.GenerationID, Descriptor: descriptor}
	}
	prefixDigest := sha256.Sum256([]byte(batch.PackLane))
	built, err := rootfsblock.BuildIncrementalGenerationsBatch(
		ctx, w.source, inputs,
		materializationBatchPublisher{
			batchID: batch.BatchID, store: w.store, publisher: w.publisher,
		},
		rootfsblock.BuildOptions{ObjectPrefix: fmt.Sprintf(
			"rootfs/v1/materialized/lanes/sha256/%x", prefixDigest,
		)},
	)
	if err != nil {
		return &objectPublicationError{fmt.Errorf("publish shared materialization objects: %w", err)}
	}
	publication := &sandboxstore.PublishRootFSGenerationMaterializationBatchRequest{
		BatchID: batch.BatchID,
		Members: make([]sandboxstore.RootFSGenerationMaterializationPublication, len(batch.Members)),
	}
	for index, member := range batch.Members {
		generationResult, found := built.Results[member.GenerationID]
		if !found {
			return fmt.Errorf("batch result for generation %s is missing", member.GenerationID)
		}
		publication.Members[index] = sandboxstore.RootFSGenerationMaterializationPublication{
			GenerationID: member.GenerationID, ExpectedLocatorVersion: member.ExpectedLocatorVersion,
			ExpectedDescriptor:     member.ExpectedDescriptor,
			MaterializedDescriptor: generationResult.Payload,
			References:             generationResult.References,
		}
	}
	if err := w.store.PublishRootFSGenerationMaterializationBatch(ctx, publication); err != nil {
		return &locatorPublicationError{fmt.Errorf("publish materialized locator batch: %w", err)}
	}
	return nil
}

type materializationBatchPublisher struct {
	batchID   string
	store     GenerationStore
	publisher rootfsblock.ImmutableObjectPublisher
}

func (p materializationBatchPublisher) PutImmutable(ctx context.Context, key string, payload []byte) error {
	kind := rootfsblock.ObjectKindMappingPage
	if strings.Contains(key, "/packs/") {
		kind = rootfsblock.ObjectKindDataPack
	}
	reference := rootfsblock.ObjectReference{
		Key: key, Kind: kind, Size: int64(len(payload)), Checksum: digest.FromBytes(payload).String(),
	}
	if err := p.store.RegisterRootFSGenerationMaterializationBatchObject(ctx, p.batchID, reference); err != nil {
		return err
	}
	if err := p.publisher.PutImmutable(ctx, key, payload); err != nil {
		return err
	}
	if err := p.store.MarkRootFSGenerationMaterializationBatchObjectUploaded(ctx, p.batchID, key); err != nil {
		return err
	}
	return nil
}

func estimateCompositeFinalDataBytes(payload []byte) (int64, error) {
	descriptor, err := rootfsblock.DecodeDescriptor(payload)
	if err != nil || descriptor.CompositeTail == nil {
		return 0, fmt.Errorf("descriptor must be composite durable: %v", err)
	}
	updates, _, err := rootfsblock.DecodeCompositeTail(
		*descriptor.CompositeTail,
		uint64(descriptor.LogicalSizeBytes/rootfsblock.LogicalBlockSize),
	)
	if err != nil {
		return 0, err
	}
	final := make(map[uint64][]byte, len(updates))
	for _, update := range updates {
		final[update.Block] = update.Data
	}
	zero := make([]byte, rootfsblock.LogicalBlockSize)
	var size int64
	for _, data := range final {
		if !bytes.Equal(data, zero) {
			size += int64(len(data))
		}
	}
	return size, nil
}
