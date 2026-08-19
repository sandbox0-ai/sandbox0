package rootfsmaterializer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const (
	DefaultScanLimit = 1_000
	DefaultInterval  = time.Second
)

type GenerationStore interface {
	ListCompositeRootFSGenerations(context.Context, int) ([]sandboxstore.RootFSGeneration, error)
	PublishRootFSGenerationMaterialization(context.Context, *sandboxstore.RootFSGenerationMaterialization) error
}

type Config struct {
	Store     GenerationStore
	Source    rootfsblock.RangeSource
	Publisher rootfsblock.ImmutableObjectPublisher
	ScanLimit int
	Interval  time.Duration
}

// Result describes one bounded materializer pass.
type Result struct {
	Scanned      int
	Materialized int
	Failed       int
}

// Worker converts region-durable PostgreSQL composite tails into immutable S3
// block mappings. Object writes are content addressed and idempotent; the
// store's locator CAS owns concurrency between replicas.
type Worker struct {
	store     GenerationStore
	source    rootfsblock.RangeSource
	publisher rootfsblock.ImmutableObjectPublisher
	scanLimit int
	interval  time.Duration
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
	if config.ScanLimit < 1 || config.ScanLimit > 10_000 {
		return nil, fmt.Errorf("materializer scan limit must be between 1 and 10000")
	}
	if config.Interval < 10*time.Millisecond {
		return nil, fmt.Errorf("materializer interval must be at least 10ms")
	}
	return &Worker{
		store: config.Store, source: config.Source, publisher: config.Publisher,
		scanLimit: config.ScanLimit, interval: config.Interval,
	}, nil
}

// RunOnce scans a bounded oldest-first batch. A corrupt or transiently failed
// generation does not starve later candidates in the same pass; the aggregate
// error preserves every failure for logs and retry.
func (w *Worker) RunOnce(ctx context.Context) (Result, error) {
	generations, err := w.store.ListCompositeRootFSGenerations(ctx, w.scanLimit)
	if err != nil {
		return Result{}, err
	}
	result := Result{Scanned: len(generations)}
	var resultErr error
	for index := range generations {
		generation := generations[index]
		if err := w.materialize(ctx, &generation); err != nil {
			result.Failed++
			resultErr = errors.Join(resultErr, fmt.Errorf("materialize generation %s: %w", generation.ID, err))
			var objectFailure *objectPublicationError
			var locatorFailure *locatorPublicationError
			if errors.As(err, &objectFailure) ||
				errors.As(err, &locatorFailure) && !errors.Is(err, sandboxstore.ErrRootFSGenerationConflict) {
				// A shared S3 or PostgreSQL outage would make every remaining
				// candidate fail. Stop this pass to avoid an outage request storm.
				break
			}
			continue
		}
		result.Materialized++
	}
	return result, resultErr
}

// Run continuously retries bounded passes until cancellation. Failures use the
// regular interval rather than an unbounded tight loop during an S3 outage.
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

func (w *Worker) materialize(ctx context.Context, generation *sandboxstore.RootFSGeneration) error {
	if generation == nil || generation.DurabilityState != sandboxstore.RootFSGenerationStateCompositeDurable {
		return fmt.Errorf("candidate is not composite durable")
	}
	descriptor, err := rootfsblock.DecodeDescriptor(generation.Descriptor)
	if err != nil {
		return fmt.Errorf("decode composite descriptor: %w", err)
	}
	if descriptor.CompositeTail == nil {
		return fmt.Errorf("composite generation has no tail")
	}
	built, err := rootfsblock.BuildIncrementalGeneration(
		ctx, w.source, descriptor, nil, w.publisher, rootfsblock.BuildOptions{},
	)
	if err != nil {
		return &objectPublicationError{fmt.Errorf("publish materialized block mapping: %w", err)}
	}
	if err := w.store.PublishRootFSGenerationMaterialization(ctx, &sandboxstore.RootFSGenerationMaterialization{
		GenerationID: generation.ID, ExpectedLocatorVersion: generation.LocatorVersion,
		ExpectedDescriptor: generation.Descriptor, MaterializedDescriptor: built.Payload,
	}); err != nil {
		return &locatorPublicationError{fmt.Errorf("publish materialized locator: %w", err)}
	}
	return nil
}
