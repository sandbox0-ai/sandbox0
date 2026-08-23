// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package rootfsimportworker converts durable OCI import operations into
// attested immutable RootFS base artifacts.
package rootfsimportworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

const (
	DefaultInterval          = time.Second
	DefaultBuildTimeout      = 2 * time.Hour
	DefaultLeaseTTL          = 2 * time.Minute
	DefaultLeaseRenewal      = 30 * time.Second
	DefaultMaxAttempts       = 5
	DefaultGarbageInterval   = time.Minute
	DefaultTerminalRetention = 24 * time.Hour
	DefaultGarbageLimit      = 100
)

// Store is the region-authoritative operation, lease, object journal, and
// ready-artifact boundary used by active-active workers.
type Store interface {
	LeaseNextRootFSImport(context.Context, string, time.Duration) (*sandboxstore.RootFSImportOperation, error)
	RenewRootFSImportLease(context.Context, sandboxstore.RootFSImportLease, time.Duration) (sandboxstore.RootFSImportLease, error)
	ReleaseRootFSImportLease(context.Context, sandboxstore.RootFSImportLease) error
	AbandonRootFSImport(context.Context, sandboxstore.RootFSImportLease, string) error
	PrepareRootFSImportObject(context.Context, sandboxstore.RootFSImportLease, rootfsblock.ObjectReference) error
	MarkRootFSImportObjectPublished(context.Context, sandboxstore.RootFSImportLease, rootfsblock.ObjectReference) error
	PublishReadyRootFSImport(context.Context, *sandboxstore.PublishReadyRootFSImportRequest) (*sandboxstore.RootFSBaseArtifact, error)
	ReconcileRootFSImportGarbage(context.Context, time.Duration, int) (*sandboxstore.RootFSImportGarbageResult, error)
}

// OperationBuilder performs the local OCI, XFS, and immutable object work for
// one fenced operation. It must not publish ready metadata itself.
type OperationBuilder interface {
	Build(context.Context, *sandboxstore.RootFSImportOperation, sandboxstore.RootFSImportLease) (rootfsimporter.BuildResult, error)
}

// OperationBuilderFunc adapts a function to OperationBuilder.
type OperationBuilderFunc func(context.Context, *sandboxstore.RootFSImportOperation, sandboxstore.RootFSImportLease) (rootfsimporter.BuildResult, error)

func (f OperationBuilderFunc) Build(
	ctx context.Context,
	operation *sandboxstore.RootFSImportOperation,
	lease sandboxstore.RootFSImportLease,
) (rootfsimporter.BuildResult, error) {
	return f(ctx, operation, lease)
}

// Config defines bounded worker behavior and the only executable compatibility
// contract this process is allowed to publish.
type Config struct {
	Store             Store
	Builder           OperationBuilder
	WorkerID          string
	Interval          time.Duration
	BuildTimeout      time.Duration
	LeaseTTL          time.Duration
	LeaseRenewal      time.Duration
	MaxAttempts       int
	GarbageInterval   time.Duration
	TerminalRetention time.Duration
	GarbageLimit      int
	ProcdProtocol     string
	ProcdDigest       string
}

// Result describes one bounded worker pass without including remote errors,
// credentials, or local staging paths.
type Result struct {
	Leased          int
	Ready           int
	Released        int
	Abandoned       int
	LeaseUncertain  int
	Failed          int
	RecoveredLeases int
	PurgedReady     int
	PurgedAbandoned int
	EnqueuedObjects int
	FailureCategory string
	OperationID     string
}

const (
	failureIncompatible = "incompatible_contract"
	failureBuild        = "build_failed"
	failureBuildTimeout = "build_timeout"
	failureLease        = "lease_uncertain"
	failurePublication  = "ready_publication_failed"
)

// AttemptError intentionally exposes only a bounded category and operation
// identity. Untrusted registry errors and local staging paths never enter logs.
type AttemptError struct {
	OperationID string
	Category    string
}

func (e *AttemptError) Error() string {
	if e == nil {
		return "rootfs import attempt failed"
	}
	return fmt.Sprintf("rootfs import operation %s failed (%s)", e.OperationID, e.Category)
}

// Worker owns one local build lane. PostgreSQL leasing allows any number of
// manager replicas to run independent Worker instances safely.
type Worker struct {
	store             Store
	builder           OperationBuilder
	workerID          string
	interval          time.Duration
	buildTimeout      time.Duration
	leaseTTL          time.Duration
	leaseRenewal      time.Duration
	maxAttempts       int
	garbageInterval   time.Duration
	terminalRetention time.Duration
	garbageLimit      int
	procdProtocol     string
	procdDigest       string

	garbageMu   sync.Mutex
	lastGarbage time.Time
}

// New validates all retry and compatibility bounds before a worker can lease
// an operation.
func New(config Config) (*Worker, error) {
	if config.Store == nil || config.Builder == nil {
		return nil, fmt.Errorf("rootfs import store and builder are required")
	}
	workerID, err := sandboxstore.NormalizeRootFSImportWorkerID(config.WorkerID)
	if err != nil {
		return nil, err
	}
	if config.Interval == 0 {
		config.Interval = DefaultInterval
	}
	if config.BuildTimeout == 0 {
		config.BuildTimeout = DefaultBuildTimeout
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = DefaultLeaseTTL
	}
	if config.LeaseRenewal == 0 {
		config.LeaseRenewal = DefaultLeaseRenewal
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = DefaultMaxAttempts
	}
	if config.GarbageInterval == 0 {
		config.GarbageInterval = DefaultGarbageInterval
	}
	if config.TerminalRetention == 0 {
		config.TerminalRetention = DefaultTerminalRetention
	}
	if config.GarbageLimit == 0 {
		config.GarbageLimit = DefaultGarbageLimit
	}
	if config.Interval < 10*time.Millisecond || config.Interval > time.Minute || config.Interval%time.Millisecond != 0 {
		return nil, fmt.Errorf("rootfs import interval must be whole milliseconds within 10ms..1m")
	}
	if config.BuildTimeout < 10*time.Millisecond || config.BuildTimeout > 24*time.Hour ||
		config.BuildTimeout%time.Millisecond != 0 {
		return nil, fmt.Errorf("rootfs import build timeout must be whole milliseconds within 10ms..24h")
	}
	if config.LeaseTTL < sandboxstore.MinRootFSImportLeaseTTL ||
		config.LeaseTTL > sandboxstore.MaxRootFSImportLeaseTTL || config.LeaseTTL%time.Millisecond != 0 {
		return nil, fmt.Errorf("rootfs import lease TTL is outside the durable store bounds")
	}
	if config.LeaseRenewal < 10*time.Millisecond || config.LeaseRenewal > config.LeaseTTL/2 ||
		config.LeaseRenewal%time.Millisecond != 0 {
		return nil, fmt.Errorf("rootfs import lease renewal must be whole milliseconds within 10ms..leaseTTL/2")
	}
	if config.MaxAttempts < 1 || config.MaxAttempts > 100 {
		return nil, fmt.Errorf("rootfs import max attempts must be within 1..100")
	}
	if config.GarbageInterval < time.Second || config.GarbageInterval > 24*time.Hour ||
		config.GarbageInterval%time.Millisecond != 0 {
		return nil, fmt.Errorf("rootfs import garbage interval must be whole milliseconds within 1s..24h")
	}
	if config.TerminalRetention < time.Minute || config.TerminalRetention > 30*24*time.Hour ||
		config.TerminalRetention%time.Millisecond != 0 {
		return nil, fmt.Errorf("rootfs import terminal retention must be whole milliseconds within 1m..30d")
	}
	if config.GarbageLimit < 1 || config.GarbageLimit > sandboxstore.MaxRootFSImportListLimit {
		return nil, fmt.Errorf("rootfs import garbage limit must be within 1..%d", sandboxstore.MaxRootFSImportListLimit)
	}
	if err := rootfsimporter.ValidateProcdProtocol(config.ProcdProtocol); err != nil {
		return nil, err
	}
	parsedProcdDigest, err := digest.Parse(config.ProcdDigest)
	if err != nil || rootfsimporter.ValidateArtifactSHA256Digest(parsedProcdDigest) != nil ||
		parsedProcdDigest.String() != config.ProcdDigest {
		return nil, fmt.Errorf("rootfs import procd digest must be canonical SHA-256")
	}
	return &Worker{
		store: config.Store, builder: config.Builder, workerID: workerID,
		interval: config.Interval, buildTimeout: config.BuildTimeout,
		leaseTTL: config.LeaseTTL, leaseRenewal: config.LeaseRenewal,
		maxAttempts: config.MaxAttempts, garbageInterval: config.GarbageInterval,
		terminalRetention: config.TerminalRetention, garbageLimit: config.GarbageLimit,
		procdProtocol: config.ProcdProtocol, procdDigest: parsedProcdDigest.String(),
	}, nil
}

// RunOnce reconciles bounded garbage and processes at most one operation.
func (w *Worker) RunOnce(ctx context.Context) (Result, error) {
	if w == nil || w.store == nil || w.builder == nil {
		return Result{}, fmt.Errorf("rootfs import worker is not configured")
	}
	result := Result{}
	garbage, err := w.reconcileGarbage(ctx)
	if err != nil {
		return result, fmt.Errorf("reconcile rootfs import garbage: %w", err)
	}
	applyGarbage(&result, garbage)
	operation, err := w.store.LeaseNextRootFSImport(ctx, w.workerID, w.leaseTTL)
	if err != nil {
		return result, fmt.Errorf("lease rootfs import operation: %w", err)
	}
	if operation == nil {
		return result, nil
	}
	result.Leased = 1
	result.OperationID = operation.ID
	lease, err := operation.Lease()
	if err != nil {
		result.Failed = 1
		result.LeaseUncertain = 1
		result.FailureCategory = failureLease
		return result, &AttemptError{OperationID: operation.ID, Category: failureLease}
	}
	if operation.Spec.ProcdProtocol != w.procdProtocol || operation.Spec.ProcdDigest != w.procdDigest {
		return w.abandon(ctx, result, lease, failureIncompatible, "worker executable contract is incompatible")
	}
	if operation.AttemptCount > w.maxAttempts {
		return w.abandon(ctx, result, lease, failureBuild, "build retry budget was exhausted")
	}

	buildCtx, cancelBuild := context.WithTimeout(ctx, w.buildTimeout)
	keeper := w.startLeaseKeeper(buildCtx, cancelBuild, lease)
	built, buildErr := w.builder.Build(buildCtx, operation, lease)
	buildContextErr := buildCtx.Err()
	cancelBuild()
	leaseErr := keeper.wait()
	if leaseErr != nil {
		result.Failed = 1
		result.LeaseUncertain = 1
		result.FailureCategory = failureLease
		return result, &AttemptError{OperationID: operation.ID, Category: failureLease}
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if buildErr == nil && buildContextErr != nil {
		buildErr = buildContextErr
	}
	if buildErr != nil {
		category := failureBuild
		if errors.Is(buildErr, context.DeadlineExceeded) || errors.Is(buildContextErr, context.DeadlineExceeded) {
			category = failureBuildTimeout
		}
		if operation.AttemptCount >= w.maxAttempts {
			return w.abandon(ctx, result, lease, category, "build retry budget was exhausted")
		}
		return w.release(ctx, result, lease, category)
	}

	// Stop periodic renewal before the final CAS, then acquire a fresh complete
	// TTL window. This avoids a successful CAS racing a renewal that correctly
	// observes the now-terminal operation as lease-lost.
	lease, err = w.store.RenewRootFSImportLease(ctx, lease, w.leaseTTL)
	if err != nil {
		result.Failed = 1
		result.LeaseUncertain = 1
		result.FailureCategory = failureLease
		return result, &AttemptError{OperationID: operation.ID, Category: failureLease}
	}
	publication := &sandboxstore.PublishReadyRootFSImportRequest{Lease: lease, Result: built}
	_, firstErr := w.store.PublishReadyRootFSImport(ctx, publication)
	if firstErr != nil {
		// An exact retry resolves commit-response loss without creating a second
		// artifact or requiring the lease to remain live after a committed CAS.
		_, err = w.store.PublishReadyRootFSImport(ctx, publication)
	}
	if firstErr == nil || err == nil {
		result.Ready = 1
		return result, nil
	}
	publicationErr := err
	if errors.Is(publicationErr, sandboxstore.ErrRootFSImportLeaseLost) {
		result.Failed = 1
		result.LeaseUncertain = 1
		result.FailureCategory = failureLease
		return result, &AttemptError{OperationID: operation.ID, Category: failureLease}
	}
	if errors.Is(publicationErr, sandboxstore.ErrRootFSImportConflict) ||
		errors.Is(publicationErr, sandboxstore.ErrRootFSBaseArtifactConflict) ||
		operation.AttemptCount >= w.maxAttempts {
		return w.abandon(ctx, result, lease, failurePublication, "ready publication was rejected")
	}
	return w.release(ctx, result, lease, failurePublication)
}

func (w *Worker) release(
	ctx context.Context,
	result Result,
	lease sandboxstore.RootFSImportLease,
	category string,
) (Result, error) {
	if err := w.store.ReleaseRootFSImportLease(ctx, lease); err != nil {
		result.Failed = 1
		result.LeaseUncertain = 1
		result.FailureCategory = failureLease
		return result, &AttemptError{OperationID: lease.OperationID, Category: failureLease}
	}
	result.Failed = 1
	result.Released = 1
	result.FailureCategory = category
	return result, &AttemptError{OperationID: lease.OperationID, Category: category}
}

func (w *Worker) abandon(
	ctx context.Context,
	result Result,
	lease sandboxstore.RootFSImportLease,
	category string,
	reason string,
) (Result, error) {
	if err := w.store.AbandonRootFSImport(ctx, lease, reason); err != nil {
		result.Failed = 1
		result.LeaseUncertain = 1
		result.FailureCategory = failureLease
		return result, &AttemptError{OperationID: lease.OperationID, Category: failureLease}
	}
	result.Failed = 1
	result.Abandoned = 1
	result.FailureCategory = category
	return result, &AttemptError{OperationID: lease.OperationID, Category: category}
}

type leaseKeeper struct {
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func (w *Worker) startLeaseKeeper(
	ctx context.Context,
	cancelBuild context.CancelFunc,
	lease sandboxstore.RootFSImportLease,
) *leaseKeeper {
	keeper := &leaseKeeper{done: make(chan struct{})}
	go func() {
		defer close(keeper.done)
		ticker := time.NewTicker(w.leaseRenewal)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewTimeout := min(w.leaseRenewal, w.leaseTTL/4)
				renewCtx, cancel := context.WithTimeout(ctx, renewTimeout)
				_, err := w.store.RenewRootFSImportLease(renewCtx, lease, w.leaseTTL)
				cancel()
				if err != nil {
					keeper.mu.Lock()
					keeper.err = err
					keeper.mu.Unlock()
					cancelBuild()
					return
				}
			}
		}
	}()
	return keeper
}

func (k *leaseKeeper) wait() error {
	if k == nil {
		return fmt.Errorf("rootfs import lease keeper is missing")
	}
	<-k.done
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.err
}

func (w *Worker) reconcileGarbage(ctx context.Context) (*sandboxstore.RootFSImportGarbageResult, error) {
	w.garbageMu.Lock()
	defer w.garbageMu.Unlock()
	now := time.Now()
	if !w.lastGarbage.IsZero() && now.Sub(w.lastGarbage) < w.garbageInterval {
		return nil, nil
	}
	result, err := w.store.ReconcileRootFSImportGarbage(ctx, w.terminalRetention, w.garbageLimit)
	if err != nil {
		return nil, err
	}
	w.lastGarbage = now
	return result, nil
}

func applyGarbage(result *Result, garbage *sandboxstore.RootFSImportGarbageResult) {
	if result == nil || garbage == nil {
		return
	}
	result.RecoveredLeases = garbage.RecoveredLeases
	result.PurgedReady = garbage.PurgedReady
	result.PurgedAbandoned = garbage.PurgedAbandoned
	result.EnqueuedObjects = garbage.EnqueuedObjects
}

// Run continuously performs bounded passes. The report callback receives only
// sanitized AttemptError values for operation-local failures.
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
