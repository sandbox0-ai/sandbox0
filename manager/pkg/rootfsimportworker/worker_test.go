package rootfsimportworker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
)

const testProcdProtocol = "sandbox0.procd.v1"

var testProcdDigest = digest.FromString("production-procd").String()

type fakeStore struct {
	mu sync.Mutex

	operation      *sandboxstore.RootFSImportOperation
	leaseErr       error
	renewErr       error
	releaseErr     error
	abandonErr     error
	publishErrors  []error
	garbage        *sandboxstore.RootFSImportGarbageResult
	garbageErr     error
	renewed        chan struct{}
	renewCalls     int
	releaseCalls   int
	abandonReasons []string
	publishCalls   int
	prepareRefs    []rootfsblock.ObjectReference
	publishedRefs  []rootfsblock.ObjectReference
	garbageCalls   int
}

func (s *fakeStore) LeaseNextRootFSImport(context.Context, string, time.Duration) (*sandboxstore.RootFSImportOperation, error) {
	return s.operation, s.leaseErr
}

func (s *fakeStore) RenewRootFSImportLease(
	_ context.Context,
	lease sandboxstore.RootFSImportLease,
	ttl time.Duration,
) (sandboxstore.RootFSImportLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewCalls++
	if s.renewed != nil && s.renewCalls == 1 {
		close(s.renewed)
	}
	if s.renewErr != nil {
		return sandboxstore.RootFSImportLease{}, s.renewErr
	}
	lease.ExpiresAt = time.Now().Add(ttl)
	return lease, nil
}

func (s *fakeStore) ReleaseRootFSImportLease(context.Context, sandboxstore.RootFSImportLease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCalls++
	return s.releaseErr
}

func (s *fakeStore) AbandonRootFSImport(_ context.Context, _ sandboxstore.RootFSImportLease, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.abandonReasons = append(s.abandonReasons, reason)
	return s.abandonErr
}

func (s *fakeStore) PrepareRootFSImportObject(
	_ context.Context,
	_ sandboxstore.RootFSImportLease,
	reference rootfsblock.ObjectReference,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prepareRefs = append(s.prepareRefs, reference)
	return nil
}

func (s *fakeStore) MarkRootFSImportObjectPublished(
	_ context.Context,
	_ sandboxstore.RootFSImportLease,
	reference rootfsblock.ObjectReference,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishedRefs = append(s.publishedRefs, reference)
	return nil
}

func (s *fakeStore) PublishReadyRootFSImport(
	context.Context,
	*sandboxstore.PublishReadyRootFSImportRequest,
) (*sandboxstore.RootFSBaseArtifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.publishCalls++
	if len(s.publishErrors) >= s.publishCalls && s.publishErrors[s.publishCalls-1] != nil {
		return nil, s.publishErrors[s.publishCalls-1]
	}
	return &sandboxstore.RootFSBaseArtifact{ArtifactDigest: digest.FromString("ready").String()}, nil
}

func (s *fakeStore) ReconcileRootFSImportGarbage(
	context.Context,
	time.Duration,
	int,
) (*sandboxstore.RootFSImportGarbageResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.garbageCalls++
	return s.garbage, s.garbageErr
}

func testOperation(attempts int) *sandboxstore.RootFSImportOperation {
	return &sandboxstore.RootFSImportOperation{
		ID: "rootfs.import.test", State: sandboxstore.RootFSImportStateBuilding,
		SourceOCIDigest: digest.FromString("alpine").String(),
		LeaseOwner:      "manager.test", LeaseToken: strings.Repeat("a", 64),
		LeaseExpiresAt: time.Now().Add(time.Minute), AttemptCount: attempts,
		Spec: rootfsimporter.OperationSpec{
			SourceOCIRef:     "docker.io/library/alpine@" + digest.FromString("alpine").String(),
			Platform:         rootfsimporter.ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"},
			FormatGeneration: 1, ProcdProtocol: testProcdProtocol, ProcdDigest: testProcdDigest,
			LogicalSizeBytes: 300 << 20,
			BlockOptions: rootfsblock.BuildOptions{
				DataRangeBytes: rootfsblock.DefaultDataRangeBytes,
				PackBytes:      rootfsblock.DefaultPackBytes, PageEntries: rootfsblock.DefaultPageEntries,
				ObjectPrefix: "rootfs/import/test",
			},
		},
	}
}

func newTestWorker(t *testing.T, store *fakeStore, builder OperationBuilder, mutate func(*Config)) *Worker {
	t.Helper()
	config := Config{
		Store: store, Builder: builder, WorkerID: "manager.test",
		Interval: 10 * time.Millisecond, LeaseTTL: 5 * time.Second, LeaseRenewal: 10 * time.Millisecond,
		MaxAttempts: 3, GarbageInterval: time.Second, TerminalRetention: time.Minute,
		GarbageLimit: 10, ProcdProtocol: testProcdProtocol, ProcdDigest: testProcdDigest,
	}
	if mutate != nil {
		mutate(&config)
	}
	worker, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestWorkerPublishesReadyWithFreshLeaseAndCommitLossRetry(t *testing.T) {
	store := &fakeStore{
		operation: testOperation(1),
		garbage: &sandboxstore.RootFSImportGarbageResult{
			RecoveredLeases: 1, PurgedReady: 2, PurgedAbandoned: 3, EnqueuedObjects: 4,
		},
		publishErrors: []error{errors.New("commit response unavailable"), nil},
	}
	builder := OperationBuilderFunc(func(
		context.Context,
		*sandboxstore.RootFSImportOperation,
		sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		return rootfsimporter.BuildResult{}, nil
	})
	result, err := newTestWorker(t, store, builder, nil).RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Ready != 1 || result.RecoveredLeases != 1 || result.PurgedReady != 2 ||
		result.PurgedAbandoned != 3 || result.EnqueuedObjects != 4 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if store.renewCalls != 1 || store.publishCalls != 2 || store.releaseCalls != 0 || len(store.abandonReasons) != 0 {
		t.Fatalf("renew=%d publish=%d release=%d abandon=%v", store.renewCalls, store.publishCalls, store.releaseCalls, store.abandonReasons)
	}
}

func TestWorkerReleasesTransientBuildWithoutLoggingCause(t *testing.T) {
	store := &fakeStore{operation: testOperation(1)}
	secret := "registry-password-and-/var/lib/private/staging"
	builder := OperationBuilderFunc(func(
		context.Context,
		*sandboxstore.RootFSImportOperation,
		sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		return rootfsimporter.BuildResult{}, errors.New(secret)
	})
	result, err := newTestWorker(t, store, builder, nil).RunOnce(context.Background())
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("error must be sanitized, got %v", err)
	}
	if result.Released != 1 || result.FailureCategory != failureBuild || store.releaseCalls != 1 {
		t.Fatalf("unexpected release result: %#v calls=%d", result, store.releaseCalls)
	}
}

func TestWorkerBoundsBuildAndRejectsLateSuccess(t *testing.T) {
	store := &fakeStore{operation: testOperation(1)}
	builder := OperationBuilderFunc(func(
		ctx context.Context,
		_ *sandboxstore.RootFSImportOperation,
		_ sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		<-ctx.Done()
		return rootfsimporter.BuildResult{}, nil
	})
	result, err := newTestWorker(t, store, builder, func(config *Config) {
		config.BuildTimeout = 30 * time.Millisecond
	}).RunOnce(context.Background())
	if err == nil || result.Released != 1 || result.FailureCategory != failureBuildTimeout ||
		store.releaseCalls != 1 || store.publishCalls != 0 {
		t.Fatalf("result=%#v release=%d publish=%d err=%v",
			result, store.releaseCalls, store.publishCalls, err)
	}
}

func TestWorkerAbandonsExhaustedBuildWithBoundedReason(t *testing.T) {
	store := &fakeStore{operation: testOperation(3)}
	builder := OperationBuilderFunc(func(
		context.Context,
		*sandboxstore.RootFSImportOperation,
		sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		return rootfsimporter.BuildResult{}, errors.New("untrusted remote detail")
	})
	result, err := newTestWorker(t, store, builder, nil).RunOnce(context.Background())
	if err == nil || result.Abandoned != 1 || len(store.abandonReasons) != 1 {
		t.Fatalf("result=%#v reasons=%v err=%v", result, store.abandonReasons, err)
	}
	if strings.Contains(store.abandonReasons[0], "untrusted") {
		t.Fatalf("untrusted cause entered durable reason: %q", store.abandonReasons[0])
	}
}

func TestWorkerCancelsBuildAndLeavesLeaseOnRenewalUncertainty(t *testing.T) {
	store := &fakeStore{
		operation: testOperation(1), renewed: make(chan struct{}), renewErr: errors.New("database timeout"),
	}
	builder := OperationBuilderFunc(func(
		ctx context.Context,
		_ *sandboxstore.RootFSImportOperation,
		_ sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		<-ctx.Done()
		return rootfsimporter.BuildResult{}, ctx.Err()
	})
	result, err := newTestWorker(t, store, builder, nil).RunOnce(context.Background())
	if err == nil || result.LeaseUncertain != 1 || store.releaseCalls != 0 || len(store.abandonReasons) != 0 {
		t.Fatalf("result=%#v release=%d abandon=%v err=%v", result, store.releaseCalls, store.abandonReasons, err)
	}
}

func TestWorkerAbandonsIncompatibleExecutableBeforeBuild(t *testing.T) {
	operation := testOperation(1)
	operation.Spec.ProcdProtocol = "sandbox0.procd.v2"
	store := &fakeStore{operation: operation}
	called := false
	builder := OperationBuilderFunc(func(
		context.Context,
		*sandboxstore.RootFSImportOperation,
		sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		called = true
		return rootfsimporter.BuildResult{}, nil
	})
	result, err := newTestWorker(t, store, builder, nil).RunOnce(context.Background())
	if err == nil || called || result.Abandoned != 1 || result.FailureCategory != failureIncompatible {
		t.Fatalf("called=%v result=%#v err=%v", called, result, err)
	}
}

func TestWorkerAbandonsImmutablePublicationConflict(t *testing.T) {
	store := &fakeStore{
		operation:     testOperation(1),
		publishErrors: []error{sandboxstore.ErrRootFSImportConflict, sandboxstore.ErrRootFSImportConflict},
	}
	builder := OperationBuilderFunc(func(
		context.Context,
		*sandboxstore.RootFSImportOperation,
		sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		return rootfsimporter.BuildResult{}, nil
	})
	result, err := newTestWorker(t, store, builder, nil).RunOnce(context.Background())
	if err == nil || result.Abandoned != 1 || result.FailureCategory != failurePublication {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestWorkerSkipsGarbageUntilBoundedInterval(t *testing.T) {
	store := &fakeStore{}
	builder := OperationBuilderFunc(func(
		context.Context,
		*sandboxstore.RootFSImportOperation,
		sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		return rootfsimporter.BuildResult{}, nil
	})
	worker := newTestWorker(t, store, builder, nil)
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.garbageCalls != 1 {
		t.Fatalf("garbage calls = %d, want 1", store.garbageCalls)
	}
}

func TestNewWorkerRejectsUnsafeLeaseAndExecutableContracts(t *testing.T) {
	store := &fakeStore{}
	builder := OperationBuilderFunc(func(
		context.Context,
		*sandboxstore.RootFSImportOperation,
		sandboxstore.RootFSImportLease,
	) (rootfsimporter.BuildResult, error) {
		return rootfsimporter.BuildResult{}, nil
	})
	for name, mutate := range map[string]func(*Config){
		"worker":        func(c *Config) { c.WorkerID = "manager worker" },
		"build timeout": func(c *Config) { c.BuildTimeout = 24*time.Hour + time.Millisecond },
		"renewal":       func(c *Config) { c.LeaseRenewal = c.LeaseTTL },
		"attempts":      func(c *Config) { c.MaxAttempts = 101 },
		"protocol":      func(c *Config) { c.ProcdProtocol = "bad protocol" },
		"digest":        func(c *Config) { c.ProcdDigest = "sha256:bad" },
	} {
		t.Run(name, func(t *testing.T) {
			config := Config{
				Store: store, Builder: builder, WorkerID: "manager.test",
				LeaseTTL: 5 * time.Second, LeaseRenewal: time.Second,
				ProcdProtocol: testProcdProtocol, ProcdDigest: testProcdDigest,
			}
			mutate(&config)
			if _, err := New(config); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
