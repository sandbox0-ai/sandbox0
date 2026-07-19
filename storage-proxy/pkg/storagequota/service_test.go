package storagequota

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

type fakeStore struct {
	mu sync.Mutex

	policies        map[teamquota.Key]*teamquota.Policy
	reserveErr      error
	beforeReserve   func(*fakeStore, teamquota.DeltaRequest)
	reserveTargets  []teamquota.Values
	committed       teamquota.Values
	pending         teamquota.Values
	reconciled      []teamquota.Values
	reconcileCAS    []int64
	commitCalled    bool
	exactCommits    []teamquota.Values
	observedExact   []teamquota.Values
	aborted         bool
	beginRequests   []teamquota.ReleaseRequest
	confirmed       bool
	recovery        *teamquota.RecoveryAllocation
	recoveryFilters []teamquota.RecoveryAllocationFilter
}

func (f *fakeStore) EffectivePolicy(_ context.Context, _ string, key teamquota.Key) (*teamquota.Policy, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.policies[key], nil
}

func (f *fakeStore) ReserveDelta(_ context.Context, req teamquota.DeltaRequest) (*teamquota.Reservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reserveErr != nil {
		return nil, f.reserveErr
	}
	if f.beforeReserve != nil {
		f.beforeReserve(f, req)
	}
	committed := f.committed.Clone()
	if committed == nil {
		committed = req.Observed.Clone()
	}
	if committed == nil {
		committed = make(teamquota.Values, len(req.Delta))
	}
	target := committed.Clone()
	for key, delta := range req.Delta {
		target[key] += delta
	}
	f.pending = target.Clone()
	f.reserveTargets = append(f.reserveTargets, target.Clone())
	return &teamquota.Reservation{
		Owner:     req.Owner,
		Operation: req.Operation,
		Committed: committed,
		Target:    target,
		Reserved:  req.Delta.Clone(),
	}, nil
}

func (f *fakeStore) Commit(context.Context, teamquota.OperationRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commitCalled = true
	if f.pending != nil {
		f.committed = f.pending.Clone()
		f.pending = nil
	}
	return nil
}

func (f *fakeStore) CommitExact(
	_ context.Context,
	_ teamquota.OperationRef,
	exact teamquota.Values,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	maximum := f.pending
	if maximum == nil && f.recovery != nil {
		maximum = f.recovery.Pending
	}
	for key, value := range exact {
		if maximum == nil || value > maximum[key] {
			return errors.New("exact target exceeds pending value")
		}
	}
	f.exactCommits = append(f.exactCommits, exact.Clone())
	f.committed = exact.Clone()
	f.pending = nil
	return nil
}

func (f *fakeStore) CommitObservedExact(
	_ context.Context,
	_ teamquota.OperationRef,
	exact teamquota.Values,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observedExact = append(f.observedExact, exact.Clone())
	f.committed = exact.Clone()
	f.pending = nil
	if f.recovery != nil {
		f.recovery.Committed = exact.Clone()
		f.recovery.Pending = nil
		f.recovery.Operation = nil
		f.recovery.State = "active"
		f.recovery.Revision++
	}
	return nil
}

func (f *fakeStore) Abort(context.Context, teamquota.OperationRef, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aborted = true
	f.pending = nil
	if f.recovery != nil {
		f.recovery.Pending = nil
		f.recovery.Operation = nil
		f.recovery.State = f.recovery.OperationBaseState
		if f.recovery.State == "" {
			f.recovery.State = "active"
		}
		f.recovery.Revision++
	}
	return nil
}

func (f *fakeStore) BeginRelease(_ context.Context, req teamquota.ReleaseRequest) (*teamquota.Reservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beginRequests = append(f.beginRequests, req)
	return &teamquota.Reservation{Owner: req.Owner, Operation: req.Operation, Target: req.Target}, nil
}

func (f *fakeStore) ConfirmRelease(context.Context, teamquota.OperationRef, teamquota.RuntimeRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirmed = true
	return nil
}

func (f *fakeStore) ConfirmReleaseTx(context.Context, pgx.Tx, teamquota.OperationRef, teamquota.RuntimeRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirmed = true
	return nil
}

func (f *fakeStore) ReconcileTargetIfRevision(
	_ context.Context,
	owner teamquota.Owner,
	target teamquota.Values,
	_ teamquota.RuntimeRef,
	expectedRevision int64,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconcileCAS = append(f.reconcileCAS, expectedRevision)
	currentRevision := int64(0)
	if f.recovery != nil {
		currentRevision = f.recovery.Revision
	}
	if currentRevision != expectedRevision {
		return false, nil
	}
	f.reconciled = append(f.reconciled, target.Clone())
	f.committed = target.Clone()
	f.recovery = &teamquota.RecoveryAllocation{
		Owner:     owner,
		Revision:  currentRevision + 1,
		Committed: target.Clone(),
	}
	return true, nil
}

func (f *fakeStore) GetRecoveryAllocation(context.Context, teamquota.Owner) (*teamquota.RecoveryAllocation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recovery == nil {
		return nil, nil
	}
	cloned := *f.recovery
	cloned.Committed = f.recovery.Committed.Clone()
	cloned.Pending = f.recovery.Pending.Clone()
	if f.recovery.Operation != nil {
		operation := *f.recovery.Operation
		cloned.Operation = &operation
	}
	if f.recovery.ReconcileAfter != nil {
		reconcileAfter := *f.recovery.ReconcileAfter
		cloned.ReconcileAfter = &reconcileAfter
	}
	return &cloned, nil
}

func (f *fakeStore) ListRecoveryAllocations(
	_ context.Context,
	filter teamquota.RecoveryAllocationFilter,
) ([]teamquota.RecoveryAllocation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recoveryFilters = append(f.recoveryFilters, filter)
	if f.recovery == nil ||
		f.recovery.Owner.ClusterID != filter.ClusterID ||
		(filter.OwnerKind != "" && f.recovery.Owner.Kind != filter.OwnerKind) ||
		(filter.OnlyDue && !f.recovery.ReconcileDue) {
		return nil, nil
	}
	return []teamquota.RecoveryAllocation{*f.recovery}, nil
}

func volumePolicies() map[teamquota.Key]*teamquota.Policy {
	return map[teamquota.Key]*teamquota.Policy{
		teamquota.KeyVolumeStorageBytes: {
			Key:   teamquota.KeyVolumeStorageBytes,
			Kind:  teamquota.KindCapacity,
			Limit: 1 << 30,
		},
		teamquota.KeyStorageObjectCount: {
			Key:   teamquota.KeyStorageObjectCount,
			Kind:  teamquota.KindCapacity,
			Limit: 1 << 20,
		},
	}
}

func snapshotPolicies() map[teamquota.Key]*teamquota.Policy {
	return map[teamquota.Key]*teamquota.Policy{
		teamquota.KeySnapshotStorageBytes: {
			Key:   teamquota.KeySnapshotStorageBytes,
			Kind:  teamquota.KindCapacity,
			Limit: 1 << 30,
		},
		teamquota.KeyStorageObjectCount: {
			Key:   teamquota.KeyStorageObjectCount,
			Kind:  teamquota.KindCapacity,
			Limit: 1 << 20,
		},
	}
}

func fixedMeasure(target teamquota.Values) Measure {
	return func() (teamquota.Values, error) {
		return target.Clone(), nil
	}
}

func fixedBound(target teamquota.Values) Bound {
	return func(teamquota.Values) (teamquota.Values, error) {
		return target.Clone(), nil
	}
}

func TestMutateReservesAndAtomicallyFinalizesCompleteVolumeBundle(t *testing.T) {
	store := &fakeStore{policies: volumePolicies(), committed: VolumeTarget(100, 5)}
	service := New(store, RegionRecoveryScope("region-1"))
	before := VolumeTarget(100, 5)
	maximum := VolumeTarget(140, 9)
	exact := VolumeTarget(125, 8)
	physicalRan := false

	err := service.Mutate(
		context.Background(),
		service.VolumeOwner("team-1", "volume-1"),
		"volume_write",
		fixedMeasure(before),
		fixedBound(maximum),
		func() error {
			store.mu.Lock()
			defer store.mu.Unlock()
			if len(store.reserveTargets) != 1 {
				t.Fatal("physical mutation ran before reservation")
			}
			physicalRan = true
			return nil
		},
		fixedMeasure(exact),
	)
	if err != nil {
		t.Fatalf("Mutate() error = %v", err)
	}
	if !physicalRan || len(store.exactCommits) != 1 || store.commitCalled {
		t.Fatalf(
			"physicalRan=%v exactCommits=%v conservativeCommit=%v",
			physicalRan,
			store.exactCommits,
			store.commitCalled,
		)
	}
	if got := store.reserveTargets[0]; !targetsEqual(got, maximum) {
		t.Fatalf("reserved target = %v, want %v", got, maximum)
	}
	if got := store.exactCommits[0]; !targetsEqual(got, exact) {
		t.Fatalf("exact committed target = %v, want %v", got, exact)
	}
	if len(store.reconciled) != 0 {
		t.Fatalf("successful mutation used bare reconciliation: %v", store.reconciled)
	}
}

func TestMutateAdoptsMeasuredTargetAboveReservedMaximum(t *testing.T) {
	before := VolumeTarget(10, 4)
	maximum := VolumeTarget(20, 6)
	exact := VolumeTarget(25, 7)
	store := &fakeStore{policies: volumePolicies(), committed: before.Clone()}
	service := New(store, RegionRecoveryScope("region-1"))

	err := service.Mutate(
		context.Background(),
		service.VolumeOwner("team-1", "volume-1"),
		"volume_write",
		fixedMeasure(before),
		fixedBound(maximum),
		func() error { return nil },
		fixedMeasure(exact),
	)

	if !teamquota.IsUnavailable(err) {
		t.Fatalf("Mutate() error = %v, want unavailable bound error", err)
	}
	if len(store.exactCommits) != 0 {
		t.Fatalf("bounded exact commits = %v, want none", store.exactCommits)
	}
	if len(store.observedExact) != 1 || !targetsEqual(store.observedExact[0], exact) {
		t.Fatalf("observed exact commits = %v, want %v", store.observedExact, exact)
	}
	if !targetsEqual(store.committed, exact) || store.pending != nil {
		t.Fatalf("ledger target = committed %v pending %v, want %v and nil", store.committed, store.pending, exact)
	}
}

func TestMutateCreatesFenceForZeroGrowth(t *testing.T) {
	target := VolumeTarget(100, 5)
	store := &fakeStore{policies: volumePolicies(), committed: target.Clone()}
	service := New(store, RegionRecoveryScope("region-1"))
	physicalRan := false

	err := service.Mutate(
		context.Background(),
		service.VolumeOwner("team-1", "volume-1"),
		"volume_no_growth",
		fixedMeasure(target),
		fixedBound(target),
		func() error {
			store.mu.Lock()
			defer store.mu.Unlock()
			if len(store.reserveTargets) != 1 {
				t.Fatal("physical mutation ran without a zero-growth reservation fence")
			}
			physicalRan = true
			return nil
		},
		fixedMeasure(target),
	)
	if err != nil {
		t.Fatalf("Mutate() error = %v", err)
	}
	if !physicalRan || len(store.exactCommits) != 1 || store.commitCalled {
		t.Fatalf(
			"physicalRan=%v exactCommits=%v conservativeCommit=%v",
			physicalRan,
			store.exactCommits,
			store.commitCalled,
		)
	}
	if got := store.reserveTargets[0]; !targetsEqual(got, target) {
		t.Fatalf("zero-growth reserved target = %v, want %v", got, target)
	}
}

func TestMutateRemeasuresAfterFenceBeforePhysicalMutation(t *testing.T) {
	var physicalMu sync.Mutex
	physical := VolumeTarget(100, 5)
	store := &fakeStore{
		policies:  volumePolicies(),
		committed: physical.Clone(),
	}
	store.beforeReserve = func(store *fakeStore, _ teamquota.DeltaRequest) {
		store.beforeReserve = nil
		store.committed = VolumeTarget(0, 5)
		physicalMu.Lock()
		physical = VolumeTarget(0, 5)
		physicalMu.Unlock()
	}
	service := New(store, RegionRecoveryScope("region-1"))
	measure := func() (teamquota.Values, error) {
		physicalMu.Lock()
		defer physicalMu.Unlock()
		return physical.Clone(), nil
	}
	physicalRan := false

	err := service.Mutate(
		context.Background(),
		service.VolumeOwner("team-1", "volume-1"),
		"volume_truncate",
		measure,
		fixedBound(VolumeTarget(150, 5)),
		func() error {
			store.mu.Lock()
			reservations := len(store.reserveTargets)
			store.mu.Unlock()
			if reservations != 2 {
				t.Fatalf("physical mutation ran after %d reservations, want stale reservation abort plus retry", reservations)
			}
			physicalMu.Lock()
			physical = VolumeTarget(150, 5)
			physicalMu.Unlock()
			physicalRan = true
			return nil
		},
		measure,
	)
	if err != nil {
		t.Fatalf("Mutate() error = %v", err)
	}
	if !physicalRan || !store.aborted {
		t.Fatalf("physicalRan=%v staleReservationAborted=%v", physicalRan, store.aborted)
	}
	if got := store.reserveTargets[0][teamquota.KeyVolumeStorageBytes]; got != 50 {
		t.Fatalf("stale reserved bytes = %d, want 50", got)
	}
	if got := store.reserveTargets[1][teamquota.KeyVolumeStorageBytes]; got != 150 {
		t.Fatalf("retried reserved bytes = %d, want 150", got)
	}
}

func TestMutateFailsClosedWhenObjectPolicyIsMissing(t *testing.T) {
	policies := volumePolicies()
	delete(policies, teamquota.KeyStorageObjectCount)
	store := &fakeStore{policies: policies}
	service := New(store, "")
	physicalRan := false

	err := service.Mutate(
		context.Background(),
		service.VolumeOwner("team-1", "volume-1"),
		"volume_create",
		fixedMeasure(VolumeTarget(0, 0)),
		fixedBound(VolumeTarget(0, 3)),
		func() error {
			physicalRan = true
			return nil
		},
		fixedMeasure(VolumeTarget(0, 3)),
	)
	if !teamquota.IsUnavailable(err) || physicalRan {
		t.Fatalf("Mutate() error = %v, physicalRan=%v", err, physicalRan)
	}
}

func TestMutateRejectsIncompleteBundleBeforePhysicalMutation(t *testing.T) {
	store := &fakeStore{policies: volumePolicies()}
	service := New(store, "")
	physicalRan := false

	err := service.Mutate(
		context.Background(),
		service.VolumeOwner("team-1", "volume-1"),
		"volume_write",
		fixedMeasure(teamquota.Values{teamquota.KeyVolumeStorageBytes: 1}),
		fixedBound(VolumeTarget(2, 2)),
		func() error {
			physicalRan = true
			return nil
		},
		fixedMeasure(VolumeTarget(2, 2)),
	)
	if !teamquota.IsUnavailable(err) || physicalRan {
		t.Fatalf("Mutate() error = %v, physicalRan=%v", err, physicalRan)
	}
}

func TestMutateReturnsOriginalErrorWhenFailedMutationIsUnchanged(t *testing.T) {
	physicalErr := errors.New("entry already exists")
	target := VolumeTarget(10, 4)
	store := &fakeStore{policies: volumePolicies(), committed: target.Clone()}
	service := New(store, "")

	err := service.Mutate(
		context.Background(),
		service.VolumeOwner("team-1", "volume-1"),
		"volume_create",
		fixedMeasure(target),
		fixedBound(VolumeTarget(10, 8)),
		func() error { return physicalErr },
		fixedMeasure(target),
	)
	if err != physicalErr || !store.aborted {
		t.Fatalf("Mutate() error = %v, aborted=%v", err, store.aborted)
	}
}

func TestMutateAtomicallyFinalizesMeasurablePartialFailure(t *testing.T) {
	physicalErr := errors.New("short write")
	before := VolumeTarget(10, 4)
	maximum := VolumeTarget(30, 8)
	exact := VolumeTarget(18, 6)
	store := &fakeStore{policies: volumePolicies(), committed: before.Clone()}
	service := New(store, "")

	err := service.Mutate(
		context.Background(),
		service.VolumeOwner("team-1", "volume-1"),
		"volume_write",
		fixedMeasure(before),
		fixedBound(maximum),
		func() error { return physicalErr },
		fixedMeasure(exact),
	)

	if !errors.Is(err, physicalErr) {
		t.Fatalf("Mutate() error = %v, want physical error", err)
	}
	if len(store.exactCommits) != 1 ||
		!targetsEqual(store.exactCommits[0], exact) {
		t.Fatalf("exact commits = %v, want %v", store.exactCommits, exact)
	}
	if store.commitCalled || len(store.reconciled) != 0 {
		t.Fatalf(
			"partial failure used split finalize: conservativeCommit=%v reconciled=%v",
			store.commitCalled,
			store.reconciled,
		)
	}
}

func TestMutateSerializesMeasurementWithPhysicalMutation(t *testing.T) {
	var stateMu sync.Mutex
	state := VolumeTarget(0, 3)
	store := &fakeStore{policies: volumePolicies(), committed: state.Clone()}
	service := New(store, "")
	run := func() error {
		measure := func() (teamquota.Values, error) {
			stateMu.Lock()
			defer stateMu.Unlock()
			return state.Clone(), nil
		}
		return service.Mutate(
			context.Background(),
			service.VolumeOwner("team-1", "volume-1"),
			"volume_create_file",
			measure,
			func(before teamquota.Values) (teamquota.Values, error) {
				return VolumeTarget(
					before[teamquota.KeyVolumeStorageBytes],
					before[teamquota.KeyStorageObjectCount]+3,
				), nil
			},
			func() error {
				stateMu.Lock()
				defer stateMu.Unlock()
				state[teamquota.KeyStorageObjectCount] += 3
				return nil
			},
			measure,
		)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- run()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Mutate() error = %v", err)
		}
	}
	if got := state[teamquota.KeyStorageObjectCount]; got != 9 {
		t.Fatalf("final object count = %d, want 9", got)
	}
	if got := store.reserveTargets[1][teamquota.KeyStorageObjectCount]; got != 9 {
		t.Fatalf("second reserved object count = %d, want 9", got)
	}
}

func TestAdoptExistingPreservesMatchingPendingDeleteBundle(t *testing.T) {
	store := &fakeStore{policies: snapshotPolicies()}
	service := New(store, RegionRecoveryScope("region-1"))
	owner := service.SnapshotOwner("team-1", "snapshot-1")
	target := SnapshotTarget(512, 20)
	store.recovery = &teamquota.RecoveryAllocation{
		Owner:        owner,
		Operation:    &teamquota.Operation{ID: deterministicDeleteOperationID(owner), Kind: "snapshot_delete"},
		Committed:    target.Clone(),
		Pending:      SnapshotTarget(0, 0),
		ReconcileDue: true,
	}

	if err := service.AdoptExisting(context.Background(), owner, fixedMeasure(target)); err != nil {
		t.Fatalf("AdoptExisting() error = %v", err)
	}
	pending, err := service.PendingRelease(context.Background(), owner)
	if err != nil || !pending {
		t.Fatalf("PendingRelease() = %v, %v, want true, nil", pending, err)
	}
	if store.aborted || store.confirmed || len(store.reconciled) != 0 {
		t.Fatalf("pending release was modified: %+v", store)
	}
}

func TestAdoptExistingLeavesFreshPreparedMutationUntouched(t *testing.T) {
	store := &fakeStore{policies: volumePolicies()}
	service := New(store, RegionRecoveryScope("region-1"))
	owner := service.VolumeOwner("team-1", "volume-1")
	store.recovery = &teamquota.RecoveryAllocation{
		Owner:        owner,
		Operation:    &teamquota.Operation{ID: "active-write", Kind: "volume_write"},
		Committed:    VolumeTarget(100, 5),
		Pending:      VolumeTarget(150, 5),
		ReconcileDue: false,
	}

	if err := service.AdoptExisting(
		context.Background(),
		owner,
		fixedMeasure(VolumeTarget(125, 5)),
	); err != nil {
		t.Fatalf("AdoptExisting() error = %v", err)
	}
	if store.aborted || store.commitCalled || len(store.reconciled) != 0 {
		t.Fatalf("fresh prepared mutation was modified: %+v", store)
	}
}

func TestAdoptExistingRecoversExpiredPreparedMutation(t *testing.T) {
	store := &fakeStore{policies: volumePolicies()}
	service := New(store, RegionRecoveryScope("region-1"))
	owner := service.VolumeOwner("team-1", "volume-1")
	target := VolumeTarget(100, 5)
	store.recovery = &teamquota.RecoveryAllocation{
		Owner:        owner,
		Operation:    &teamquota.Operation{ID: "expired-write", Kind: "volume_write"},
		Committed:    target.Clone(),
		Pending:      VolumeTarget(150, 5),
		ReconcileDue: true,
	}

	if err := service.AdoptExisting(context.Background(), owner, fixedMeasure(target)); err != nil {
		t.Fatalf("AdoptExisting() error = %v", err)
	}
	if !store.aborted || store.commitCalled {
		t.Fatalf("expired unchanged mutation: aborted=%v committed=%v", store.aborted, store.commitCalled)
	}
	if len(store.reconciled) != 0 || len(store.exactCommits) != 0 {
		t.Fatalf(
			"expired unchanged mutation performed a second write: reconciled=%v exact=%v",
			store.reconciled,
			store.exactCommits,
		)
	}
}

func TestAdoptExistingAtomicallyFinalizesChangedExpiredMutation(t *testing.T) {
	store := &fakeStore{policies: volumePolicies()}
	service := New(store, RegionRecoveryScope("region-1"))
	owner := service.VolumeOwner("team-1", "volume-1")
	exact := VolumeTarget(125, 5)
	store.recovery = &teamquota.RecoveryAllocation{
		Owner:        owner,
		Operation:    &teamquota.Operation{ID: "expired-write", Kind: "volume_write"},
		Committed:    VolumeTarget(100, 5),
		Pending:      VolumeTarget(150, 5),
		ReconcileDue: true,
	}

	if err := service.AdoptExisting(context.Background(), owner, fixedMeasure(exact)); err != nil {
		t.Fatalf("AdoptExisting() error = %v", err)
	}
	if len(store.exactCommits) != 1 ||
		!targetsEqual(store.exactCommits[0], exact) {
		t.Fatalf("exact commits = %v, want %v", store.exactCommits, exact)
	}
	if store.commitCalled || store.aborted || len(store.reconciled) != 0 {
		t.Fatalf(
			"expired mutation used split recovery: commit=%v abort=%v reconciled=%v",
			store.commitCalled,
			store.aborted,
			store.reconciled,
		)
	}
}

func TestAdoptExistingAdoptsExpiredTargetAbovePending(t *testing.T) {
	store := &fakeStore{policies: volumePolicies()}
	service := New(store, RegionRecoveryScope("region-1"))
	owner := service.VolumeOwner("team-1", "volume-1")
	exact := VolumeTarget(175, 6)
	store.recovery = &teamquota.RecoveryAllocation{
		Owner:        owner,
		State:        "reserved",
		Operation:    &teamquota.Operation{ID: "expired-write", Kind: "volume_write"},
		Committed:    VolumeTarget(100, 5),
		Pending:      VolumeTarget(150, 5),
		ReconcileDue: true,
	}

	err := service.AdoptExisting(context.Background(), owner, fixedMeasure(exact))

	if !teamquota.IsUnavailable(err) {
		t.Fatalf("AdoptExisting() error = %v, want unavailable bound error", err)
	}
	if len(store.exactCommits) != 0 {
		t.Fatalf("bounded exact commits = %v, want none", store.exactCommits)
	}
	if len(store.observedExact) != 1 || !targetsEqual(store.observedExact[0], exact) {
		t.Fatalf("observed exact commits = %v, want %v", store.observedExact, exact)
	}
	if store.recovery.Operation != nil ||
		!targetsEqual(store.recovery.Committed, exact) ||
		store.recovery.Pending != nil {
		t.Fatalf("recovered allocation = %+v, want committed observed target", store.recovery)
	}
}

func TestListDueRecoveryAllocationsUsesRegionalScope(t *testing.T) {
	store := &fakeStore{policies: volumePolicies()}
	service := New(store, RegionRecoveryScope("region-1"))
	store.recovery = &teamquota.RecoveryAllocation{
		Owner: teamquota.Owner{
			TeamID:    "team-1",
			Kind:      OwnerKindVolume,
			ID:        "volume-1",
			ClusterID: RegionRecoveryScope("region-1"),
		},
		Operation:    &teamquota.Operation{ID: "create-op", Kind: "volume_create"},
		ReconcileDue: true,
	}

	allocations, err := service.ListDueRecoveryAllocations(
		context.Background(),
		OwnerKindVolume,
		"allocation-before",
		17,
	)
	if err != nil {
		t.Fatalf("ListDueRecoveryAllocations() error = %v", err)
	}
	if len(allocations) != 1 {
		t.Fatalf("allocations = %+v, want one", allocations)
	}
	if len(store.recoveryFilters) != 1 {
		t.Fatalf("recovery filters = %+v, want one", store.recoveryFilters)
	}
	filter := store.recoveryFilters[0]
	if filter.ClusterID != RegionRecoveryScope("region-1") ||
		filter.OwnerKind != OwnerKindVolume ||
		filter.AfterAllocationID != "allocation-before" ||
		!filter.OnlyDue ||
		filter.Limit != 17 {
		t.Fatalf("recovery filter = %+v", filter)
	}
}

func TestAbortAbsentCatalogVolumeCreateClearsExpiredZeroBaseReservation(t *testing.T) {
	store := &fakeStore{policies: volumePolicies()}
	service := New(store, RegionRecoveryScope("region-1"))
	owner := service.VolumeOwner("team-1", "volume-1")
	store.recovery = &teamquota.RecoveryAllocation{
		Owner:              owner,
		State:              "reserved",
		Operation:          &teamquota.Operation{ID: "create-op", Kind: "volume_create"},
		OperationBaseState: "released",
		Committed:          VolumeTarget(0, 0),
		Pending:            VolumeTarget(0, 3),
		ReconcileDue:       true,
	}

	aborted, err := service.AbortAbsentCatalogVolumeCreate(context.Background(), owner)
	if err != nil {
		t.Fatalf("AbortAbsentCatalogVolumeCreate() error = %v", err)
	}
	if !aborted || !store.aborted {
		t.Fatalf("aborted = %v store.aborted = %v, want true", aborted, store.aborted)
	}
	if store.recovery.Operation != nil || store.recovery.Pending != nil || store.recovery.State != "released" {
		t.Fatalf("recovery allocation = %+v, want released terminal state", store.recovery)
	}
}

func TestAbortAbsentCatalogVolumeCreateRetainsUnsafeMutation(t *testing.T) {
	store := &fakeStore{policies: volumePolicies()}
	service := New(store, RegionRecoveryScope("region-1"))
	owner := service.VolumeOwner("team-1", "volume-1")
	store.recovery = &teamquota.RecoveryAllocation{
		Owner:        owner,
		State:        "reserved",
		Operation:    &teamquota.Operation{ID: "write-op", Kind: "volume_write"},
		Committed:    VolumeTarget(100, 5),
		Pending:      VolumeTarget(150, 6),
		ReconcileDue: true,
	}

	aborted, err := service.AbortAbsentCatalogVolumeCreate(context.Background(), owner)
	if err != nil {
		t.Fatalf("AbortAbsentCatalogVolumeCreate() error = %v", err)
	}
	if aborted || store.aborted || store.recovery.Operation == nil {
		t.Fatalf("unsafe mutation was changed: aborted=%v allocation=%+v", aborted, store.recovery)
	}
}

func TestAdoptExistingSkipsStaleAbsentAllocationObservation(t *testing.T) {
	store := &fakeStore{policies: volumePolicies()}
	service := New(store, RegionRecoveryScope("region-1"))
	owner := service.VolumeOwner("team-1", "volume-1")
	concurrentTarget := VolumeTarget(150, 5)

	err := service.AdoptExisting(
		context.Background(),
		owner,
		func() (teamquota.Values, error) {
			store.mu.Lock()
			store.recovery = &teamquota.RecoveryAllocation{
				Owner:     owner,
				Revision:  1,
				Committed: concurrentTarget.Clone(),
			}
			store.committed = concurrentTarget.Clone()
			store.mu.Unlock()
			return VolumeTarget(100, 5), nil
		},
	)
	if err != nil {
		t.Fatalf("AdoptExisting() error = %v", err)
	}
	if len(store.reconciled) != 0 {
		t.Fatalf("stale absent observation reconciled targets = %v, want none", store.reconciled)
	}
	if got := store.committed; !targetsEqual(got, concurrentTarget) {
		t.Fatalf("committed target = %v, want concurrent target %v", got, concurrentTarget)
	}
	if len(store.reconcileCAS) != 1 || store.reconcileCAS[0] != 0 {
		t.Fatalf("conditional revisions = %v, want [0]", store.reconcileCAS)
	}
}

func TestReconcileSkipsObservationStaleAfterConcurrentMutation(t *testing.T) {
	store := &fakeStore{policies: volumePolicies()}
	service := New(store, RegionRecoveryScope("region-1"))
	owner := service.VolumeOwner("team-1", "volume-1")
	store.recovery = &teamquota.RecoveryAllocation{
		Owner:     owner,
		Revision:  7,
		Committed: VolumeTarget(100, 5),
	}
	concurrentTarget := VolumeTarget(150, 5)

	err := service.Reconcile(
		context.Background(),
		owner,
		func() (teamquota.Values, error) {
			store.mu.Lock()
			store.recovery.Revision = 8
			store.recovery.Committed = concurrentTarget.Clone()
			store.committed = concurrentTarget.Clone()
			store.mu.Unlock()
			return VolumeTarget(125, 5), nil
		},
	)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(store.reconciled) != 0 {
		t.Fatalf("stale observation reconciled targets = %v, want none", store.reconciled)
	}
	if got := store.committed; !targetsEqual(got, concurrentTarget) {
		t.Fatalf("committed target = %v, want concurrent target %v", got, concurrentTarget)
	}
	if len(store.reconcileCAS) != 1 || store.reconcileCAS[0] != 7 {
		t.Fatalf("conditional revisions = %v, want [7]", store.reconcileCAS)
	}
}

func TestAdoptExistingMeasurementFailureLeavesCommittedUsage(t *testing.T) {
	store := &fakeStore{
		policies:  volumePolicies(),
		committed: VolumeTarget(100, 5),
	}
	service := New(store, RegionRecoveryScope("region-1"))
	owner := service.VolumeOwner("team-1", "volume-1")
	store.recovery = &teamquota.RecoveryAllocation{
		Owner:     owner,
		Revision:  3,
		Committed: store.committed.Clone(),
	}

	err := service.AdoptExisting(
		context.Background(),
		owner,
		func() (teamquota.Values, error) {
			return nil, errors.New("physical state is unavailable")
		},
	)
	if !teamquota.IsUnavailable(err) {
		t.Fatalf("AdoptExisting() error = %v, want UnavailableError", err)
	}
	if len(store.reconciled) != 0 || len(store.reconcileCAS) != 0 {
		t.Fatalf(
			"failed measurement changed reconciliation state: reconciled=%v conditional=%v",
			store.reconciled,
			store.reconcileCAS,
		)
	}
	if got, want := store.committed, VolumeTarget(100, 5); !targetsEqual(got, want) {
		t.Fatalf("committed target = %v, want %v", got, want)
	}
}

func TestBeginReleaseUsesCompleteZeroTarget(t *testing.T) {
	store := &fakeStore{policies: snapshotPolicies()}
	service := New(store, "")
	release, err := service.BeginRelease(
		context.Background(),
		service.SnapshotOwner("team-1", "snapshot-1"),
		"snapshot_delete",
	)
	if err != nil {
		t.Fatalf("BeginRelease() error = %v", err)
	}
	if got, want := store.beginRequests[0].Target, SnapshotTarget(0, 0); !targetsEqual(got, want) {
		t.Fatalf("release target = %v, want %v", got, want)
	}
	if err := release.Confirm(context.Background()); err != nil {
		t.Fatalf("Confirm() error = %v", err)
	}
	if !store.confirmed {
		t.Fatal("release was not confirmed")
	}
}
