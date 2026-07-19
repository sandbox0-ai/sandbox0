package snapshot

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota"
)

type catalogQuotaStore struct {
	reconciled map[string]int64
	recovery   *teamquota.RecoveryAllocation
	aborts     int
}

func (s *catalogQuotaStore) EffectivePolicy(_ context.Context, teamID string, key teamquota.Key) (*teamquota.Policy, error) {
	return &teamquota.Policy{
		TeamID: teamID,
		Key:    key,
		Kind:   teamquota.KindCapacity,
		Limit:  1 << 30,
	}, nil
}

func (s *catalogQuotaStore) ReserveDelta(_ context.Context, request teamquota.DeltaRequest) (*teamquota.Reservation, error) {
	committed := request.Observed.Clone()
	target := committed.Clone()
	for key, delta := range request.Delta {
		target[key] += delta
	}
	return &teamquota.Reservation{
		Owner:     request.Owner,
		Operation: request.Operation,
		Committed: committed,
		Target:    target,
		Reserved:  request.Delta.Clone(),
	}, nil
}

func (s *catalogQuotaStore) Commit(context.Context, teamquota.OperationRef) error {
	return nil
}

func (s *catalogQuotaStore) CommitExact(
	context.Context,
	teamquota.OperationRef,
	teamquota.Values,
) error {
	return nil
}

func (s *catalogQuotaStore) Abort(context.Context, teamquota.OperationRef, string) error {
	s.aborts++
	if s.recovery != nil {
		s.recovery.Operation = nil
		s.recovery.Pending = nil
		s.recovery.State = s.recovery.OperationBaseState
	}
	return nil
}

func (s *catalogQuotaStore) BeginRelease(_ context.Context, request teamquota.ReleaseRequest) (*teamquota.Reservation, error) {
	return &teamquota.Reservation{Owner: request.Owner, Operation: request.Operation, Target: request.Target}, nil
}

func (s *catalogQuotaStore) ConfirmRelease(context.Context, teamquota.OperationRef, teamquota.RuntimeRef) error {
	return nil
}

func (s *catalogQuotaStore) ConfirmReleaseTx(context.Context, pgx.Tx, teamquota.OperationRef, teamquota.RuntimeRef) error {
	return nil
}

func (s *catalogQuotaStore) ReconcileTargetIfRevision(
	_ context.Context,
	owner teamquota.Owner,
	target teamquota.Values,
	_ teamquota.RuntimeRef,
	expectedRevision int64,
) (bool, error) {
	if expectedRevision != 0 {
		return false, nil
	}
	if s.reconciled == nil {
		s.reconciled = make(map[string]int64)
	}
	for key, value := range target {
		s.reconciled[fmt.Sprintf("%s/%s/%s", owner.Kind, owner.ID, key)] = value
	}
	return true, nil
}

func (s *catalogQuotaStore) GetRecoveryAllocation(
	_ context.Context,
	owner teamquota.Owner,
) (*teamquota.RecoveryAllocation, error) {
	if s.recovery == nil ||
		s.recovery.Owner.TeamID != owner.TeamID ||
		s.recovery.Owner.Kind != owner.Kind ||
		s.recovery.Owner.ID != owner.ID {
		return nil, nil
	}
	allocation := *s.recovery
	allocation.Committed = s.recovery.Committed.Clone()
	allocation.Pending = s.recovery.Pending.Clone()
	if s.recovery.Operation != nil {
		operation := *s.recovery.Operation
		allocation.Operation = &operation
	}
	return &allocation, nil
}

func (s *catalogQuotaStore) ListRecoveryAllocations(
	_ context.Context,
	filter teamquota.RecoveryAllocationFilter,
) ([]teamquota.RecoveryAllocation, error) {
	if s.recovery == nil ||
		s.recovery.Owner.ClusterID != filter.ClusterID ||
		(filter.OwnerKind != "" && s.recovery.Owner.Kind != filter.OwnerKind) ||
		(filter.OnlyDue && !s.recovery.ReconcileDue) {
		return nil, nil
	}
	return []teamquota.RecoveryAllocation{*s.recovery}, nil
}

func TestReconcileCatalogStorageAdoptsExactVolumeAndSnapshotBytes(t *testing.T) {
	mgr, _, _, engine := newS0FSSnapshotTestManager(t, "volume-1")
	writeS0FSFile(t, engine, "payload.txt", "catalog payload")

	snapshot, err := mgr.createS0FSSnapshot(context.Background(), &CreateSnapshotRequest{
		VolumeID: "volume-1",
		TeamID:   "team-1",
		UserID:   "user-1",
		Name:     "startup adoption",
	})
	if err != nil {
		t.Fatalf("createS0FSSnapshot() error = %v", err)
	}
	volumeBytes, err := engine.StorageBytes()
	if err != nil {
		t.Fatalf("StorageBytes() error = %v", err)
	}

	store := &catalogQuotaStore{}
	mgr.SetStorageQuota(storagequota.New(store, "test-cluster"))
	if err := mgr.ReconcileCatalogStorage(context.Background()); err != nil {
		t.Fatalf("ReconcileCatalogStorage() error = %v", err)
	}

	if got := store.reconciled["volume/volume-1/volume_storage_bytes"]; got != volumeBytes {
		t.Fatalf("volume adopted bytes = %d, want %d", got, volumeBytes)
	}
	snapshotKey := fmt.Sprintf("snapshot/%s/snapshot_storage_bytes", snapshot.ID)
	if got := store.reconciled[snapshotKey]; got != snapshot.SizeBytes {
		t.Fatalf("snapshot adopted bytes = %d, want %d", got, snapshot.SizeBytes)
	}
}

func TestReconcileCatalogStorageAbortsExpiredAbsentVolumeCreate(t *testing.T) {
	repo := newFakeRepo()
	recoveryScope := storagequota.RegionRecoveryScope("region-1")
	owner := teamquota.Owner{
		TeamID:    "team-1",
		Kind:      storagequota.OwnerKindVolume,
		ID:        "volume-missing",
		ClusterID: recoveryScope,
	}
	store := &catalogQuotaStore{recovery: &teamquota.RecoveryAllocation{
		AllocationID:       "allocation-1",
		Owner:              owner,
		State:              "reserved",
		Operation:          &teamquota.Operation{ID: "create-op", Kind: "volume_create"},
		OperationBaseState: "released",
		Committed:          storagequota.VolumeTarget(0, 0),
		Pending:            storagequota.VolumeTarget(0, 3),
		ReconcileDue:       true,
	}}
	mgr := &Manager{
		repo:         repo,
		storageQuota: storagequota.New(store, recoveryScope),
	}

	if err := mgr.ReconcileCatalogStorage(context.Background()); err != nil {
		t.Fatalf("ReconcileCatalogStorage() error = %v", err)
	}
	if store.aborts != 1 {
		t.Fatalf("quota aborts = %d, want 1", store.aborts)
	}
	if store.recovery.Operation != nil || store.recovery.State != "released" {
		t.Fatalf("recovery allocation = %+v, want released", store.recovery)
	}
}

type pagedCatalogQuotaStore struct {
	*catalogQuotaStore

	mu          sync.Mutex
	allocations map[string]*teamquota.RecoveryAllocation
	filters     []teamquota.RecoveryAllocationFilter
	aborts      int
	commits     int
	ignoreAfter bool
}

func newPagedCatalogQuotaStore(
	allocations ...teamquota.RecoveryAllocation,
) *pagedCatalogQuotaStore {
	store := &pagedCatalogQuotaStore{
		catalogQuotaStore: &catalogQuotaStore{},
		allocations:       make(map[string]*teamquota.RecoveryAllocation, len(allocations)),
	}
	for i := range allocations {
		allocation := cloneCatalogRecoveryAllocation(&allocations[i])
		store.allocations[allocation.AllocationID] = allocation
	}
	return store
}

func (s *pagedCatalogQuotaStore) GetRecoveryAllocation(
	_ context.Context,
	owner teamquota.Owner,
) (*teamquota.RecoveryAllocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, allocation := range s.allocations {
		if allocation.Owner.TeamID == owner.TeamID &&
			allocation.Owner.Kind == owner.Kind &&
			allocation.Owner.ID == owner.ID {
			return cloneCatalogRecoveryAllocation(allocation), nil
		}
	}
	return nil, nil
}

func (s *pagedCatalogQuotaStore) ListRecoveryAllocations(
	_ context.Context,
	filter teamquota.RecoveryAllocationFilter,
) ([]teamquota.RecoveryAllocation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.filters = append(s.filters, filter)

	ids := make([]string, 0, len(s.allocations))
	for id, allocation := range s.allocations {
		if allocation.Owner.ClusterID != filter.ClusterID ||
			(filter.OwnerKind != "" && allocation.Owner.Kind != filter.OwnerKind) ||
			(filter.OnlyDue && (!allocation.ReconcileDue || allocation.Operation == nil)) ||
			(!s.ignoreAfter && id <= filter.AfterAllocationID) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if filter.Limit > 0 && len(ids) > filter.Limit {
		ids = ids[:filter.Limit]
	}
	result := make([]teamquota.RecoveryAllocation, 0, len(ids))
	for _, id := range ids {
		result = append(result, *cloneCatalogRecoveryAllocation(s.allocations[id]))
	}
	return result, nil
}

func (s *pagedCatalogQuotaStore) Abort(
	_ context.Context,
	operation teamquota.OperationRef,
	_ string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	allocation := s.allocationForOwnerLocked(operation.Owner)
	if allocation == nil || allocation.Operation == nil {
		return nil
	}
	if allocation.Operation.ID != operation.ID ||
		allocation.Operation.Generation != operation.Generation {
		return fmt.Errorf("operation changed")
	}
	s.aborts++
	allocation.Operation = nil
	allocation.Pending = nil
	allocation.State = allocation.OperationBaseState
	allocation.ReconcileDue = false
	allocation.ReconcileAfter = nil
	return nil
}

func (s *pagedCatalogQuotaStore) CommitExact(
	_ context.Context,
	operation teamquota.OperationRef,
	exact teamquota.Values,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	allocation := s.allocationForOwnerLocked(operation.Owner)
	if allocation == nil {
		return fmt.Errorf("allocation not found")
	}
	if allocation.Operation == nil {
		if catalogTargetsEqual(allocation.Committed, exact) {
			return nil
		}
		return fmt.Errorf("operation changed")
	}
	if allocation.Operation.ID != operation.ID ||
		allocation.Operation.Generation != operation.Generation {
		return fmt.Errorf("operation changed")
	}
	s.commits++
	allocation.Committed = exact.Clone()
	allocation.Pending = nil
	allocation.Operation = nil
	allocation.State = "allocated"
	allocation.ReconcileDue = false
	allocation.ReconcileAfter = nil
	return nil
}

func (s *pagedCatalogQuotaStore) allocationForOwnerLocked(
	owner teamquota.Owner,
) *teamquota.RecoveryAllocation {
	for _, allocation := range s.allocations {
		if allocation.Owner.TeamID == owner.TeamID &&
			allocation.Owner.Kind == owner.Kind &&
			allocation.Owner.ID == owner.ID {
			return allocation
		}
	}
	return nil
}

func (s *pagedCatalogQuotaStore) allocation(
	allocationID string,
) *teamquota.RecoveryAllocation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneCatalogRecoveryAllocation(s.allocations[allocationID])
}

func (s *pagedCatalogQuotaStore) counts() (filters, aborts, commits int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.filters), s.aborts, s.commits
}

func cloneCatalogRecoveryAllocation(
	allocation *teamquota.RecoveryAllocation,
) *teamquota.RecoveryAllocation {
	if allocation == nil {
		return nil
	}
	cloned := *allocation
	cloned.Committed = allocation.Committed.Clone()
	cloned.Pending = allocation.Pending.Clone()
	if allocation.Operation != nil {
		operation := *allocation.Operation
		cloned.Operation = &operation
	}
	if allocation.ReconcileAfter != nil {
		reconcileAfter := *allocation.ReconcileAfter
		cloned.ReconcileAfter = &reconcileAfter
	}
	return &cloned
}

func catalogTargetsEqual(left, right teamquota.Values) bool {
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	for key, value := range right {
		if left[key] != value {
			return false
		}
	}
	return true
}

type recordingCatalogRepository struct {
	*fakeRepo

	mu                sync.Mutex
	volumeLists       int
	snapshotLists     int
	volumeGets        []string
	snapshotGets      []string
	volumeGetEntered  chan<- struct{}
	volumeGetReleased <-chan struct{}
}

func (r *recordingCatalogRepository) ListSandboxVolumes(
	ctx context.Context,
) ([]*db.SandboxVolume, error) {
	r.mu.Lock()
	r.volumeLists++
	r.mu.Unlock()
	return r.fakeRepo.ListSandboxVolumes(ctx)
}

func (r *recordingCatalogRepository) ListSnapshots(
	ctx context.Context,
) ([]*db.Snapshot, error) {
	r.mu.Lock()
	r.snapshotLists++
	r.mu.Unlock()
	return r.fakeRepo.ListSnapshots(ctx)
}

func (r *recordingCatalogRepository) GetSandboxVolume(
	ctx context.Context,
	id string,
) (*db.SandboxVolume, error) {
	r.mu.Lock()
	r.volumeGets = append(r.volumeGets, id)
	entered := r.volumeGetEntered
	released := r.volumeGetReleased
	r.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if released != nil {
		select {
		case <-released:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return r.fakeRepo.GetSandboxVolume(ctx, id)
}

func (r *recordingCatalogRepository) GetSnapshot(
	ctx context.Context,
	id string,
) (*db.Snapshot, error) {
	r.mu.Lock()
	r.snapshotGets = append(r.snapshotGets, id)
	r.mu.Unlock()
	return r.fakeRepo.GetSnapshot(ctx, id)
}

func (r *recordingCatalogRepository) calls() (
	volumeLists int,
	snapshotLists int,
	volumeGets []string,
	snapshotGets []string,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.volumeLists,
		r.snapshotLists,
		append([]string(nil), r.volumeGets...),
		append([]string(nil), r.snapshotGets...)
}

func dueCatalogAllocation(
	allocationID string,
	owner teamquota.Owner,
	operationKind string,
	committed teamquota.Values,
	pending teamquota.Values,
) teamquota.RecoveryAllocation {
	return teamquota.RecoveryAllocation{
		AllocationID:       allocationID,
		Revision:           1,
		Owner:              owner,
		State:              "reserved",
		Operation:          &teamquota.Operation{ID: "operation-" + allocationID, Kind: operationKind, Generation: 1},
		OperationBaseState: "released",
		Committed:          committed.Clone(),
		Pending:            pending.Clone(),
		ReconcileDue:       true,
	}
}

func TestRecoverDueCatalogStorageUsesPointReadsAndRetainsAbsentSnapshot(t *testing.T) {
	scope := storagequota.RegionRecoveryScope("region-1")
	volumeOwner := teamquota.Owner{
		TeamID:    "team-1",
		Kind:      storagequota.OwnerKindVolume,
		ID:        "volume-due",
		ClusterID: scope,
	}
	snapshotOwner := teamquota.Owner{
		TeamID:    "team-1",
		Kind:      storagequota.OwnerKindSnapshot,
		ID:        "snapshot-absent",
		ClusterID: scope,
	}
	store := newPagedCatalogQuotaStore(
		dueCatalogAllocation(
			"allocation-volume",
			volumeOwner,
			"volume_create",
			storagequota.VolumeTarget(0, 0),
			storagequota.VolumeTarget(0, storagequota.CatalogObjectCount),
		),
		dueCatalogAllocation(
			"allocation-snapshot",
			snapshotOwner,
			"snapshot_create",
			storagequota.SnapshotTarget(0, 0),
			storagequota.SnapshotTarget(128, 3),
		),
	)
	repo := &recordingCatalogRepository{fakeRepo: newFakeRepo()}
	repo.volumes[volumeOwner.ID] = &db.SandboxVolume{
		ID:      volumeOwner.ID,
		TeamID:  volumeOwner.TeamID,
		Backend: "s3",
	}
	mgr := &Manager{
		repo:         repo,
		storageQuota: storagequota.New(store, scope),
	}

	if err := mgr.RecoverDueCatalogStorage(context.Background()); err != nil {
		t.Fatalf("RecoverDueCatalogStorage() error = %v", err)
	}
	volumeLists, snapshotLists, volumeGets, snapshotGets := repo.calls()
	if volumeLists != 0 || snapshotLists != 0 {
		t.Fatalf(
			"periodic catalog lists = volumes:%d snapshots:%d, want zero",
			volumeLists,
			snapshotLists,
		)
	}
	if fmt.Sprint(volumeGets) != "[volume-due]" {
		t.Fatalf("volume point reads = %v, want [volume-due]", volumeGets)
	}
	if fmt.Sprint(snapshotGets) != "[snapshot-absent]" {
		t.Fatalf("snapshot point reads = %v, want [snapshot-absent]", snapshotGets)
	}
	if allocation := store.allocation("allocation-volume"); allocation.Operation != nil ||
		!catalogTargetsEqual(
			allocation.Committed,
			storagequota.VolumeTarget(0, storagequota.CatalogObjectCount),
		) {
		t.Fatalf("recovered volume allocation = %+v", allocation)
	}
	if allocation := store.allocation("allocation-snapshot"); allocation.Operation == nil {
		t.Fatalf("absent snapshot allocation was released: %+v", allocation)
	}
	_, aborts, commits := store.counts()
	if aborts != 0 || commits != 1 {
		t.Fatalf("quota mutations = aborts:%d commits:%d, want 0/1", aborts, commits)
	}
}

func TestRecoverDueCatalogStoragePaginatesWithoutCatalogScan(t *testing.T) {
	scope := storagequota.RegionRecoveryScope("region-1")
	allocations := make([]teamquota.RecoveryAllocation, 0, 205)
	for i := 0; i < 205; i++ {
		allocationID := fmt.Sprintf("allocation-%03d", i)
		allocations = append(allocations, dueCatalogAllocation(
			allocationID,
			teamquota.Owner{
				TeamID:    "team-1",
				Kind:      storagequota.OwnerKindVolume,
				ID:        fmt.Sprintf("volume-%03d", i),
				ClusterID: scope,
			},
			"volume_create",
			storagequota.VolumeTarget(0, 0),
			storagequota.VolumeTarget(0, storagequota.CatalogObjectCount),
		))
	}
	store := newPagedCatalogQuotaStore(allocations...)
	repo := &recordingCatalogRepository{fakeRepo: newFakeRepo()}
	mgr := &Manager{
		repo:         repo,
		storageQuota: storagequota.New(store, scope),
	}

	if err := mgr.RecoverDueCatalogStorage(context.Background()); err != nil {
		t.Fatalf("RecoverDueCatalogStorage() error = %v", err)
	}
	volumeLists, snapshotLists, volumeGets, snapshotGets := repo.calls()
	if volumeLists != 0 || snapshotLists != 0 || len(snapshotGets) != 0 {
		t.Fatalf(
			"unexpected catalog scans/reads: volume_lists=%d snapshot_lists=%d snapshot_gets=%v",
			volumeLists,
			snapshotLists,
			snapshotGets,
		)
	}
	if len(volumeGets) != len(allocations) {
		t.Fatalf("volume point reads = %d, want %d", len(volumeGets), len(allocations))
	}
	filters, aborts, commits := store.counts()
	// Three volume pages (100, 100, 5) and one empty snapshot page.
	if filters != 4 || aborts != len(allocations) || commits != 0 {
		t.Fatalf(
			"recovery counts = filters:%d aborts:%d commits:%d, want 4/%d/0",
			filters,
			aborts,
			commits,
			len(allocations),
		)
	}
}

func TestRecoverDueCatalogStorageStopsWhenPaginationDoesNotAdvance(t *testing.T) {
	scope := storagequota.RegionRecoveryScope("region-1")
	allocations := make([]teamquota.RecoveryAllocation, 0, catalogQuotaRecoveryBatchSize)
	for i := 0; i < catalogQuotaRecoveryBatchSize; i++ {
		allocationID := fmt.Sprintf("allocation-%03d", i)
		allocations = append(allocations, dueCatalogAllocation(
			allocationID,
			teamquota.Owner{
				TeamID:    "team-1",
				Kind:      storagequota.OwnerKindVolume,
				ID:        fmt.Sprintf("volume-%03d", i),
				ClusterID: scope,
			},
			"volume_write",
			storagequota.VolumeTarget(100, 3),
			storagequota.VolumeTarget(150, 4),
		))
	}
	store := newPagedCatalogQuotaStore(allocations...)
	store.ignoreAfter = true
	repo := &recordingCatalogRepository{fakeRepo: newFakeRepo()}
	mgr := &Manager{
		repo:         repo,
		storageQuota: storagequota.New(store, scope),
	}

	err := mgr.RecoverDueCatalogStorage(context.Background())
	if err == nil || !strings.Contains(err.Error(), "pagination did not advance") {
		t.Fatalf("RecoverDueCatalogStorage() error = %v, want no-progress error", err)
	}
	filters, aborts, _ := store.counts()
	if filters != 3 {
		// Two repeated volume pages plus the independent empty snapshot page.
		t.Fatalf("recovery filter calls = %d, want 3", filters)
	}
	if aborts != 0 {
		t.Fatalf("unsafe absent mutations aborted = %d, want zero", aborts)
	}
	_, _, volumeGets, _ := repo.calls()
	if len(volumeGets) != 2*catalogQuotaRecoveryBatchSize {
		t.Fatalf(
			"bounded point reads = %d, want %d",
			len(volumeGets),
			2*catalogQuotaRecoveryBatchSize,
		)
	}
}

func TestRecoverDueCatalogStorageIsIdempotentAcrossReplicas(t *testing.T) {
	scope := storagequota.RegionRecoveryScope("region-1")
	owner := teamquota.Owner{
		TeamID:    "team-1",
		Kind:      storagequota.OwnerKindVolume,
		ID:        "volume-absent",
		ClusterID: scope,
	}
	store := newPagedCatalogQuotaStore(dueCatalogAllocation(
		"allocation-1",
		owner,
		"volume_create",
		storagequota.VolumeTarget(0, 0),
		storagequota.VolumeTarget(0, storagequota.CatalogObjectCount),
	))
	entered := make(chan struct{}, 2)
	released := make(chan struct{})
	defer func() {
		select {
		case <-released:
		default:
			close(released)
		}
	}()
	repo := &recordingCatalogRepository{
		fakeRepo:          newFakeRepo(),
		volumeGetEntered:  entered,
		volumeGetReleased: released,
	}
	managers := []*Manager{
		{repo: repo, storageQuota: storagequota.New(store, scope)},
		{repo: repo, storageQuota: storagequota.New(store, scope)},
	}
	errs := make(chan error, len(managers))
	for _, mgr := range managers {
		go func(manager *Manager) {
			errs <- manager.RecoverDueCatalogStorage(context.Background())
		}(mgr)
	}
	for range managers {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("replicas did not reach the same catalog point read")
		}
	}
	close(released)
	for range managers {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent RecoverDueCatalogStorage() error = %v", err)
		}
	}
	if allocation := store.allocation("allocation-1"); allocation.Operation != nil {
		t.Fatalf("recovered allocation = %+v, want terminal state", allocation)
	}
	_, aborts, _ := store.counts()
	if aborts != 1 {
		t.Fatalf("durable aborts = %d, want exactly one", aborts)
	}
}

func TestRecoverDueCatalogStorageContinuesAfterBadOwner(t *testing.T) {
	scope := storagequota.RegionRecoveryScope("region-1")
	badOwner := teamquota.Owner{
		TeamID:    "team-1",
		Kind:      storagequota.OwnerKindVolume,
		ID:        "volume-bad",
		ClusterID: scope,
	}
	goodOwner := teamquota.Owner{
		TeamID:    "team-1",
		Kind:      storagequota.OwnerKindVolume,
		ID:        "volume-good",
		ClusterID: scope,
	}
	store := newPagedCatalogQuotaStore(
		dueCatalogAllocation(
			"allocation-1",
			badOwner,
			"volume_create",
			storagequota.VolumeTarget(0, 0),
			storagequota.VolumeTarget(0, storagequota.CatalogObjectCount),
		),
		dueCatalogAllocation(
			"allocation-2",
			goodOwner,
			"volume_create",
			storagequota.VolumeTarget(0, 0),
			storagequota.VolumeTarget(0, storagequota.CatalogObjectCount),
		),
	)
	repo := &recordingCatalogRepository{fakeRepo: newFakeRepo()}
	repo.volumes[badOwner.ID] = &db.SandboxVolume{
		ID:      badOwner.ID,
		TeamID:  "another-team",
		Backend: "s3",
	}
	mgr := &Manager{
		repo:         repo,
		storageQuota: storagequota.New(store, scope),
	}

	err := mgr.RecoverDueCatalogStorage(context.Background())
	if err == nil || !strings.Contains(err.Error(), "conflicts with TeamQuota owner") {
		t.Fatalf("RecoverDueCatalogStorage() error = %v, want ownership conflict", err)
	}
	if allocation := store.allocation("allocation-1"); allocation.Operation == nil {
		t.Fatalf("bad owner allocation was changed: %+v", allocation)
	}
	if allocation := store.allocation("allocation-2"); allocation.Operation != nil {
		t.Fatalf("later owner was starved: %+v", allocation)
	}
	_, aborts, _ := store.counts()
	if aborts != 1 {
		t.Fatalf("later safe aborts = %d, want one", aborts)
	}
}

func TestRecoverDueCatalogStorageRejectsNonDuePageEntry(t *testing.T) {
	scope := storagequota.RegionRecoveryScope("region-1")
	allocation := dueCatalogAllocation(
		"allocation-1",
		teamquota.Owner{
			TeamID:    "team-1",
			Kind:      storagequota.OwnerKindVolume,
			ID:        "volume-1",
			ClusterID: scope,
		},
		"volume_create",
		storagequota.VolumeTarget(0, 0),
		storagequota.VolumeTarget(0, storagequota.CatalogObjectCount),
	)
	allocation.ReconcileDue = false
	store := newPagedCatalogQuotaStore(allocation)
	// Force the fake lister to violate OnlyDue so the manager-side contract
	// validation is exercised.
	store.recovery = &allocation
	store.allocations = nil
	repo := &recordingCatalogRepository{fakeRepo: newFakeRepo()}
	mgr := &Manager{
		repo: repo,
		storageQuota: storagequota.New(&nonDueCatalogQuotaStore{
			pagedCatalogQuotaStore: store,
			allocation:             allocation,
		}, scope),
	}

	err := mgr.RecoverDueCatalogStorage(context.Background())
	if err == nil || !strings.Contains(err.Error(), "non-due allocation") {
		t.Fatalf("RecoverDueCatalogStorage() error = %v, want non-due error", err)
	}
	_, _, volumeGets, snapshotGets := repo.calls()
	if len(volumeGets) != 0 || len(snapshotGets) != 0 {
		t.Fatalf("catalog reads occurred for invalid page: volumes=%v snapshots=%v", volumeGets, snapshotGets)
	}
}

type nonDueCatalogQuotaStore struct {
	*pagedCatalogQuotaStore
	allocation teamquota.RecoveryAllocation
}

func (s *nonDueCatalogQuotaStore) ListRecoveryAllocations(
	_ context.Context,
	filter teamquota.RecoveryAllocationFilter,
) ([]teamquota.RecoveryAllocation, error) {
	if filter.OwnerKind != storagequota.OwnerKindVolume {
		return nil, nil
	}
	return []teamquota.RecoveryAllocation{s.allocation}, nil
}
