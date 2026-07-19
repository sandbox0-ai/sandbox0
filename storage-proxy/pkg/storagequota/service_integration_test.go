package storagequota

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

func TestMutateRejectsStaleCrossReplicaGrowthBeforePhysicalMutation(t *testing.T) {
	repo, _ := newStorageQuotaTestRepository(t)
	ctx := context.Background()
	const teamID = "team-stale-growth"
	installStoragePolicies(t, repo, teamID, 15)

	serviceA := New(repo, RegionRecoveryScope("region-1"))
	serviceB := New(repo, RegionRecoveryScope("region-1"))
	owner := serviceA.VolumeOwner(teamID, "volume-1")
	initial := VolumeTarget(0, 1)
	if err := repo.ReconcileTarget(ctx, owner, initial, teamquota.RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}

	var physicalMu sync.Mutex
	physical := initial.Clone()
	measure := func() (teamquota.Values, error) {
		physicalMu.Lock()
		defer physicalMu.Unlock()
		return physical.Clone(), nil
	}
	addTenBound := func(before teamquota.Values) (teamquota.Values, error) {
		target := before.Clone()
		target[teamquota.KeyVolumeStorageBytes] += 10
		return target, nil
	}
	addTenPhysical := func() error {
		physicalMu.Lock()
		defer physicalMu.Unlock()
		physical[teamquota.KeyVolumeStorageBytes] += 10
		return nil
	}

	bObserved := make(chan struct{})
	aDone := make(chan struct{})
	var signalOnce sync.Once
	var bMutationCalls atomic.Int32
	bErr := make(chan error, 1)
	go func() {
		bErr <- serviceB.Mutate(
			ctx,
			owner,
			"volume_write_b",
			measure,
			func(before teamquota.Values) (teamquota.Values, error) {
				signalOnce.Do(func() {
					close(bObserved)
					<-aDone
				})
				return addTenBound(before)
			},
			func() error {
				bMutationCalls.Add(1)
				return addTenPhysical()
			},
			measure,
		)
	}()

	select {
	case <-bObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("replica B did not capture its stale physical baseline")
	}
	aErr := serviceA.Mutate(
		ctx,
		owner,
		"volume_write_a",
		measure,
		addTenBound,
		addTenPhysical,
		measure,
	)
	close(aDone)
	if aErr != nil {
		t.Fatalf("replica A Mutate() error = %v", aErr)
	}

	select {
	case err := <-bErr:
		if !teamquota.IsExceeded(err) {
			t.Fatalf("replica B Mutate() error = %v, want ExceededError", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replica B did not finish")
	}
	if got := bMutationCalls.Load(); got != 0 {
		t.Fatalf("replica B physical mutation calls = %d, want 0", got)
	}
	physicalMu.Lock()
	finalPhysical := physical.Clone()
	physicalMu.Unlock()
	if got := finalPhysical[teamquota.KeyVolumeStorageBytes]; got != 10 {
		t.Fatalf("physical volume bytes = %d, want 10", got)
	}
	assertStorageQuotaStatus(t, repo, teamID, teamquota.KeyVolumeStorageBytes, 10, 0)
	if err := repo.ValidateUsageInvariant(ctx, teamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestZeroGrowthMutationFencesCrossReplicaOwner(t *testing.T) {
	repo, _ := newStorageQuotaTestRepository(t)
	ctx := context.Background()
	const teamID = "team-zero-growth-fence"
	installStoragePolicies(t, repo, teamID, 100)

	serviceA := New(repo, RegionRecoveryScope("region-1"))
	serviceB := New(repo, RegionRecoveryScope("region-1"))
	owner := serviceA.VolumeOwner(teamID, "volume-1")
	target := VolumeTarget(10, 1)
	if err := repo.ReconcileTarget(ctx, owner, target, teamquota.RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}

	aMutating := make(chan struct{})
	releaseA := make(chan struct{})
	aErr := make(chan error, 1)
	go func() {
		aErr <- serviceA.Mutate(
			ctx,
			owner,
			"volume_metadata_a",
			fixedMeasure(target),
			fixedBound(target),
			func() error {
				close(aMutating)
				<-releaseA
				return nil
			},
			fixedMeasure(target),
		)
	}()

	select {
	case <-aMutating:
	case <-time.After(5 * time.Second):
		t.Fatal("replica A did not acquire the zero-growth operation fence")
	}
	var bMutationCalls atomic.Int32
	err := serviceB.Mutate(
		ctx,
		owner,
		"volume_metadata_b",
		fixedMeasure(target),
		fixedBound(target),
		func() error {
			bMutationCalls.Add(1)
			return nil
		},
		fixedMeasure(target),
	)
	var conflict *teamquota.OperationConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("replica B Mutate() error = %v, want OperationConflictError", err)
	}
	if got := bMutationCalls.Load(); got != 0 {
		t.Fatalf("replica B physical mutation calls = %d, want 0", got)
	}
	close(releaseA)
	select {
	case err := <-aErr:
		if err != nil {
			t.Fatalf("replica A Mutate() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replica A did not finish")
	}
	assertStorageQuotaStatus(t, repo, teamID, teamquota.KeyVolumeStorageBytes, 10, 0)
	if err := repo.ValidateUsageInvariant(ctx, teamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestAdoptExistingRecoversOnlyExpiredReplicaMutation(t *testing.T) {
	repo, pool := newStorageQuotaTestRepository(t)
	ctx := context.Background()
	const teamID = "team-recovery-deadline"
	installStoragePolicies(t, repo, teamID, 100)

	serviceA := New(repo, RegionRecoveryScope("region-1"))
	serviceB := New(repo, RegionRecoveryScope("region-1"))
	owner := serviceA.VolumeOwner(teamID, "volume-1")
	var physicalMu sync.Mutex
	physical := VolumeTarget(10, 1)
	if err := repo.ReconcileTarget(ctx, owner, physical, teamquota.RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}
	measure := func() (teamquota.Values, error) {
		physicalMu.Lock()
		defer physicalMu.Unlock()
		return physical.Clone(), nil
	}

	aMutating := make(chan struct{})
	releaseA := make(chan struct{})
	aErr := make(chan error, 1)
	go func() {
		aErr <- serviceA.Mutate(
			ctx,
			owner,
			"volume_write_a",
			measure,
			func(before teamquota.Values) (teamquota.Values, error) {
				target := before.Clone()
				target[teamquota.KeyVolumeStorageBytes] += 10
				return target, nil
			},
			func() error {
				close(aMutating)
				<-releaseA
				physicalMu.Lock()
				physical[teamquota.KeyVolumeStorageBytes] += 10
				physicalMu.Unlock()
				return nil
			},
			measure,
		)
	}()
	select {
	case <-aMutating:
	case <-time.After(5 * time.Second):
		t.Fatal("replica A did not establish its prepared mutation")
	}

	fresh, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(fresh) error = %v", err)
	}
	if fresh == nil || fresh.Operation == nil || fresh.ReconcileAfter == nil || fresh.ReconcileDue {
		t.Fatalf("fresh recovery allocation = %+v, want a not-due operation", fresh)
	}
	freshOperation := *fresh.Operation
	if err := serviceB.AdoptExisting(ctx, owner, fixedMeasure(VolumeTarget(10, 1))); err != nil {
		t.Fatalf("replica B AdoptExisting(fresh) error = %v", err)
	}
	stillFresh, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(after fresh adoption) error = %v", err)
	}
	if stillFresh == nil || stillFresh.Operation == nil || *stillFresh.Operation != freshOperation {
		t.Fatalf("fresh prepared operation was replaced or aborted: %+v", stillFresh)
	}
	assertStorageQuotaStatus(t, repo, teamID, teamquota.KeyVolumeStorageBytes, 10, 10)

	close(releaseA)
	select {
	case err := <-aErr:
		if err != nil {
			t.Fatalf("replica A Mutate() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replica A did not finish")
	}
	assertStorageQuotaStatus(t, repo, teamID, teamquota.KeyVolumeStorageBytes, 20, 0)

	expiredOperation := teamquota.Operation{
		ID:   "crashed-volume-write",
		Kind: "volume_write",
	}
	if _, err := repo.ReserveDelta(ctx, teamquota.DeltaRequest{
		Owner:     owner,
		Operation: expiredOperation,
		Delta:     VolumeTarget(10, 0),
		Observed:  VolumeTarget(20, 1),
	}); err != nil {
		t.Fatalf("ReserveDelta(crashed operation) error = %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE quota.allocations
		SET reconcile_after = NOW() - INTERVAL '1 second'
		WHERE team_id = $1 AND owner_kind = $2 AND owner_id = $3
	`, owner.TeamID, owner.Kind, owner.ID); err != nil {
		t.Fatalf("expire crashed operation: %v", err)
	}
	expired, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(expired) error = %v", err)
	}
	if expired == nil || expired.Operation == nil || !expired.ReconcileDue {
		t.Fatalf("expired recovery allocation = %+v, want due operation", expired)
	}
	if err := serviceB.AdoptExisting(ctx, owner, fixedMeasure(VolumeTarget(20, 1))); err != nil {
		t.Fatalf("replica B AdoptExisting(expired) error = %v", err)
	}
	recovered, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation(recovered) error = %v", err)
	}
	if recovered == nil || recovered.Operation != nil {
		t.Fatalf("recovered allocation = %+v, want no prepared operation", recovered)
	}
	assertStorageQuotaStatus(t, repo, teamID, teamquota.KeyVolumeStorageBytes, 20, 0)
	if err := repo.ValidateUsageInvariant(ctx, teamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func TestAdoptExistingSkipsStaleExactAfterConcurrentMutation(t *testing.T) {
	repo, _ := newStorageQuotaTestRepository(t)
	ctx := context.Background()
	const teamID = "team-stale-catalog-observation"
	installStoragePolicies(t, repo, teamID, 1000)

	serviceA := New(repo, RegionRecoveryScope("region-1"))
	serviceB := New(repo, RegionRecoveryScope("region-1"))
	owner := serviceA.VolumeOwner(teamID, "volume-1")
	initial := VolumeTarget(100, 1)
	if err := repo.ReconcileTarget(ctx, owner, initial, teamquota.RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(initial) error = %v", err)
	}
	physical := initial.Clone()
	measurePhysical := func() (teamquota.Values, error) {
		return physical.Clone(), nil
	}

	err := serviceA.AdoptExisting(
		ctx,
		owner,
		func() (teamquota.Values, error) {
			// Model a startup scan that observed an intermediate physical size
			// before another replica completed its fenced mutation.
			stale := VolumeTarget(125, 1)
			mutationErr := serviceB.Mutate(
				ctx,
				owner,
				"concurrent_volume_write",
				measurePhysical,
				fixedBound(VolumeTarget(150, 1)),
				func() error {
					physical = VolumeTarget(150, 1)
					return nil
				},
				measurePhysical,
			)
			if mutationErr != nil {
				return nil, mutationErr
			}
			return stale, nil
		},
	)
	if err != nil {
		t.Fatalf("AdoptExisting() error = %v", err)
	}

	allocation, err := repo.GetRecoveryAllocation(ctx, owner)
	if err != nil {
		t.Fatalf("GetRecoveryAllocation() error = %v", err)
	}
	if allocation == nil || !targetsEqual(allocation.Committed, physical) {
		t.Fatalf("allocation = %+v, want committed %v", allocation, physical)
	}
	assertStorageQuotaStatus(t, repo, teamID, teamquota.KeyVolumeStorageBytes, 150, 0)
	if err := repo.ValidateUsageInvariant(ctx, teamID); err != nil {
		t.Fatalf("ValidateUsageInvariant() error = %v", err)
	}
}

func installStoragePolicies(
	t *testing.T,
	repo *teamquota.Repository,
	teamID string,
	byteLimit int64,
) {
	t.Helper()
	ctx := context.Background()
	for _, policy := range []teamquota.Policy{
		{
			Key:   teamquota.KeyVolumeStorageBytes,
			Kind:  teamquota.KindCapacity,
			Limit: byteLimit,
		},
		{
			Key:   teamquota.KeyStorageObjectCount,
			Kind:  teamquota.KindCapacity,
			Limit: 100,
		},
	} {
		if err := repo.UnsafePutTeamPolicyForTest(ctx, teamID, policy); err != nil {
			t.Fatalf("PutTeamPolicy(%s) error = %v", policy.Key, err)
		}
	}
}

func assertStorageQuotaStatus(
	t *testing.T,
	repo *teamquota.Repository,
	teamID string,
	key teamquota.Key,
	committed int64,
	reserved int64,
) {
	t.Helper()
	statuses, err := repo.ListStatus(context.Background(), teamID)
	if err != nil {
		t.Fatalf("ListStatus() error = %v", err)
	}
	for _, status := range statuses {
		if status.Key == key {
			if status.Committed != committed || status.Reserved != reserved {
				t.Fatalf(
					"status for %s = committed %d reserved %d, want %d/%d",
					key,
					status.Committed,
					status.Reserved,
					committed,
					reserved,
				)
			}
			return
		}
	}
	t.Fatalf("status for %s is missing", key)
}

func newStorageQuotaTestRepository(t *testing.T) (*teamquota.Repository, *pgxpool.Pool) {
	t.Helper()
	pool := newStorageQuotaTestDatabase(t)
	if err := teamquota.RunMigrations(context.Background(), pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	return teamquota.NewRepository(pool), pool
}

func newStorageQuotaTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	ctx := context.Background()
	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatalf("connect test database admin: %v", err)
	}
	databaseName := "storage_quota_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedName := `"` + strings.ReplaceAll(databaseName, `"`, `""`) + `"`
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quotedName); err != nil {
		admin.Close()
		t.Fatalf("create test database: %v", err)
	}
	testConfig := adminConfig.Copy()
	testConfig.ConnConfig.Database = databaseName
	testConfig.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, testConfig)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP DATABASE "+quotedName)
		admin.Close()
		t.Fatalf("connect isolated test database: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+quotedName); err != nil {
			t.Errorf("drop test database: %v", err)
		}
		admin.Close()
	})
	return pool
}
