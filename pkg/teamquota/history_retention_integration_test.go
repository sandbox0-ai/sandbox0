package teamquota

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestCapacityHistoryRetentionPreservesRecoveryAndLiveReplay(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	teamID := "team-history-safety"
	if err := repo.UnsafePutTeamPolicyForTest(ctx, teamID, Policy{
		Key:   KeySandboxRuntimeCount,
		Kind:  KindCapacity,
		Limit: 100,
	}); err != nil {
		t.Fatalf("PutTeamPolicy(setup) error = %v", err)
	}

	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO quota.allocations (
			allocation_id, team_id, owner_kind, owner_id, cluster_id,
			state, last_operation_id, last_operation_generation,
			last_operation_result, operation_fence_generation, updated_at
		) VALUES
			(
				'live-history', $1, 'sandbox', 'live-history', 'cluster-1',
				'active', 'keep-history', 7, 'committed', 7, NOW()
			),
			(
				'live-transfer', $1, 'sandbox', 'live-transfer', 'cluster-1',
				'active', 'keep-transfer', 9, 'committed', 9, NOW()
			),
			(
				'prepared-allocation', $1, 'sandbox', 'prepared', 'cluster-1',
				'reserved', '', 0, '', 10, NOW()
			),
			(
				'released-safe', $1, 'sandbox', 'released-safe', 'cluster-1',
				'released', 'release-safe', 1, 'committed', 1,
				NOW() - INTERVAL '48 hours'
			),
			(
				'released-nonzero', $1, 'sandbox', 'released-nonzero', 'cluster-1',
				'released', 'release-nonzero', 1, 'committed', 1,
				NOW() - INTERVAL '48 hours'
			)
	`, teamID); err != nil {
		t.Fatalf("insert retention allocations: %v", err)
	}
	if _, err := repo.pool.Exec(ctx, `
		UPDATE quota.allocations
		SET operation_id = 'prepared-history',
			operation_kind = 'resize',
			operation_generation = 10,
			operation_base_state = 'active',
			reconcile_after = NOW()
		WHERE allocation_id = 'prepared-allocation'
	`); err != nil {
		t.Fatalf("prepare allocation recovery fixture: %v", err)
	}
	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO quota.allocation_items (
			allocation_id, quota_key, committed_value, pending_value
		) VALUES
			('released-nonzero', $1, 1, NULL),
			('prepared-allocation', $1, 1, 2)
	`, string(KeySandboxRuntimeCount)); err != nil {
		t.Fatalf("insert allocation item fixtures: %v", err)
	}
	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO quota.allocation_operations (
			allocation_id, operation_id, operation_kind,
			operation_generation, request_fingerprint, state,
			created_at, completed_at
		) VALUES
			(
				'live-history', 'keep-history', 'resize',
				7, 'keep', 'committed',
				NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours'
			),
			(
				'live-history', 'drop-history', 'resize',
				6, 'drop', 'committed',
				NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours'
			),
			(
				'prepared-allocation', 'prepared-history', 'resize',
				10, 'prepared', 'prepared', NOW(), NULL
			)
	`); err != nil {
		t.Fatalf("insert allocation history fixtures: %v", err)
	}
	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO quota.transfer_operations (
			team_id, operation_id, operation_kind, operation_generation,
			request_fingerprint, state,
			source_allocation_id, destination_allocation_id,
			pod_namespace, pod_name, pod_uid, runtime_generation,
			created_at, completed_at
		) VALUES
			(
				$1, 'keep-transfer', 'hot_claim', 9,
				'keep', 'committed',
				'live-history', 'live-transfer',
				'sandbox', 'pod-1', 'uid-1', 9,
				NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours'
			),
			(
				$1, 'drop-transfer', 'hot_claim', 8,
				'drop', 'aborted',
				'live-history', 'live-transfer',
				'sandbox', 'pod-1', 'uid-1', 8,
				NOW() - INTERVAL '48 hours', NOW() - INTERVAL '48 hours'
			),
			(
				$1, 'prepared-transfer', 'hot_claim', 10,
				'prepared', 'prepared',
				'live-history', 'live-transfer',
				'sandbox', 'pod-2', 'uid-2', 10,
				NOW(), NULL
			)
	`, teamID); err != nil {
		t.Fatalf("insert transfer history fixtures: %v", err)
	}

	if _, err := repo.pool.Exec(ctx, `
		UPDATE quota.team_states
		SET revision = $2
		WHERE team_id = $1
	`, teamID, capacityHistoryPruneRevisionGap-1); err != nil {
		t.Fatalf("prepare automatic prune revision: %v", err)
	}
	if err := repo.ReconcileTarget(ctx, Owner{
		TeamID:    teamID,
		Kind:      "sandbox",
		ID:        "reconcile-trigger",
		ClusterID: "cluster-1",
	}, Values{KeySandboxRuntimeCount: 0}, RuntimeRef{}); err != nil {
		t.Fatalf("ReconcileTarget(trigger prune) error = %v", err)
	}

	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.allocation_operations
		WHERE operation_id = 'keep-history'
	`, 1)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.allocation_operations
		WHERE operation_id = 'drop-history'
	`, 0)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.allocation_operations
		WHERE operation_id = 'prepared-history' AND state = 'prepared'
	`, 1)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.transfer_operations
		WHERE operation_id = 'keep-transfer'
	`, 1)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.transfer_operations
		WHERE operation_id = 'drop-transfer'
	`, 0)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.transfer_operations
		WHERE operation_id = 'prepared-transfer' AND state = 'prepared'
	`, 1)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.allocations
		WHERE allocation_id = 'released-safe'
	`, 0)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.allocations
		WHERE allocation_id = 'released-nonzero'
	`, 1)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.allocations
		WHERE allocation_id IN (
			'live-history', 'live-transfer', 'prepared-allocation'
		)
	`, 3)
}

func TestCapacityHistoryRetentionCapsTerminalRowsAcrossReplicas(t *testing.T) {
	repo := newTeamQuotaTestRepository(t)
	ctx := context.Background()
	teamID := "team-history-caps"
	if err := repo.UnsafePutTeamPolicyForTest(ctx, teamID, Policy{
		Key:   KeySandboxRuntimeCount,
		Kind:  KindCapacity,
		Limit: 100,
	}); err != nil {
		t.Fatalf("PutTeamPolicy(setup) error = %v", err)
	}

	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO quota.allocations (
			allocation_id, team_id, owner_kind, owner_id, cluster_id,
			state, last_operation_id, last_operation_generation,
			last_operation_result, operation_fence_generation
		) VALUES
			(
				'history-owner', $1, 'sandbox', 'history-owner', 'cluster-1',
				'active', 'history-protected', 1, 'committed', $2
			),
			(
				'transfer-source', $1, 'warm_pool', 'transfer-source', 'cluster-1',
				'active', 'transfer-protected', 1, 'committed', $3
			),
			(
				'transfer-destination', $1, 'sandbox', 'transfer-destination', 'cluster-1',
				'active', 'transfer-protected', 1, 'committed', $3
			)
	`, teamID, maxTerminalOperationsPerOwner+5,
		maxTerminalTransfersPerTeam+5); err != nil {
		t.Fatalf("insert cap allocations: %v", err)
	}
	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO quota.allocation_operations (
			allocation_id, operation_id, operation_kind,
			operation_generation, request_fingerprint, state,
			created_at, completed_at
		)
		SELECT
			'history-owner',
			CASE
				WHEN value = 1 THEN 'history-protected'
				ELSE 'history-' || value::TEXT
			END,
			'resize',
			value,
			'fingerprint-' || value::TEXT,
			'committed',
			NOW(),
			NOW()
		FROM generate_series(1, $1::BIGINT) AS value
	`, maxTerminalOperationsPerOwner+5); err != nil {
		t.Fatalf("insert capped allocation history: %v", err)
	}
	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO quota.transfer_operations (
			team_id, operation_id, operation_kind, operation_generation,
			request_fingerprint, state,
			source_allocation_id, destination_allocation_id,
			pod_namespace, pod_name, pod_uid, runtime_generation,
			created_at, completed_at
		)
		SELECT
			$1,
			CASE
				WHEN value = 1 THEN 'transfer-protected'
				ELSE 'transfer-' || value::TEXT
			END,
			'hot_claim',
			value,
			'fingerprint-' || value::TEXT,
			'committed',
			'transfer-source',
			'transfer-destination',
			'sandbox',
			'pod-' || value::TEXT,
			'uid-' || value::TEXT,
			value,
			NOW(),
			NOW()
		FROM generate_series(1, $2::BIGINT) AS value
	`, teamID, maxTerminalTransfersPerTeam+5); err != nil {
		t.Fatalf("insert capped transfer history: %v", err)
	}
	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO quota.allocations (
			allocation_id, team_id, owner_kind, owner_id, cluster_id,
			state, updated_at
		)
		SELECT
			'released-' || value::TEXT,
			$1,
			'sandbox',
			'released-' || value::TEXT,
			'cluster-1',
			'released',
			NOW()
		FROM generate_series(1, $2::BIGINT) AS value
	`, teamID, maxReleasedAllocationsPerTeam+5); err != nil {
		t.Fatalf("insert capped released allocations: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for index := 0; index < 8; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- pruneCapacityHistoryForTest(ctx, repo, teamID)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent pruneCapacityHistoryForTest() error = %v", err)
		}
	}

	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.allocation_operations
		WHERE allocation_id = 'history-owner'
			AND state IN ('committed', 'aborted')
	`, maxTerminalOperationsPerOwner)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.transfer_operations
		WHERE team_id = 'team-history-caps'
			AND state IN ('committed', 'aborted')
	`, maxTerminalTransfersPerTeam)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.allocations
		WHERE team_id = 'team-history-caps'
			AND state = 'released'
	`, maxReleasedAllocationsPerTeam)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.allocation_operations
		WHERE operation_id = 'history-protected'
	`, 1)
	assertTeamQuotaCount(t, repo.pool, `
		SELECT COUNT(*)
		FROM quota.transfer_operations
		WHERE operation_id = 'transfer-protected'
	`, 1)
}

func pruneCapacityHistoryForTest(ctx context.Context, repo *Repository, teamID string) error {
	return repo.inTx(ctx, "test team quota history retention", func(tx pgx.Tx) error {
		if err := lockTeam(ctx, tx, teamID); err != nil {
			return err
		}
		return pruneTeamCapacityHistory(ctx, tx, teamID)
	})
}
