package sandboxstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestListSandboxExpirationCandidatesFiltersStateAndLifecycleIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	now := time.Date(2026, time.August, 21, 12, 10, 0, 0, time.UTC)
	record := func(id, state string, soft, hard time.Time) *SandboxRecord {
		return &SandboxRecord{
			ID: id, TeamID: "team-expiry", UserID: "user-expiry",
			TemplateID: "template-expiry", TemplateName: "template-expiry",
			TemplateNamespace: "template-default", ClusterID: "cluster-a",
			DesiredState:     state,
			ResourceMillicpu: 1000, ResourceMemoryMiB: 1024,
			ExpiresAt: soft, HardExpiresAt: hard, CreatedAt: now.Add(-time.Hour),
		}
	}
	records := []*SandboxRecord{
		record("hard-active", SandboxDesiredStateActive,
			now.Add(-time.Minute), now.Add(-2*time.Second)),
		record("hard-paused", SandboxDesiredStatePaused,
			time.Time{}, now.Add(-time.Second)),
		record("soft-active", SandboxDesiredStateActive,
			now.Add(-3*time.Second), now.Add(time.Hour)),
		record("soft-blocked", SandboxDesiredStateActive,
			now.Add(-2*time.Second), time.Time{}),
		record("soft-paused", SandboxDesiredStatePaused,
			now.Add(-time.Second), time.Time{}),
		record("hard-terminating", SandboxDesiredStateTerminating,
			time.Time{}, now.Add(-time.Second)),
		record("future", SandboxDesiredStateActive,
			now.Add(time.Second), now.Add(time.Hour)),
	}
	for _, candidate := range records {
		require.NoError(t, store.UpsertSandbox(ctx, candidate))
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO manager.sandbox_lifecycle_txns (
			txn_id, sandbox_id, kind, phase, source, epoch, from_generation
		) VALUES ($1, $2, $3, $4, $5, 1, 1)
	`, "pause-soft-blocked", "soft-blocked", SandboxLifecycleKindPause,
		SandboxLifecyclePhasePreparing, SandboxLifecycleSourceAuto)
	require.NoError(t, err)

	limited, err := store.ListSandboxExpirationCandidates(
		ctx, now, 2,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"hard-active", "hard-paused"}, expirationCandidateIDs(limited))

	candidates, err := store.ListSandboxExpirationCandidates(
		ctx, now, 20,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"hard-active", "hard-paused", "soft-active"}, expirationCandidateIDs(candidates))
	require.Equal(t, now.Add(-3*time.Second), candidates[2].ExpiresAt)
	require.Equal(t, now.Add(time.Hour), candidates[2].HardExpiresAt)
}

func expirationCandidateIDs(candidates []SandboxExpirationCandidate) []string {
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.SandboxID)
	}
	return ids
}
