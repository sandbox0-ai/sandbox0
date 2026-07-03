package service

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
)

type recordingSandboxLifecycleMeteringRecorder struct {
	facts []*SandboxPauseMeteringFact
	err   error
}

func (r *recordingSandboxLifecycleMeteringRecorder) RecordSandboxPaused(_ context.Context, fact *SandboxPauseMeteringFact) error {
	if fact != nil {
		copied := *fact
		r.facts = append(r.facts, &copied)
	}
	return r.err
}

func TestRecordSandboxPausedMeteringBuildsFactFromRecord(t *testing.T) {
	recorder := &recordingSandboxLifecycleMeteringRecorder{}
	svc := &SandboxService{
		config: SandboxServiceConfig{
			SandboxMemoryPerCPU: "4Gi",
			SandboxMaxMemory:    "32Gi",
		},
		lifecycleMetering: recorder,
	}
	claimedAt := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	pausedAt := claimedAt.Add(5 * time.Minute)
	record := &SandboxRecord{
		ID:                  "sandbox-1",
		TeamID:              "team-1",
		UserID:              "user-1",
		TemplateID:          "template-1",
		ClusterID:           "cluster-1",
		CurrentPodNamespace: "ns-1",
		ClaimedAt:           claimedAt,
		TemplateSpec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{
				Resources: v1alpha1.ResourceQuota{
					CPU:    resource.MustParse("500m"),
					Memory: resource.MustParse("1Gi"),
				},
			},
		},
	}

	svc.recordSandboxPausedMetering(context.Background(), record, pausedAt)

	if len(recorder.facts) != 1 {
		t.Fatalf("recorded facts = %d, want 1", len(recorder.facts))
	}
	fact := recorder.facts[0]
	if fact.SandboxID != "sandbox-1" || fact.TeamID != "team-1" || fact.TemplateID != "template-1" {
		t.Fatalf("fact identity = %#v", fact)
	}
	if fact.ResourceMillicpu != 500 || fact.ResourceMemoryMiB != 1024 {
		t.Fatalf("fact resources = %dm/%dMiB, want 500m/1024MiB", fact.ResourceMillicpu, fact.ResourceMemoryMiB)
	}
	if !fact.PausedAt.Equal(pausedAt) || !fact.ClaimedAt.Equal(claimedAt) {
		t.Fatalf("fact times = claimed %v paused %v", fact.ClaimedAt, fact.PausedAt)
	}
}

func TestRepositorySandboxLifecycleMeteringRecorderKeepsPauseEventWhenWindowMissing(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS metering CASCADE")
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS metering CASCADE")
	})
	require.NoError(t, meteringpkg.RunMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))

	repo := meteringpkg.NewRepository(pool)
	recorder := NewSandboxLifecycleMeteringRecorder(repo, "region-1", "cluster-1")
	claimedAt := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	pausedAt := claimedAt.Add(5 * time.Minute)

	require.NoError(t, recorder.RecordSandboxPaused(ctx, &SandboxPauseMeteringFact{
		SandboxID:   "sandbox-1",
		Namespace:   "ns-1",
		TeamID:      "team-1",
		UserID:      "user-1",
		TemplateID:  "template-1",
		ClaimedAt:   claimedAt,
		ActiveSince: claimedAt,
		PausedAt:    pausedAt,
	}))

	events, err := repo.ListEventsAfter(ctx, 0, 10)
	require.NoError(t, err)
	require.True(t, meteringTestHasEvent(events, meteringpkg.EventTypeSandboxPaused, "sandbox-1"), "missing sandbox.paused event: %#v", events)

	windows, err := repo.ListWindowsAfter(ctx, 0, 10)
	require.NoError(t, err)
	require.Empty(t, windows)

	state, err := repo.GetSandboxProjectionState(ctx, "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, state)
	require.True(t, state.Paused)
}

func meteringTestHasEvent(events []*meteringpkg.Event, eventType, sandboxID string) bool {
	for _, event := range events {
		if event != nil && event.EventType == eventType && event.SubjectID == sandboxID {
			return true
		}
	}
	return false
}
