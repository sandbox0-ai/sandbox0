package service

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
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
