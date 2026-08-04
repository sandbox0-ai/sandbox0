package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	corev1 "k8s.io/api/core/v1"
)

func TestResolveSandboxTemplateSourceUsesDurableClaimSpec(t *testing.T) {
	record := &SandboxRecord{
		ID:           "sandbox-1",
		TeamID:       "team-1",
		UserID:       "user-1",
		TemplateID:   "base",
		ClusterID:    "cluster-a",
		DesiredState: SandboxDesiredStateActive,
		TemplateSpec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "ubuntu:22.04"},
		},
	}
	store := &memorySandboxStore{records: map[string]*SandboxRecord{record.ID: record}}
	pod := createTestPod(record.ID, record.TeamID, record.TemplateID, controller.PoolTypeActive, time.Now().Add(-time.Minute), time.Now().Add(time.Hour), false)
	service := &SandboxService{sandboxStore: store, podLister: newTestPodLister(t, pod)}

	source, err := service.ResolveSandboxTemplateSource(context.Background(), record.ID, record.TeamID)
	if err != nil {
		t.Fatalf("ResolveSandboxTemplateSource() error = %v", err)
	}
	if source.SandboxID != record.ID || source.TeamID != record.TeamID || source.ClusterID != record.ClusterID {
		t.Fatalf("source identity = %#v, want sandbox/team/cluster from durable record", source)
	}
	if source.Spec.MainContainer.Image != record.TemplateSpec.MainContainer.Image {
		t.Fatalf("source image = %q, want %q", source.Spec.MainContainer.Image, record.TemplateSpec.MainContainer.Image)
	}
	source.Spec.MainContainer.Image = "changed"
	if record.TemplateSpec.MainContainer.Image != "ubuntu:22.04" {
		t.Fatal("resolved source spec must be a deep copy")
	}
}

func TestResolveSandboxTemplateSourceRejectsActiveSourceWithoutRunningRuntime(t *testing.T) {
	record := &SandboxRecord{
		ID:           "sandbox-1",
		TeamID:       "team-1",
		DesiredState: SandboxDesiredStateActive,
		TemplateSpec: v1alpha1.SandboxTemplateSpec{},
	}
	now := time.Now()
	tests := []struct {
		name string
		pods []*corev1.Pod
	}{
		{name: "missing"},
		{
			name: "failed",
			pods: []*corev1.Pod{
				createTestPodWithPhase(record.ID, record.TeamID, "base", controller.PoolTypeActive, now.Add(-time.Minute), now.Add(time.Hour), false, corev1.PodFailed),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &memorySandboxStore{records: map[string]*SandboxRecord{record.ID: record}}
			service := &SandboxService{sandboxStore: store, podLister: newTestPodLister(t, tt.pods...)}

			_, err := service.ResolveSandboxTemplateSource(context.Background(), record.ID, record.TeamID)

			if !errors.Is(err, template.ErrTemplateSourceNotReady) {
				t.Fatalf("ResolveSandboxTemplateSource() error = %v, want %v", err, template.ErrTemplateSourceNotReady)
			}
		})
	}
}

func TestResolveSandboxTemplateSourceRejectsUnavailableImageBuildCapability(t *testing.T) {
	t.Parallel()

	service := &SandboxService{
		sandboxStore: &memorySandboxStore{records: map[string]*SandboxRecord{}},
	}
	service.SetTemplateImageBuildAvailable(false)

	_, err := service.ResolveSandboxTemplateSource(context.Background(), "sandbox-1", "team-1")
	if !errors.Is(err, template.ErrTemplateSourceUnavailable) {
		t.Fatalf("ResolveSandboxTemplateSource() error = %v, want %v", err, template.ErrTemplateSourceUnavailable)
	}
}

func TestResolveSandboxTemplateSourceRejectsCrossTeamAndNonCaptureableState(t *testing.T) {
	tests := []struct {
		name        string
		record      *SandboxRecord
		teamID      string
		targetError error
	}{
		{
			name: "cross team",
			record: &SandboxRecord{
				ID:           "sandbox-1",
				TeamID:       "team-1",
				DesiredState: SandboxDesiredStateActive,
			},
			teamID:      "team-2",
			targetError: template.ErrTemplateSourceForbidden,
		},
		{
			name: "deleted",
			record: &SandboxRecord{
				ID:           "sandbox-1",
				TeamID:       "team-1",
				DesiredState: SandboxDesiredStateDeleted,
			},
			teamID:      "team-1",
			targetError: template.ErrTemplateSourceNotFound,
		},
		{
			name: "transitional",
			record: &SandboxRecord{
				ID:           "sandbox-1",
				TeamID:       "team-1",
				DesiredState: SandboxDesiredStateTerminating,
			},
			teamID:      "team-1",
			targetError: template.ErrTemplateSourceNotReady,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			service := &SandboxService{
				sandboxStore: &memorySandboxStore{records: map[string]*SandboxRecord{tt.record.ID: tt.record}},
			}
			_, err := service.ResolveSandboxTemplateSource(context.Background(), tt.record.ID, tt.teamID)
			if !errors.Is(err, tt.targetError) {
				t.Fatalf("ResolveSandboxTemplateSource() error = %v, want %v", err, tt.targetError)
			}
		})
	}
}
