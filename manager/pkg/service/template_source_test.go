package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

func TestResolveSandboxTemplateSourceUsesDurableClaimSpec(t *testing.T) {
	record := &SandboxRecord{
		ID:         "sandbox-1",
		TeamID:     "team-1",
		UserID:     "user-1",
		TemplateID: "base",
		ClusterID:  "cluster-a",
		Status:     SandboxStatusRunning,
		TemplateSpec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{Image: "ubuntu:22.04"},
		},
	}
	store := &memorySandboxStore{records: map[string]*SandboxRecord{record.ID: record}}
	service := &SandboxService{sandboxStore: store}

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
				ID:     "sandbox-1",
				TeamID: "team-1",
				Status: SandboxStatusRunning,
			},
			teamID:      "team-2",
			targetError: template.ErrTemplateSourceForbidden,
		},
		{
			name: "deleted",
			record: &SandboxRecord{
				ID:     "sandbox-1",
				TeamID: "team-1",
				Status: SandboxStatusDeleted,
			},
			teamID:      "team-1",
			targetError: template.ErrTemplateSourceNotFound,
		},
		{
			name: "transitional",
			record: &SandboxRecord{
				ID:     "sandbox-1",
				TeamID: "team-1",
				Status: "pausing",
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
