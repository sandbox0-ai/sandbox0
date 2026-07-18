package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/template"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// ResolveSandboxTemplateSource returns the durable template context recorded
// when the source sandbox was claimed.
func (s *SandboxService) ResolveSandboxTemplateSource(ctx context.Context, sandboxID, teamID string) (*template.SandboxTemplateSource, error) {
	if s == nil || s.sandboxStore == nil {
		return nil, template.ErrTemplateSourceUnavailable
	}
	if s.templateImageBuildCapabilityConfigured && !s.templateImageBuildAvailable {
		return nil, fmt.Errorf("%w: template image publisher is not configured in the source cluster", template.ErrTemplateSourceUnavailable)
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	if sandboxID == "" || teamID == "" {
		return nil, fmt.Errorf("%w: sandbox_id and team_id are required", template.ErrTemplateSourceNotFound)
	}
	record, err := s.sandboxStore.GetSandbox(ctx, sandboxID)
	if err != nil {
		if errors.Is(err, ErrSandboxRecordNotFound) || apierrors.IsNotFound(err) {
			return nil, template.ErrTemplateSourceNotFound
		}
		return nil, fmt.Errorf("%w: %v", template.ErrTemplateSourceUnavailable, err)
	}
	if err := validateRootFSSourceSandboxRecord(record, sandboxID, teamID, s.now()); err != nil {
		switch {
		case apierrors.IsNotFound(err):
			return nil, template.ErrTemplateSourceNotFound
		case apierrors.IsForbidden(err):
			return nil, template.ErrTemplateSourceForbidden
		default:
			return nil, fmt.Errorf("%w: %v", template.ErrTemplateSourceNotReady, err)
		}
	}
	return &template.SandboxTemplateSource{
		SandboxID:  record.ID,
		TeamID:     record.TeamID,
		UserID:     record.UserID,
		ClusterID:  record.ClusterID,
		TemplateID: record.TemplateID,
		Spec:       *record.TemplateSpec.DeepCopy(),
	}, nil
}
