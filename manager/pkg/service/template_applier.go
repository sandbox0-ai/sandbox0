package service

import (
	"context"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
)

// TemplateApplier adapts TemplateService to the reconciler interface.
type TemplateApplier struct {
	service *TemplateService
}

// NewTemplateApplier creates a TemplateApplier.
func NewTemplateApplier(service *TemplateService) *TemplateApplier {
	return &TemplateApplier{service: service}
}

func (a *TemplateApplier) ListTemplates(ctx context.Context) ([]*v1alpha1.SandboxTemplate, error) {
	return a.service.ListTemplates(ctx)
}

func (a *TemplateApplier) GetTemplate(ctx context.Context, id string) (*v1alpha1.SandboxTemplate, error) {
	return a.service.GetTemplate(ctx, id)
}

func (a *TemplateApplier) CreateTemplate(ctx context.Context, template *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error) {
	return a.service.CreateTemplate(ctx, template)
}

func (a *TemplateApplier) UpdateTemplate(ctx context.Context, template *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error) {
	return a.service.UpdateTemplate(ctx, template)
}

func (a *TemplateApplier) DeleteTemplate(ctx context.Context, id string) error {
	return a.service.DeleteTemplate(ctx, id)
}
