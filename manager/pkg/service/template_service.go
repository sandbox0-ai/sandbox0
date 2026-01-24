package service

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/manager/pkg/controller"
	clientset "github.com/sandbox0-ai/infra/manager/pkg/generated/clientset/versioned"
	"github.com/sandbox0-ai/infra/manager/pkg/network"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TemplateService handles template operations
type TemplateService struct {
	crdClient      clientset.Interface
	templateLister controller.TemplateLister
	logger         *zap.Logger
	network        network.Provider
}

// NewTemplateService creates a new TemplateService
func NewTemplateService(
	crdClient clientset.Interface,
	templateLister controller.TemplateLister,
	networkProvider network.Provider,
	logger *zap.Logger,
) *TemplateService {
	return &TemplateService{
		crdClient:      crdClient,
		templateLister: templateLister,
		logger:         logger,
		network:        networkProvider,
	}
}

// CreateTemplate creates a new template
func (s *TemplateService) CreateTemplate(ctx context.Context, template *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error) {
	s.logger.Info("Creating template", zap.String("name", template.Name))

	// Ensure namespace is set. If not, use "default" or whatever.
	// We should probably use the same namespace as the manager or let the user specify.
	if template.Namespace == "" {
		cfg := config.LoadManagerConfig()
		if cfg.TemplateNamespace != "" {
			template.Namespace = cfg.TemplateNamespace
		} else {
			template.Namespace = "sandbox0"
		}
	}

	if s.network != nil {
		if err := s.network.EnsureBaseline(ctx, template.Namespace); err != nil {
			s.logger.Warn("Network provider baseline failed",
				zap.String("provider", s.network.Name()),
				zap.String("namespace", template.Namespace),
				zap.Error(err),
			)
		}
	}

	// Set default values if needed
	if template.Spec.Pool.MinIdle < 0 {
		template.Spec.Pool.MinIdle = 0
	}
	if template.Spec.Pool.MaxIdle < template.Spec.Pool.MinIdle {
		template.Spec.Pool.MaxIdle = template.Spec.Pool.MinIdle
	}

	result, err := s.crdClient.Sandbox0V1alpha1().SandboxTemplates(template.Namespace).Create(ctx, template, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create template: %w", err)
	}

	return result, nil
}

// GetTemplate gets a template by ID (name)
// We assume templates are in a specific namespace (usually "default" or configured one),
// but here we might need to know the namespace.
// However, the API typically just provides ID.
// Assuming the manager is configured with a default namespace or we search/use the lister.
// The lister's Get method takes namespace and name.
func (s *TemplateService) GetTemplate(ctx context.Context, namespace, id string) (*v1alpha1.SandboxTemplate, error) {
	template, err := s.templateLister.Get(namespace, id)
	if err != nil {
		return nil, err
	}
	return template, nil
}

// ListTemplates lists all templates in a namespace
func (s *TemplateService) ListTemplates(ctx context.Context, namespace string) ([]*v1alpha1.SandboxTemplate, error) {
	// If namespace is empty, we might want to list all or use default.
	// The lister's List method returns all templates from all namespaces the informer watches.
	// If we want to filter by namespace, we can do it manually or use the client.

	// Using lister for performance
	templates, err := s.templateLister.List()
	if err != nil {
		return nil, err
	}

	// Filter by namespace if provided
	if namespace != "" {
		var filtered []*v1alpha1.SandboxTemplate
		for _, t := range templates {
			if t.Namespace == namespace {
				filtered = append(filtered, t)
			}
		}
		return filtered, nil
	}

	return templates, nil
}

// UpdateTemplate updates an existing template
func (s *TemplateService) UpdateTemplate(ctx context.Context, template *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error) {
	s.logger.Info("Updating template", zap.String("name", template.Name))

	// Helper to get current version for optimistic locking
	current, err := s.crdClient.Sandbox0V1alpha1().SandboxTemplates(template.Namespace).Get(ctx, template.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get current template: %w", err)
	}

	template.ResourceVersion = current.ResourceVersion

	// Preserve status
	template.Status = current.Status

	result, err := s.crdClient.Sandbox0V1alpha1().SandboxTemplates(template.Namespace).Update(ctx, template, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("update template: %w", err)
	}

	return result, nil
}

// DeleteTemplate deletes a template
func (s *TemplateService) DeleteTemplate(ctx context.Context, namespace, id string) error {
	s.logger.Info("Deleting template", zap.String("name", id))

	err := s.crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil // Already deleted
		}
		return fmt.Errorf("delete template: %w", err)
	}

	return nil
}

// WarmPool triggers pool warming for a template
// This might just mean ensuring the pool settings are correct or explicitly triggering a reconciliation if needed.
// Since the operator watches changes, updating the template is usually enough.
// "Warming" usually implies increasing MinIdle.
func (s *TemplateService) WarmPool(ctx context.Context, namespace, id string, count int32) error {
	s.logger.Info("Warming pool", zap.String("name", id), zap.Int32("count", count))

	// Get current template
	template, err := s.crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace).Get(ctx, id, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get template: %w", err)
	}

	// Update MinIdle if needed
	if template.Spec.Pool.MinIdle < count {
		template.Spec.Pool.MinIdle = count
		if template.Spec.Pool.MaxIdle < count {
			template.Spec.Pool.MaxIdle = count
		}

		_, err = s.crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace).Update(ctx, template, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("update template pool settings: %w", err)
		}
	}

	return nil
}
