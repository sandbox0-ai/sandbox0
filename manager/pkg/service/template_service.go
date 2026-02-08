package service

import (
	"context"
	"fmt"
	"os"

	"github.com/sandbox0-ai/infra/infra-operator/api/config"
	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/manager/pkg/controller"
	clientset "github.com/sandbox0-ai/infra/manager/pkg/generated/clientset/versioned"
	"github.com/sandbox0-ai/infra/manager/pkg/network"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

// TemplateService handles template operations
type TemplateService struct {
	k8sClient       kubernetes.Interface
	crdClient       clientset.Interface
	templateLister  controller.TemplateLister
	namespaceLister corelisters.NamespaceLister
	logger          *zap.Logger
	network         network.Provider
	registry        config.RegistryConfig
}

// NewTemplateService creates a new TemplateService
func NewTemplateService(
	k8sClient kubernetes.Interface,
	crdClient clientset.Interface,
	templateLister controller.TemplateLister,
	namespaceLister corelisters.NamespaceLister,
	networkProvider network.Provider,
	registryConfig config.RegistryConfig,
	logger *zap.Logger,
) *TemplateService {
	if networkProvider == nil {
		networkProvider = network.NewNoopProvider()
	}
	return &TemplateService{
		k8sClient:       k8sClient,
		crdClient:       crdClient,
		templateLister:  templateLister,
		namespaceLister: namespaceLister,
		logger:          logger,
		network:         networkProvider,
		registry:        registryConfig,
	}
}

// CreateTemplate creates a new template
func (s *TemplateService) CreateTemplate(ctx context.Context, template *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error) {
	s.logger.Info("Creating template", zap.String("name", template.Name))

	namespace, err := s.resolveTemplateNamespace(template)
	if err != nil {
		return nil, fmt.Errorf("resolve template namespace: %w", err)
	}
	template.Namespace = namespace

	if err := s.ensureNamespace(ctx, namespace); err != nil {
		return nil, err
	}

	if s.network != nil {
		if err := s.network.EnsureBaseline(ctx, namespace); err != nil {
			s.logger.Warn("Network provider baseline failed",
				zap.String("provider", s.network.Name()),
				zap.String("namespace", namespace),
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

	result, err := s.crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace).Create(ctx, template, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create template: %w", err)
	}

	return result, nil
}

// GetTemplate gets a template by ID (name) from the configured namespace.
func (s *TemplateService) GetTemplate(ctx context.Context, id string) (*v1alpha1.SandboxTemplate, error) {
	template, err := s.findTemplateByName(id)
	if err != nil {
		return nil, err
	}
	return template, nil
}

// ListTemplates lists templates across namespaces.
func (s *TemplateService) ListTemplates(ctx context.Context) ([]*v1alpha1.SandboxTemplate, error) {
	templates, err := s.templateLister.List()
	if err != nil {
		return nil, err
	}
	return templates, nil
}

// UpdateTemplate updates an existing template
func (s *TemplateService) UpdateTemplate(ctx context.Context, template *v1alpha1.SandboxTemplate) (*v1alpha1.SandboxTemplate, error) {
	s.logger.Info("Updating template", zap.String("name", template.Name))

	// Helper to get current version for optimistic locking
	current, err := s.findTemplateByName(template.Name)
	if err != nil {
		return nil, fmt.Errorf("get current template: %w", err)
	}
	namespace := current.Namespace
	template.Namespace = namespace

	template.ResourceVersion = current.ResourceVersion

	// Preserve status
	template.Status = current.Status

	result, err := s.crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace).Update(ctx, template, metav1.UpdateOptions{})
	if err != nil {
		return nil, fmt.Errorf("update template: %w", err)
	}

	return result, nil
}

// DeleteTemplate deletes a template from the configured namespace.
func (s *TemplateService) DeleteTemplate(ctx context.Context, id string) error {
	s.logger.Info("Deleting template", zap.String("name", id))

	existing, err := s.findTemplateByName(id)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("resolve template namespace: %w", err)
	}
	err = s.crdClient.Sandbox0V1alpha1().SandboxTemplates(existing.Namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil // Already deleted
		}
		return fmt.Errorf("delete template: %w", err)
	}

	return nil
}

func (s *TemplateService) resolveTemplateNamespace(template *v1alpha1.SandboxTemplate) (string, error) {
	if template.Namespace != "" {
		return template.Namespace, nil
	}
	if template.Labels != nil && template.Labels["sandbox0.ai/template-scope"] == naming.ScopeTeam {
		teamID := ""
		if template.Annotations != nil {
			teamID = template.Annotations["sandbox0.ai/template-team-id"]
		}
		return naming.TemplateNamespaceForTeam(teamID)
	}
	return naming.TemplateNamespaceForBuiltin(template.Name)
}

func (s *TemplateService) findTemplateByName(id string) (*v1alpha1.SandboxTemplate, error) {
	templates, err := s.templateLister.List()
	if err != nil {
		return nil, err
	}
	for _, template := range templates {
		if template.Name == id {
			return template, nil
		}
	}
	return nil, errors.NewNotFound(v1alpha1.Resource("sandboxtemplate"), id)
}

func (s *TemplateService) ensureNamespace(ctx context.Context, namespace string) error {
	if s.k8sClient == nil {
		return fmt.Errorf("k8s client is required to ensure namespace %s", namespace)
	}
	if s.namespaceLister == nil {
		return fmt.Errorf("namespace lister is required to ensure namespace %s", namespace)
	}

	if _, err := s.namespaceLister.Get(namespace); err == nil {
		return s.ensureRegistryPullSecret(ctx, namespace)
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("get namespace %s from cache: %w", namespace, err)
	}

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "sandbox0-manager",
			},
		},
	}
	if _, err := s.k8sClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %s: %w", namespace, err)
	}
	if err := s.ensureRegistryPullSecret(ctx, namespace); err != nil {
		return err
	}
	return nil
}

func (s *TemplateService) ensureRegistryPullSecret(ctx context.Context, namespace string) error {
	if s.registry.CredentialsFile == "" || s.registry.PullSecretName == "" || s.registry.Registry == "" {
		return nil
	}
	if s.k8sClient == nil {
		return fmt.Errorf("k8s client is required to ensure registry pull secret")
	}

	creds, err := os.ReadFile(s.registry.CredentialsFile)
	if err != nil {
		return fmt.Errorf("read registry credentials: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.registry.PullSecretName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "sandbox0-manager",
			},
		},
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{
			corev1.DockerConfigJsonKey: creds,
		},
	}

	existing, err := s.k8sClient.CoreV1().Secrets(namespace).Get(ctx, s.registry.PullSecretName, metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("get registry pull secret: %w", err)
		}
		if _, err := s.k8sClient.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create registry pull secret: %w", err)
		}
	} else {
		updated := existing.DeepCopy()
		updated.Type = corev1.SecretTypeDockerConfigJson
		if updated.Data == nil {
			updated.Data = map[string][]byte{}
		}
		updated.Data[corev1.DockerConfigJsonKey] = creds
		if _, err := s.k8sClient.CoreV1().Secrets(namespace).Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update registry pull secret: %w", err)
		}
	}

	return s.ensureDefaultServiceAccountPullSecret(ctx, namespace, s.registry.PullSecretName)
}

func (s *TemplateService) ensureDefaultServiceAccountPullSecret(ctx context.Context, namespace, secretName string) error {
	if secretName == "" {
		return nil
	}
	sa, err := s.k8sClient.CoreV1().ServiceAccounts(namespace).Get(ctx, "default", metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("get default serviceaccount: %w", err)
		}
		sa = &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "default",
				Namespace: namespace,
			},
		}
	}

	for _, ref := range sa.ImagePullSecrets {
		if ref.Name == secretName {
			if sa.ResourceVersion == "" {
				if _, err := s.k8sClient.CoreV1().ServiceAccounts(namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
					return fmt.Errorf("create default serviceaccount: %w", err)
				}
			}
			return nil
		}
	}

	sa.ImagePullSecrets = append(sa.ImagePullSecrets, corev1.LocalObjectReference{Name: secretName})
	if sa.ResourceVersion == "" {
		if _, err := s.k8sClient.CoreV1().ServiceAccounts(namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("create default serviceaccount: %w", err)
		}
		return nil
	}
	if _, err := s.k8sClient.CoreV1().ServiceAccounts(namespace).Update(ctx, sa, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update default serviceaccount: %w", err)
	}
	return nil
}
