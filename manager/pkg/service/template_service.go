package service

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	clientset "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/namespacepolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

const (
	managerNamespaceLabelKey   = "app.kubernetes.io/managed-by"
	managerNamespaceLabelValue = "sandbox0-manager"
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
	namespacePolicy namespacepolicy.TemplateNamespaceReconciler
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

// SetNamespacePolicyReconciler installs the manager-owned template namespace baseline reconciler.
func (s *TemplateService) SetNamespacePolicyReconciler(reconciler namespacepolicy.TemplateNamespaceReconciler) {
	s.namespacePolicy = reconciler
}

// RegistryHosts returns registry hosts reserved for platform-scoped private images.
func (s *TemplateService) RegistryHosts() []string {
	hosts := make([]string, 0, 2)
	seen := make(map[string]struct{}, 2)
	for _, value := range []string{s.registry.PushRegistry, s.registry.PullRegistry} {
		host := strings.TrimSpace(value)
		if host == "" {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		hosts = append(hosts, host)
	}
	return hosts
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
	if s.namespacePolicy != nil {
		if err := s.namespacePolicy.EnsureBaseline(ctx, namespace); err != nil {
			return nil, fmt.Errorf("ensure template namespace baseline: %w", err)
		}
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

	deleteNamespace, err := s.shouldDeleteNamespaceForTemplate(ctx, existing)
	if err != nil {
		return fmt.Errorf("resolve template namespace cleanup: %w", err)
	}
	if deleteNamespace {
		// Request namespace deletion first, then continue deleting the template CR
		// and matching pods so terminating namespaces do not keep stale template
		// objects around while the single-cluster reconciler is still looping.
		if err := s.deleteTemplateNamespace(ctx, existing.Namespace); err != nil {
			return err
		}
	}

	err = s.crdClient.Sandbox0V1alpha1().SandboxTemplates(existing.Namespace).Delete(ctx, id, metav1.DeleteOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("delete template: %w", err)
		}
	}

	if err := s.deleteTemplatePods(ctx, existing.Namespace, id); err != nil {
		return err
	}

	return nil
}

func (s *TemplateService) shouldDeleteNamespaceForTemplate(ctx context.Context, template *v1alpha1.SandboxTemplate) (bool, error) {
	if template == nil || template.Namespace == "" || s.k8sClient == nil || s.crdClient == nil {
		return false, nil
	}

	managed, err := s.isManagedTemplateNamespace(ctx, template.Namespace)
	if err != nil {
		return false, err
	}
	if !managed {
		return false, nil
	}

	hasOtherTemplates, err := s.namespaceHasOtherTemplates(ctx, template.Namespace, template.Name)
	if err != nil {
		return false, err
	}
	return !hasOtherTemplates, nil
}

func (s *TemplateService) isManagedTemplateNamespace(ctx context.Context, namespace string) (bool, error) {
	ns, err := s.k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get namespace %s: %w", namespace, err)
	}
	if ns.Labels[managerNamespaceLabelKey] != managerNamespaceLabelValue {
		s.logger.Info("Skipping template namespace cleanup for unmanaged namespace",
			zap.String("namespace", namespace),
		)
		return false, nil
	}
	return true, nil
}

func (s *TemplateService) namespaceHasOtherTemplates(ctx context.Context, namespace, deletingName string) (bool, error) {
	templates, err := s.crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("list templates in namespace %s: %w", namespace, err)
	}
	for _, template := range templates.Items {
		if template.Name != deletingName {
			return true, nil
		}
	}
	return false, nil
}

func (s *TemplateService) deleteTemplateNamespace(ctx context.Context, namespace string) error {
	if namespace == "" || s.k8sClient == nil {
		return nil
	}
	if err := s.k8sClient.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{}); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete template namespace %s: %w", namespace, err)
	}
	s.logger.Info("Template namespace deletion requested",
		zap.String("namespace", namespace),
	)
	return nil
}

func (s *TemplateService) deleteTemplatePods(ctx context.Context, namespace, templateID string) error {
	if namespace == "" || templateID == "" || s.k8sClient == nil {
		return nil
	}
	selector := labels.Set{controller.LabelTemplateID: templateID}.AsSelector().String()
	pods, err := s.k8sClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("list template pods in namespace %s: %w", namespace, err)
	}

	for _, pod := range pods.Items {
		poolType := pod.Labels[controller.LabelPoolType]
		if poolType != controller.PoolTypeIdle && poolType != controller.PoolTypeActive {
			continue
		}
		deleteOptions := metav1.DeleteOptions{}
		if poolType == controller.PoolTypeIdle {
			gracePeriodSeconds := int64(0)
			uid := pod.UID
			deleteOptions.GracePeriodSeconds = &gracePeriodSeconds
			deleteOptions.Preconditions = &metav1.Preconditions{UID: &uid}
		}
		if err := s.k8sClient.CoreV1().Pods(namespace).Delete(ctx, pod.Name, deleteOptions); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("delete template pod %s/%s: %w", namespace, pod.Name, err)
		}
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
		if err := s.ensureRegistryPullSecret(ctx, namespace); err != nil {
			return err
		}
		return controller.EnsureNetdMITMCASecret(ctx, s.k8sClient, namespace)
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("get namespace %s from cache: %w", namespace, err)
	}

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
			Labels: map[string]string{
				managerNamespaceLabelKey: managerNamespaceLabelValue,
			},
		},
	}
	if _, err := s.k8sClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %s: %w", namespace, err)
	}
	if err := s.ensureRegistryPullSecret(ctx, namespace); err != nil {
		return err
	}
	return controller.EnsureNetdMITMCASecret(ctx, s.k8sClient, namespace)
}

func (s *TemplateService) ensureRegistryPullSecret(ctx context.Context, namespace string) error {
	if s.registry.PullCredentialsFile == "" || s.registry.PullSecretName == "" || s.registry.PullRegistry == "" {
		return nil
	}
	if s.k8sClient == nil {
		return fmt.Errorf("k8s client is required to ensure registry pull secret")
	}

	creds, err := os.ReadFile(s.registry.PullCredentialsFile)
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
