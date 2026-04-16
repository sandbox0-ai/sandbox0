package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	clientsetfake "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned/fake"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type stubTemplateLister struct{}

func (stubTemplateLister) List() ([]*v1alpha1.SandboxTemplate, error) { return nil, nil }
func (stubTemplateLister) Get(string, string) (*v1alpha1.SandboxTemplate, error) {
	return nil, errors.New("not implemented")
}

type recordingTemplateNamespacePolicy struct {
	calls []string
	err   error
}

func (r *recordingTemplateNamespacePolicy) EnsureBaseline(_ context.Context, namespace string) error {
	r.calls = append(r.calls, namespace)
	return r.err
}

func newTemplateServiceForTests() (*TemplateService, *fake.Clientset) {
	k8sClient := fake.NewSimpleClientset()
	crdClient := clientsetfake.NewSimpleClientset()
	namespaceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	service := NewTemplateService(
		k8sClient,
		crdClient,
		stubTemplateLister{},
		corelisters.NewNamespaceLister(namespaceIndexer),
		nil,
		config.RegistryConfig{},
		zap.NewNop(),
	)
	return service, k8sClient
}

type listBackedTemplateLister struct {
	templates []*v1alpha1.SandboxTemplate
}

func (l *listBackedTemplateLister) List() ([]*v1alpha1.SandboxTemplate, error) {
	return l.templates, nil
}

func (l *listBackedTemplateLister) Get(namespace, name string) (*v1alpha1.SandboxTemplate, error) {
	for _, template := range l.templates {
		if template.Namespace == namespace && template.Name == name {
			return template, nil
		}
	}
	return nil, apierrors.NewNotFound(v1alpha1.Resource("sandboxtemplate"), name)
}

func newTemplateServiceForDeleteTests(k8sClient *fake.Clientset, templates ...*v1alpha1.SandboxTemplate) (*TemplateService, *clientsetfake.Clientset) {
	crdObjects := make([]runtime.Object, 0, len(templates))
	listerTemplates := make([]*v1alpha1.SandboxTemplate, 0, len(templates))
	for _, template := range templates {
		crdObjects = append(crdObjects, template.DeepCopy())
		listerTemplates = append(listerTemplates, template.DeepCopy())
	}
	crdClient := clientsetfake.NewSimpleClientset(crdObjects...)
	namespaceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	service := NewTemplateService(
		k8sClient,
		crdClient,
		&listBackedTemplateLister{templates: listerTemplates},
		corelisters.NewNamespaceLister(namespaceIndexer),
		nil,
		config.RegistryConfig{},
		zap.NewNop(),
	)
	return service, crdClient
}

func TestCreateTemplateEnsuresNamespaceBaseline(t *testing.T) {
	service, k8sClient := newTemplateServiceForTests()
	reconciler := &recordingTemplateNamespacePolicy{}
	service.SetNamespacePolicyReconciler(reconciler)

	template := &v1alpha1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}
	created, err := service.CreateTemplate(context.Background(), template)
	require.NoError(t, err)

	expectedNamespace, err := naming.TemplateNamespaceForBuiltin("demo")
	require.NoError(t, err)
	assert.Equal(t, expectedNamespace, created.Namespace)
	assert.Equal(t, []string{expectedNamespace}, reconciler.calls)
	_, err = k8sClient.CoreV1().Namespaces().Get(context.Background(), expectedNamespace, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCreateTeamTemplateEnsuresNamespaceBaseline(t *testing.T) {
	service, k8sClient := newTemplateServiceForTests()
	reconciler := &recordingTemplateNamespacePolicy{}
	service.SetNamespacePolicyReconciler(reconciler)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo",
			Labels: map[string]string{
				"sandbox0.ai/template-scope": naming.ScopeTeam,
			},
			Annotations: map[string]string{
				"sandbox0.ai/template-team-id": "team-123",
			},
		},
	}
	created, err := service.CreateTemplate(context.Background(), template)
	require.NoError(t, err)

	expectedNamespace, err := naming.TemplateNamespaceForTeam("team-123")
	require.NoError(t, err)
	assert.Equal(t, expectedNamespace, created.Namespace)
	assert.Equal(t, []string{expectedNamespace}, reconciler.calls)
	_, err = k8sClient.CoreV1().Namespaces().Get(context.Background(), expectedNamespace, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCreateTemplateFailsWhenNamespaceBaselineFails(t *testing.T) {
	service, _ := newTemplateServiceForTests()
	reconciler := &recordingTemplateNamespacePolicy{err: errors.New("boom")}
	service.SetNamespacePolicyReconciler(reconciler)

	_, err := service.CreateTemplate(context.Background(), &v1alpha1.SandboxTemplate{ObjectMeta: metav1.ObjectMeta{Name: "demo"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ensure template namespace baseline")
	assert.Len(t, reconciler.calls, 1)
}

func TestDeletePublicTemplateDeletesManagedNamespace(t *testing.T) {
	ctx := context.Background()
	namespace, err := naming.TemplateNamespaceForBuiltin("demo")
	require.NoError(t, err)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: namespace,
		},
	}
	k8sClient := fake.NewSimpleClientset(managedNamespace(namespace))
	service, _ := newTemplateServiceForDeleteTests(k8sClient, template)

	err = service.DeleteTemplate(ctx, "demo")
	require.NoError(t, err)

	_, err = k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "namespace should be deleted")
}

func TestDeleteMissingTemplateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	k8sClient := fake.NewSimpleClientset()
	service, _ := newTemplateServiceForDeleteTests(k8sClient)

	err := service.DeleteTemplate(ctx, "missing")
	require.NoError(t, err)
}

func TestDeleteTemplateKeepsUnmanagedNamespaceAndDeletesTemplatePods(t *testing.T) {
	ctx := context.Background()
	namespace, err := naming.TemplateNamespaceForBuiltin("demo")
	require.NoError(t, err)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: namespace,
		},
	}
	targetPod := templatePod(namespace, "demo-pod", "demo", controller.PoolTypeActive)
	otherPod := templatePod(namespace, "other-pod", "other", controller.PoolTypeActive)
	k8sClient := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}},
		targetPod,
		otherPod,
	)
	service, crdClient := newTemplateServiceForDeleteTests(k8sClient, template)

	err = service.DeleteTemplate(ctx, "demo")
	require.NoError(t, err)

	_, err = k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	require.NoError(t, err, "unmanaged namespace should be preserved")
	_, err = crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace).Get(ctx, "demo", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "template CRD should be deleted")
	_, err = k8sClient.CoreV1().Pods(namespace).Get(ctx, "demo-pod", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "target template pod should be deleted")
	_, err = k8sClient.CoreV1().Pods(namespace).Get(ctx, "other-pod", metav1.GetOptions{})
	require.NoError(t, err, "other template pod should be preserved")
}

func TestDeleteTemplateCleansPodsWhenCRDAlreadyGone(t *testing.T) {
	ctx := context.Background()
	namespace, err := naming.TemplateNamespaceForBuiltin("demo")
	require.NoError(t, err)

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: namespace,
		},
	}
	targetPod := templatePod(namespace, "demo-pod", "demo", controller.PoolTypeActive)
	k8sClient := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}},
		targetPod,
	)
	crdClient := clientsetfake.NewSimpleClientset()
	namespaceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	service := NewTemplateService(
		k8sClient,
		crdClient,
		&listBackedTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
		corelisters.NewNamespaceLister(namespaceIndexer),
		nil,
		config.RegistryConfig{},
		zap.NewNop(),
	)

	err = service.DeleteTemplate(ctx, "demo")
	require.NoError(t, err)

	_, err = k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	require.NoError(t, err, "unmanaged namespace should be preserved")
	_, err = k8sClient.CoreV1().Pods(namespace).Get(ctx, "demo-pod", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "target template pod should still be deleted")
}

func TestDeleteTeamTemplateKeepsSharedNamespaceAndDeletesTemplatePods(t *testing.T) {
	ctx := context.Background()
	namespace, err := naming.TemplateNamespaceForTeam("team-123")
	require.NoError(t, err)

	target := teamTemplate(namespace, "demo", "team-123")
	other := teamTemplate(namespace, "other", "team-123")
	targetPod := templatePod(namespace, "demo-pod", "demo", controller.PoolTypeActive)
	otherPod := templatePod(namespace, "other-pod", "other", controller.PoolTypeActive)
	k8sClient := fake.NewSimpleClientset(
		managedNamespace(namespace),
		targetPod,
		otherPod,
	)
	service, crdClient := newTemplateServiceForDeleteTests(k8sClient, target, other)

	err = service.DeleteTemplate(ctx, "demo")
	require.NoError(t, err)

	_, err = k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	require.NoError(t, err, "shared team namespace should be preserved")
	_, err = crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace).Get(ctx, "demo", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "target template CRD should be deleted")
	_, err = crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace).Get(ctx, "other", metav1.GetOptions{})
	require.NoError(t, err, "other team template should be preserved")
	_, err = k8sClient.CoreV1().Pods(namespace).Get(ctx, "demo-pod", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "target template pod should be deleted")
	_, err = k8sClient.CoreV1().Pods(namespace).Get(ctx, "other-pod", metav1.GetOptions{})
	require.NoError(t, err, "other template pod should be preserved")
}

func TestDeleteLastTeamTemplateDeletesManagedNamespace(t *testing.T) {
	ctx := context.Background()
	namespace, err := naming.TemplateNamespaceForTeam("team-123")
	require.NoError(t, err)

	template := teamTemplate(namespace, "demo", "team-123")
	k8sClient := fake.NewSimpleClientset(
		managedNamespace(namespace),
		templatePod(namespace, "demo-pod", "demo", controller.PoolTypeActive),
	)
	service, _ := newTemplateServiceForDeleteTests(k8sClient, template)

	err = service.DeleteTemplate(ctx, "demo")
	require.NoError(t, err)

	_, err = k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "last team template should remove managed namespace")
}

func managedNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				managerNamespaceLabelKey: managerNamespaceLabelValue,
			},
		},
	}
}

func teamTemplate(namespace, name, teamID string) *v1alpha1.SandboxTemplate {
	return &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"sandbox0.ai/template-scope": naming.ScopeTeam,
			},
			Annotations: map[string]string{
				"sandbox0.ai/template-team-id": teamID,
			},
		},
	}
}

func templatePod(namespace, name, templateID, poolType string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				controller.LabelTemplateID: templateID,
				controller.LabelPoolType:   poolType,
				controller.LabelSandboxID:  name,
			},
		},
	}
}
