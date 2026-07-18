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

func TestTemplateServiceRegistryHostsIncludesInternalAlias(t *testing.T) {
	service := &TemplateService{registry: config.RegistryConfig{
		PushRegistry:     "registry.example.com",
		PullRegistry:     "registry.example.com",
		InternalRegistry: "registry.sandbox0-system.svc:5000",
	}}

	assert.Equal(t, []string{
		"registry.example.com",
		"registry.sandbox0-system.svc:5000",
	}, service.RegistryHosts())
}

func (r *recordingTemplateNamespacePolicy) EnsureBaseline(_ context.Context, namespace string) error {
	r.calls = append(r.calls, namespace)
	return r.err
}

func newTemplateServiceForTests(t *testing.T) (*TemplateService, *fake.Clientset) {
	k8sClient := fake.NewSimpleClientset()
	crdClient := clientsetfake.NewSimpleClientset()
	namespaceLister, podLister, secretLister, serviceAccountLister := newTemplateServiceK8sListers(t, k8sClient)
	service := NewTemplateService(
		k8sClient,
		crdClient,
		stubTemplateLister{},
		namespaceLister,
		podLister,
		secretLister,
		serviceAccountLister,
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

func newTemplateServiceForDeleteTests(t *testing.T, k8sClient *fake.Clientset, templates ...*v1alpha1.SandboxTemplate) (*TemplateService, *clientsetfake.Clientset) {
	crdObjects := make([]runtime.Object, 0, len(templates))
	listerTemplates := make([]*v1alpha1.SandboxTemplate, 0, len(templates))
	for _, template := range templates {
		crdObjects = append(crdObjects, template.DeepCopy())
		listerTemplates = append(listerTemplates, template.DeepCopy())
	}
	crdClient := clientsetfake.NewSimpleClientset(crdObjects...)
	namespaceLister, podLister, secretLister, serviceAccountLister := newTemplateServiceK8sListers(t, k8sClient)
	service := NewTemplateService(
		k8sClient,
		crdClient,
		&listBackedTemplateLister{templates: listerTemplates},
		namespaceLister,
		podLister,
		secretLister,
		serviceAccountLister,
		nil,
		config.RegistryConfig{},
		zap.NewNop(),
	)
	return service, crdClient
}

func TestCreateTemplateEnsuresNamespaceBaseline(t *testing.T) {
	service, k8sClient := newTemplateServiceForTests(t)
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
	service, k8sClient := newTemplateServiceForTests(t)
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
	service, _ := newTemplateServiceForTests(t)
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
	service, _ := newTemplateServiceForDeleteTests(t, k8sClient, template)

	err = service.DeleteTemplate(ctx, "demo")
	require.NoError(t, err)

	_, err = k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "namespace should be deleted")
}

func TestDeleteMissingTemplateIsIdempotent(t *testing.T) {
	ctx := context.Background()
	k8sClient := fake.NewSimpleClientset()
	service, _ := newTemplateServiceForDeleteTests(t, k8sClient)

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
	service, crdClient := newTemplateServiceForDeleteTests(t, k8sClient, template)

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
	namespaceLister, podLister, secretLister, serviceAccountLister := newTemplateServiceK8sListers(t, k8sClient)
	service := NewTemplateService(
		k8sClient,
		crdClient,
		&listBackedTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
		namespaceLister,
		podLister,
		secretLister,
		serviceAccountLister,
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
	service, crdClient := newTemplateServiceForDeleteTests(t, k8sClient, target, other)

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
	pod := templatePod(namespace, "demo-pod", "demo", controller.PoolTypeActive)
	k8sClient := fake.NewSimpleClientset(
		managedNamespace(namespace),
		pod,
	)
	service, crdClient := newTemplateServiceForDeleteTests(t, k8sClient, template)

	err = service.DeleteTemplate(ctx, "demo")
	require.NoError(t, err)

	_, err = k8sClient.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "last team template should remove managed namespace")
	_, err = crdClient.Sandbox0V1alpha1().SandboxTemplates(namespace).Get(ctx, "demo", metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "last team template CRD should be deleted")
	_, err = k8sClient.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err), "last team template pod should be deleted")
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

func newTemplateServiceK8sListers(t *testing.T, client *fake.Clientset) (corelisters.NamespaceLister, corelisters.PodLister, corelisters.SecretLister, corelisters.ServiceAccountLister) {
	t.Helper()
	ctx := context.Background()
	namespaceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	secretIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	serviceAccountIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})

	namespaces, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	for i := range namespaces.Items {
		require.NoError(t, namespaceIndexer.Add(namespaces.Items[i].DeepCopy()))
	}
	pods, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	for i := range pods.Items {
		require.NoError(t, podIndexer.Add(pods.Items[i].DeepCopy()))
	}
	secrets, err := client.CoreV1().Secrets(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	for i := range secrets.Items {
		require.NoError(t, secretIndexer.Add(secrets.Items[i].DeepCopy()))
	}
	serviceAccounts, err := client.CoreV1().ServiceAccounts(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	for i := range serviceAccounts.Items {
		require.NoError(t, serviceAccountIndexer.Add(serviceAccounts.Items[i].DeepCopy()))
	}

	return corelisters.NewNamespaceLister(namespaceIndexer),
		corelisters.NewPodLister(podIndexer),
		corelisters.NewSecretLister(secretIndexer),
		corelisters.NewServiceAccountLister(serviceAccountIndexer)
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
