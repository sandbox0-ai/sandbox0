package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	clientsetfake "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned/fake"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
