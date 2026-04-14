package rbac

import (
	"context"
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileManagerRBACIncludesNetworkPolicies(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rbacv1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra).Build()
	reconciler := NewReconciler(&common.ResourceManager{
		Client: client,
		Scheme: scheme,
	})

	require.NoError(t, reconciler.ReconcileManagerRBAC(context.Background(), infra))

	role := &rbacv1.ClusterRole{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: "demo-manager"}, role))

	found := false
	for _, rule := range role.Rules {
		if !contains(rule.APIGroups, "networking.k8s.io") {
			continue
		}
		if !contains(rule.Resources, "networkpolicies") {
			continue
		}
		assert.ElementsMatch(t, []string{"get", "list", "watch", "create", "update", "patch", "delete"}, rule.Verbs)
		found = true
	}
	assert.True(t, found, "expected manager cluster role to include networkpolicies permissions")
}

func TestReconcileManagerRBACIncludesPodResize(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rbacv1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra).Build()
	reconciler := NewReconciler(&common.ResourceManager{
		Client: client,
		Scheme: scheme,
	})

	require.NoError(t, reconciler.ReconcileManagerRBAC(context.Background(), infra))

	role := &rbacv1.ClusterRole{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: "demo-manager"}, role))

	found := false
	for _, rule := range role.Rules {
		if !contains(rule.APIGroups, "") {
			continue
		}
		if !contains(rule.Resources, "pods/resize") {
			continue
		}
		assert.ElementsMatch(t, []string{"get", "list", "watch", "create", "update", "patch", "delete"}, rule.Verbs)
		found = true
	}
	assert.True(t, found, "expected manager cluster role to include pods/resize permissions")
}

func TestReconcileManagerRBACIncludesPodLogsReadPermission(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rbacv1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra).Build()
	reconciler := NewReconciler(&common.ResourceManager{
		Client: client,
		Scheme: scheme,
	})

	require.NoError(t, reconciler.ReconcileManagerRBAC(context.Background(), infra))

	role := &rbacv1.ClusterRole{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: "demo-manager"}, role))

	found := false
	for _, rule := range role.Rules {
		if !contains(rule.APIGroups, "") {
			continue
		}
		if !contains(rule.Resources, "pods/log") {
			continue
		}
		assert.ElementsMatch(t, []string{"get"}, rule.Verbs)
		found = true
	}
	assert.True(t, found, "expected manager cluster role to include pods/log read permission")
}

func TestReconcileCtldRBACIncludesPodReadPermissions(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rbacv1.AddToScheme(scheme))
	require.NoError(t, infrav1alpha1.AddToScheme(scheme))

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra).Build()
	reconciler := NewReconciler(&common.ResourceManager{Client: client, Scheme: scheme})

	require.NoError(t, reconciler.ReconcileCtldRBAC(context.Background(), infra))

	sa := &corev1.ServiceAccount{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: "demo-ctld", Namespace: "sandbox0-system"}, sa))

	role := &rbacv1.ClusterRole{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: "demo-ctld"}, role))

	found := false
	for _, rule := range role.Rules {
		if !contains(rule.Resources, "pods") {
			continue
		}
		assert.ElementsMatch(t, []string{"get", "list", "watch"}, rule.Verbs)
		found = true
	}
	assert.True(t, found, "expected ctld cluster role to include pod read permissions")
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
