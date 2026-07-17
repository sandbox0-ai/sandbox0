package namespacepolicy

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	networkinglisters "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
)

func TestEnsureBaselineCreatesPolicies(t *testing.T) {
	client := fake.NewSimpleClientset()
	reconciler, err := NewReconciler(client, newNetworkPolicyLister(t), Config{SystemNamespace: "sandbox0-system", ProcdPort: 49983}, zap.NewNop())
	require.NoError(t, err)

	require.NoError(t, reconciler.EnsureBaseline(context.Background(), "tpl-demo"))

	deny, err := client.NetworkingV1().NetworkPolicies("tpl-demo").Get(context.Background(), policyDenyIngressName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, sandboxPodSelector(), deny.Spec.PodSelector)
	assert.Equal(t, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}, deny.Spec.PolicyTypes)
	assert.Empty(t, deny.Spec.Ingress)

	allow, err := client.NetworkingV1().NetworkPolicies("tpl-demo").Get(context.Background(), policyAllowSystemName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, allow.Spec.Ingress, 4)
	assert.Equal(t, "sandbox0-system", allow.Spec.Ingress[0].From[0].NamespaceSelector.MatchLabels[metadataNamespaceLabel])
	assert.Equal(t, internalauth.ServiceManager, allow.Spec.Ingress[0].From[0].PodSelector.MatchLabels[appNameLabelKey])
	require.Len(t, allow.Spec.Ingress[0].Ports, 1)
	assert.Equal(t, corev1.ProtocolTCP, *allow.Spec.Ingress[0].Ports[0].Protocol)
	assert.Equal(t, int32(49983), allow.Spec.Ingress[0].Ports[0].Port.IntVal)
	assert.Equal(t, internalauth.ServiceSSHGateway, allow.Spec.Ingress[1].From[0].PodSelector.MatchLabels[appNameLabelKey])
	require.Len(t, allow.Spec.Ingress[1].Ports, 1)
	assert.Equal(t, corev1.ProtocolTCP, *allow.Spec.Ingress[1].Ports[0].Protocol)
	assert.Equal(t, int32(49983), allow.Spec.Ingress[1].Ports[0].Port.IntVal)
	assert.Equal(t, internalauth.ServiceCtld, allow.Spec.Ingress[2].From[0].PodSelector.MatchLabels[appNameLabelKey])
	require.Len(t, allow.Spec.Ingress[2].Ports, 1)
	assert.Equal(t, corev1.ProtocolTCP, *allow.Spec.Ingress[2].Ports[0].Protocol)
	assert.Equal(t, int32(49983), allow.Spec.Ingress[2].Ports[0].Port.IntVal)
	assert.Equal(t, internalauth.ServiceClusterGateway, allow.Spec.Ingress[3].From[0].PodSelector.MatchLabels[appNameLabelKey])
	assert.Empty(t, allow.Spec.Ingress[3].Ports)
}

func TestEnsureBaselineRepairsDrift(t *testing.T) {
	drifted := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      policyAllowSystemName,
			Namespace: "tpl-demo",
			Labels:    map[string]string{"stale": "true"},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "wrong"},
			},
		},
	}
	client := fake.NewSimpleClientset(drifted)

	reconciler, err := NewReconciler(client, newNetworkPolicyLister(t, drifted), Config{SystemNamespace: "sandbox0-system", ProcdPort: 49983}, zap.NewNop())
	require.NoError(t, err)

	require.NoError(t, reconciler.EnsureBaseline(context.Background(), "tpl-demo"))

	allow, err := client.NetworkingV1().NetworkPolicies("tpl-demo").Get(context.Background(), policyAllowSystemName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, sandboxPodSelector(), allow.Spec.PodSelector)
	assert.Equal(t, managedByLabelValue, allow.Labels[managedByLabelKey])
	assert.Equal(t, componentLabelValue, allow.Labels[componentLabelKey])

	deny, err := client.NetworkingV1().NetworkPolicies("tpl-demo").Get(context.Background(), policyDenyIngressName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, sandboxIDLabelKey, deny.Spec.PodSelector.MatchExpressions[0].Key)
}

func TestEnsureBaselineNoopsWhenCacheMatches(t *testing.T) {
	client := fake.NewSimpleClientset()
	policies := desiredPoliciesForTest(t, "tpl-demo")
	reconciler, err := NewReconciler(client, newNetworkPolicyLister(t, policies...), Config{SystemNamespace: "sandbox0-system", ProcdPort: 49983}, zap.NewNop())
	require.NoError(t, err)

	require.NoError(t, reconciler.EnsureBaseline(context.Background(), "tpl-demo"))
	assert.Empty(t, client.Actions())
}

func desiredPoliciesForTest(t *testing.T, namespace string) []*networkingv1.NetworkPolicy {
	t.Helper()
	reconciler, err := NewReconciler(fake.NewSimpleClientset(), nil, Config{SystemNamespace: "sandbox0-system", ProcdPort: 49983}, zap.NewNop())
	require.NoError(t, err)
	return reconciler.desiredPolicies(namespace)
}

func newNetworkPolicyLister(t *testing.T, policies ...*networkingv1.NetworkPolicy) networkinglisters.NetworkPolicyLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, policy := range policies {
		require.NoError(t, indexer.Add(policy))
	}
	return networkinglisters.NewNetworkPolicyLister(indexer)
}
