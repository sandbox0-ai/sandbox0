package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

type memoryBindingStore struct {
	records map[string]*egressauth.BindingRecord
}

func newMemoryBindingStore() *memoryBindingStore {
	return &memoryBindingStore{records: make(map[string]*egressauth.BindingRecord)}
}

func (s *memoryBindingStore) GetBindings(_ context.Context, clusterID, sandboxID string) (*egressauth.BindingRecord, error) {
	return cloneBindingRecord(s.records[s.bindingKey(clusterID, sandboxID)]), nil
}

func (s *memoryBindingStore) UpsertBindings(_ context.Context, record *egressauth.BindingRecord) error {
	if record == nil {
		return nil
	}
	s.records[s.bindingKey(record.ClusterID, record.SandboxID)] = cloneBindingRecord(record)
	return nil
}

func (s *memoryBindingStore) DeleteBindings(_ context.Context, clusterID, sandboxID string) error {
	delete(s.records, s.bindingKey(clusterID, sandboxID))
	return nil
}

func (s *memoryBindingStore) bindingKey(clusterID, sandboxID string) string {
	return clusterID + "/" + sandboxID
}

type assertingNetworkProvider struct {
	applyFunc func(network.SandboxPolicyInput)
}

func (p *assertingNetworkProvider) Name() string { return "test" }

func (p *assertingNetworkProvider) EnsureBaseline(context.Context, string) error {
	return nil
}

func (p *assertingNetworkProvider) ApplySandboxPolicy(_ context.Context, input network.SandboxPolicyInput) error {
	if p.applyFunc != nil {
		p.applyFunc(input)
	}
	return nil
}

func (p *assertingNetworkProvider) RemoveSandboxPolicy(context.Context, string, string) error {
	return nil
}

func newSandboxServiceForNetworkTests(
	t *testing.T,
	pod *corev1.Pod,
	store egressauth.BindingStore,
	provider network.Provider,
) (*SandboxService, *fake.Clientset, cache.Indexer) {
	t.Helper()

	client := fake.NewSimpleClientset(pod.DeepCopy())
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	require.NoError(t, indexer.Add(pod.DeepCopy()))

	svc := &SandboxService{
		k8sClient:            client,
		podLister:            corelisters.NewPodLister(indexer),
		NetworkPolicyService: NewNetworkPolicyService(zap.NewNop()),
		networkProvider:      provider,
		credentialStore:      store,
		clock:                systemTime{},
		logger:               zap.NewNop(),
	}
	return svc, client, indexer
}

func testSandboxNetworkPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1",
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID: "team-1",
				controller.AnnotationConfig: "{}",
			},
		},
	}
}

func testCredentialPolicy(ref, authHeader string) *v1alpha1.TplSandboxNetworkPolicy {
	return &v1alpha1.TplSandboxNetworkPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			Rules: []v1alpha1.EgressCredentialRule{{
				Name:          "example-api",
				CredentialRef: ref,
				Protocol:      v1alpha1.EgressAuthProtocolHTTPS,
				Domains:       []string{"api.example.com"},
			}},
		},
		Credentials: &v1alpha1.NetworkCredentialsSpec{
			Bindings: []v1alpha1.CredentialBinding{{
				Ref:      ref,
				Provider: "static",
				Headers:  map[string]string{"Authorization": authHeader},
			}},
		},
	}
}

func TestUpdateNetworkPolicyRollsBackBindingsWhenPodUpdateFails(t *testing.T) {
	ctx := context.Background()
	pod := testSandboxNetworkPod()
	store := newMemoryBindingStore()
	require.NoError(t, store.UpsertBindings(ctx, &egressauth.BindingRecord{
		ClusterID: naming.DefaultClusterID,
		SandboxID: pod.Name,
		TeamID:    "team-1",
		Bindings: []egressauth.CredentialBinding{{
			Ref:      "existing-ref",
			Provider: "static",
			Headers:  map[string]string{"Authorization": "Bearer existing"},
		}},
	}))

	svc, client, _ := newSandboxServiceForNetworkTests(t, pod, store, network.NewNoopProvider())
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("boom")
	})

	_, err := svc.UpdateNetworkPolicy(ctx, pod.Name, testCredentialPolicy("new-ref", "Bearer new"))
	require.Error(t, err)

	record, err := store.GetBindings(ctx, naming.DefaultClusterID, pod.Name)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Len(t, record.Bindings, 1)
	assert.Equal(t, "existing-ref", record.Bindings[0].Ref)
}

func TestUpdateSandboxRollsBackBindingsWhenPodUpdateFails(t *testing.T) {
	ctx := context.Background()
	pod := testSandboxNetworkPod()
	store := newMemoryBindingStore()
	require.NoError(t, store.UpsertBindings(ctx, &egressauth.BindingRecord{
		ClusterID: naming.DefaultClusterID,
		SandboxID: pod.Name,
		TeamID:    "team-1",
		Bindings: []egressauth.CredentialBinding{{
			Ref:      "existing-ref",
			Provider: "static",
			Headers:  map[string]string{"Authorization": "Bearer existing"},
		}},
	}))

	svc, client, _ := newSandboxServiceForNetworkTests(t, pod, store, network.NewNoopProvider())
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("boom")
	})

	_, err := svc.UpdateSandbox(ctx, pod.Name, &SandboxUpdateConfig{
		Network: testCredentialPolicy("new-ref", "Bearer new"),
	})
	require.Error(t, err)

	record, err := store.GetBindings(ctx, naming.DefaultClusterID, pod.Name)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Len(t, record.Bindings, 1)
	assert.Equal(t, "existing-ref", record.Bindings[0].Ref)
}

func TestUpdateNetworkPolicyStoresBindingsOutsidePodConfig(t *testing.T) {
	ctx := context.Background()
	pod := testSandboxNetworkPod()
	store := newMemoryBindingStore()
	provider := &assertingNetworkProvider{
		applyFunc: func(input network.SandboxPolicyInput) {
			record, err := store.GetBindings(ctx, naming.DefaultClusterID, pod.Name)
			require.NoError(t, err)
			require.NotNil(t, record)
			require.Len(t, record.Bindings, 1)
			assert.Equal(t, "example-ref", record.Bindings[0].Ref)
			require.NotNil(t, input.NetworkPolicy)
			require.NotNil(t, input.NetworkPolicy.Egress)
			require.Len(t, input.NetworkPolicy.Egress.Rules, 1)
			assert.Equal(t, "example-ref", input.NetworkPolicy.Egress.Rules[0].CredentialRef)
		},
	}

	svc, client, indexer := newSandboxServiceForNetworkTests(t, pod, store, provider)

	updated, err := svc.UpdateNetworkPolicy(ctx, pod.Name, testCredentialPolicy("example-ref", "Bearer stored"))
	require.NoError(t, err)
	require.NotNil(t, updated)

	storedPod, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, indexer.Update(storedPod.DeepCopy()))
	svc.podLister = corelisters.NewPodLister(indexer)

	var cfg SandboxConfig
	require.NoError(t, json.Unmarshal([]byte(storedPod.Annotations[controller.AnnotationConfig]), &cfg))
	require.NotNil(t, cfg.Network)
	assert.Nil(t, cfg.Network.Credentials)

	effective, err := svc.GetNetworkPolicy(ctx, pod.Name)
	require.NoError(t, err)
	require.NotNil(t, effective.Credentials)
	require.Len(t, effective.Credentials.Bindings, 1)
	assert.Equal(t, "example-ref", effective.Credentials.Bindings[0].Ref)
}
