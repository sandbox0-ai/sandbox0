package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
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
	records        map[string]*egressauth.BindingRecord
	sourcesByRef   map[string]*egressauth.CredentialSource
	sourceVersions map[string]*egressauth.CredentialSourceVersion
}

func newMemoryBindingStore() *memoryBindingStore {
	return &memoryBindingStore{
		records:        make(map[string]*egressauth.BindingRecord),
		sourcesByRef:   make(map[string]*egressauth.CredentialSource),
		sourceVersions: make(map[string]*egressauth.CredentialSourceVersion),
	}
}

func (s *memoryBindingStore) GetBindings(_ context.Context, teamID, sandboxID string) (*egressauth.BindingRecord, error) {
	return cloneBindingRecord(s.records[s.bindingKey(teamID, sandboxID)]), nil
}

func (s *memoryBindingStore) UpsertBindings(_ context.Context, record *egressauth.BindingRecord) error {
	if record == nil {
		return nil
	}
	s.records[s.bindingKey(record.TeamID, record.SandboxID)] = cloneBindingRecord(record)
	return nil
}

func (s *memoryBindingStore) DeleteBindings(_ context.Context, teamID, sandboxID string) error {
	delete(s.records, s.bindingKey(teamID, sandboxID))
	return nil
}

func (s *memoryBindingStore) GetSourceByRef(_ context.Context, teamID, ref string) (*egressauth.CredentialSource, error) {
	return cloneCredentialSource(s.sourcesByRef[s.sourceRefKey(teamID, ref)]), nil
}

func (s *memoryBindingStore) GetSourceVersion(_ context.Context, sourceID, version int64) (*egressauth.CredentialSourceVersion, error) {
	return cloneCredentialSourceVersion(s.sourceVersions[s.sourceVersionKey(sourceID, version)]), nil
}

func (s *memoryBindingStore) bindingKey(teamID, sandboxID string) string {
	return teamID + "/" + sandboxID
}

func (s *memoryBindingStore) sourceRefKey(teamID, ref string) string {
	return teamID + "/" + ref
}

func (s *memoryBindingStore) sourceVersionKey(sourceID, version int64) string {
	return fmt.Sprintf("%d/%d", sourceID, version)
}

func (s *memoryBindingStore) addStaticHeadersSource(teamID, ref string, sourceID, version int64, values map[string]string) {
	s.sourcesByRef[s.sourceRefKey(teamID, ref)] = &egressauth.CredentialSource{
		ID:             sourceID,
		TeamID:         teamID,
		Name:           ref,
		ResolverKind:   "static_headers",
		CurrentVersion: version,
		Status:         "active",
	}
	s.sourceVersions[s.sourceVersionKey(sourceID, version)] = &egressauth.CredentialSourceVersion{
		SourceID:     sourceID,
		Version:      version,
		ResolverKind: "static_headers",
		Spec: egressauth.CredentialSourceSecretSpec{
			StaticHeaders: &egressauth.StaticHeadersSourceSpec{
				Values: cloneStringMap(values),
			},
		},
	}
}

type assertingNetworkProvider struct {
	applyFunc  func(network.SandboxPolicyInput)
	applyErr   error
	removeFunc func(namespace, sandboxID string)
	removeErr  error
}

func (p *assertingNetworkProvider) Name() string { return "test" }

func (p *assertingNetworkProvider) EnsureBaseline(context.Context, string) error {
	return nil
}

func (p *assertingNetworkProvider) ApplySandboxPolicy(_ context.Context, input network.SandboxPolicyInput) error {
	if p.applyFunc != nil {
		p.applyFunc(input)
	}
	return p.applyErr
}

func (p *assertingNetworkProvider) RemoveSandboxPolicy(_ context.Context, namespace, sandboxID string) error {
	if p.removeFunc != nil {
		p.removeFunc(namespace, sandboxID)
	}
	return p.removeErr
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

func testCredentialPolicy(ref string) *v1alpha1.SandboxNetworkPolicy {
	return &v1alpha1.SandboxNetworkPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			CredentialRules: []v1alpha1.EgressCredentialRule{{
				Name:          "example-api",
				CredentialRef: ref,
				Protocol:      v1alpha1.EgressAuthProtocolHTTPS,
				Domains:       []string{"api.example.com"},
			}},
		},
	}
}

func testCredentialBindings(ref, authHeader string) []v1alpha1.CredentialBinding {
	return []v1alpha1.CredentialBinding{{
		Ref:       ref,
		SourceRef: ref,
		Projection: v1alpha1.ProjectionSpec{
			Type: v1alpha1.CredentialProjectionTypeHTTPHeaders,
			HTTPHeaders: &v1alpha1.HTTPHeadersProjection{
				Headers: []v1alpha1.ProjectedHeader{{
					Name:          "Authorization",
					ValueTemplate: authHeader,
				}},
			},
		},
	}}
}

func testNetworkPolicy(ref, authHeader string) *v1alpha1.SandboxNetworkPolicy {
	return &v1alpha1.SandboxNetworkPolicy{
		Mode:               v1alpha1.NetworkModeBlockAll,
		Egress:             testCredentialPolicy(ref).Egress,
		CredentialBindings: testCredentialBindings(ref, authHeader),
	}
}

func TestUpdateNetworkPolicyRollsBackBindingsWhenPodUpdateFails(t *testing.T) {
	ctx := context.Background()
	pod := testSandboxNetworkPod()
	store := newMemoryBindingStore()
	store.addStaticHeadersSource("team-1", "new-ref", 2, 1, map[string]string{"token": "new"})
	require.NoError(t, store.UpsertBindings(ctx, &egressauth.BindingRecord{
		SandboxID: pod.Name,
		TeamID:    "team-1",
		Bindings: []egressauth.CredentialBinding{{
			Ref:           "existing-ref",
			SourceRef:     "existing-ref",
			SourceID:      1,
			SourceVersion: 1,
			Projection: egressauth.ProjectionSpec{
				Type: egressauth.CredentialProjectionTypeHTTPHeaders,
				HTTPHeaders: &egressauth.HTTPHeadersProjection{
					Headers: []egressauth.ProjectedHeader{{
						Name:          "Authorization",
						ValueTemplate: "Bearer {{ .token }}",
					}},
				},
			},
		}},
	}))

	svc, client, _ := newSandboxServiceForNetworkTests(t, pod, store, network.NewNoopProvider())
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("boom")
	})

	_, err := svc.UpdateNetworkPolicy(ctx, pod.Name, testNetworkPolicy("new-ref", "Bearer new"))
	require.Error(t, err)

	record, err := store.GetBindings(ctx, "team-1", pod.Name)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Len(t, record.Bindings, 1)
	assert.Equal(t, "existing-ref", record.Bindings[0].Ref)
}

func TestUpdateSandboxRollsBackBindingsWhenPodUpdateFails(t *testing.T) {
	ctx := context.Background()
	pod := testSandboxNetworkPod()
	store := newMemoryBindingStore()
	store.addStaticHeadersSource("team-1", "new-ref", 2, 1, map[string]string{"token": "new"})
	require.NoError(t, store.UpsertBindings(ctx, &egressauth.BindingRecord{
		SandboxID: pod.Name,
		TeamID:    "team-1",
		Bindings: []egressauth.CredentialBinding{{
			Ref:           "existing-ref",
			SourceRef:     "existing-ref",
			SourceID:      1,
			SourceVersion: 1,
			Projection: egressauth.ProjectionSpec{
				Type: egressauth.CredentialProjectionTypeHTTPHeaders,
				HTTPHeaders: &egressauth.HTTPHeadersProjection{
					Headers: []egressauth.ProjectedHeader{{
						Name:          "Authorization",
						ValueTemplate: "Bearer {{ .token }}",
					}},
				},
			},
		}},
	}))

	svc, client, _ := newSandboxServiceForNetworkTests(t, pod, store, network.NewNoopProvider())
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("boom")
	})

	_, err := svc.UpdateSandbox(ctx, pod.Name, &SandboxUpdateConfig{
		Network: testNetworkPolicy("new-ref", "Bearer new"),
	})
	require.Error(t, err)

	record, err := store.GetBindings(ctx, "team-1", pod.Name)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Len(t, record.Bindings, 1)
	assert.Equal(t, "existing-ref", record.Bindings[0].Ref)
}

func TestRequestCredentialBindingsUsesNetworkBindings(t *testing.T) {
	cfg := &SandboxConfig{
		Network: &v1alpha1.SandboxNetworkPolicy{
			CredentialBindings: testCredentialBindings("nested-ref", "Bearer nested"),
		},
	}

	bindings := requestCredentialBindings(cfg)
	require.Len(t, bindings, 1)
	assert.Equal(t, "nested-ref", bindings[0].Ref)
}

func TestTemplateCredentialBindingsUsesNestedNetworkBindings(t *testing.T) {
	bindings := templateCredentialBindings(&v1alpha1.SandboxNetworkPolicy{
		CredentialBindings: testCredentialBindings("nested-ref", "Bearer nested"),
	})

	require.Len(t, bindings, 1)
	assert.Equal(t, "nested-ref", bindings[0].Ref)
}

func TestUpdateNetworkPolicyStoresBindingsOutsidePodConfig(t *testing.T) {
	ctx := context.Background()
	pod := testSandboxNetworkPod()
	store := newMemoryBindingStore()
	store.addStaticHeadersSource("team-1", "example-ref", 3, 1, map[string]string{"token": "stored"})
	provider := &assertingNetworkProvider{
		applyFunc: func(input network.SandboxPolicyInput) {
			record, err := store.GetBindings(ctx, "team-1", pod.Name)
			require.NoError(t, err)
			require.NotNil(t, record)
			require.Len(t, record.Bindings, 1)
			assert.Equal(t, "example-ref", record.Bindings[0].Ref)
			require.NotNil(t, input.NetworkPolicy)
			require.NotNil(t, input.NetworkPolicy.Egress)
			require.Len(t, input.NetworkPolicy.Egress.CredentialRules, 1)
			assert.Equal(t, "example-ref", input.NetworkPolicy.Egress.CredentialRules[0].CredentialRef)
		},
	}

	svc, client, indexer := newSandboxServiceForNetworkTests(t, pod, store, provider)

	updated, err := svc.UpdateNetworkPolicy(ctx, pod.Name, testNetworkPolicy("example-ref", "Bearer stored"))
	require.NoError(t, err)
	require.NotNil(t, updated)

	storedPod, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, indexer.Update(storedPod.DeepCopy()))
	svc.podLister = corelisters.NewPodLister(indexer)

	var cfg SandboxConfig
	require.NoError(t, json.Unmarshal([]byte(storedPod.Annotations[controller.AnnotationConfig]), &cfg))
	require.NotNil(t, cfg.Network)
	assert.Nil(t, cfg.Network.CredentialBindings)

	effective, err := svc.GetNetworkPolicy(ctx, pod.Name)
	require.NoError(t, err)
	require.Len(t, effective.CredentialBindings, 1)
	assert.Equal(t, "example-ref", effective.CredentialBindings[0].Ref)
}

func cloneCredentialSource(in *egressauth.CredentialSource) *egressauth.CredentialSource {
	if in == nil {
		return nil
	}
	cloned := *in
	return &cloned
}

func cloneCredentialSourceVersion(in *egressauth.CredentialSourceVersion) *egressauth.CredentialSourceVersion {
	if in == nil {
		return nil
	}
	cloned := *in
	if in.Spec.StaticHeaders != nil {
		cloned.Spec.StaticHeaders = &egressauth.StaticHeadersSourceSpec{
			Values: cloneStringMap(in.Spec.StaticHeaders.Values),
		}
	}
	return &cloned
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
