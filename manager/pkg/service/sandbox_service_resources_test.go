package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func TestEffectiveSandboxResourceQuotaDerivesCPUFromMemory(t *testing.T) {
	svc := &SandboxService{config: SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"}}
	template := newSandboxResourceTestTemplate(t)

	quota, err := svc.effectiveSandboxResourceQuota(template, &SandboxConfig{
		Resources: &SandboxResourceConfig{Memory: "128Mi"},
	})
	if err != nil {
		t.Fatalf("effectiveSandboxResourceQuota() error = %v", err)
	}
	assertQuantity(t, quota.Memory, "128Mi")
	assertQuantity(t, quota.CPU, "32m")
}

func TestValidateSandboxMemoryBounds(t *testing.T) {
	tests := []struct {
		name      string
		maxMemory string
		memory    string
		wantErr   string
	}{
		{name: "minimum accepted", memory: "128Mi"},
		{name: "below minimum rejected", memory: "127Mi", wantErr: "must be >= 128Mi"},
		{name: "default max accepted", memory: "32Gi"},
		{name: "above default max rejected", memory: "33Gi", wantErr: "must be <= 32Gi"},
		{name: "custom max accepted", maxMemory: "64Gi", memory: "64Gi"},
		{name: "above custom max rejected", maxMemory: "64Gi", memory: "65Gi", wantErr: "must be <= 64Gi"},
		{name: "invalid rejected", memory: "large", wantErr: "is invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &SandboxService{config: SandboxServiceConfig{SandboxMaxMemory: tt.maxMemory}}
			_, err := svc.validateSandboxMemory(tt.memory)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateSandboxMemory() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !errors.Is(err, ErrInvalidClaimRequest) {
				t.Fatalf("validateSandboxMemory() error = %v, want ErrInvalidClaimRequest", err)
			}
			if got := err.Error(); !contains(got, tt.wantErr) {
				t.Fatalf("validateSandboxMemory() error = %q, want substring %q", got, tt.wantErr)
			}
		})
	}
}

func TestCreateNewPodAppliesClaimMemoryResources(t *testing.T) {
	withClaimTestPublicKey(t)
	template := newSandboxResourceTestTemplate(t)
	client := fake.NewSimpleClientset()
	svc := &SandboxService{
		k8sClient:    client,
		secretLister: newClaimTestSecretLister(t),
		clock:        systemTime{},
		config:       SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"},
		logger:       zap.NewNop(),
	}

	pod, err := svc.createNewPod(context.Background(), template, &ClaimRequest{
		TeamID: "team-a",
		UserID: "user-a",
		Config: &SandboxConfig{
			Resources: &SandboxResourceConfig{Memory: "2Gi"},
		},
	})
	if err != nil {
		t.Fatalf("createNewPod() error = %v", err)
	}
	container := sandboxRuntimeContainer(t, pod)
	assertQuantity(t, container.Resources.Limits[corev1.ResourceMemory], "2Gi")
	assertQuantity(t, container.Resources.Limits[corev1.ResourceCPU], "500m")
	assertResizePolicy(t, container.ResizePolicy, corev1.ResourceCPU, corev1.NotRequired)
	assertResizePolicy(t, container.ResizePolicy, corev1.ResourceMemory, corev1.NotRequired)
}

func TestCreateNewPodAppliesTemplateResourcesByDefault(t *testing.T) {
	withClaimTestPublicKey(t)
	template := newSandboxResourceTestTemplate(t)
	client := fake.NewSimpleClientset()
	svc := &SandboxService{
		k8sClient:    client,
		secretLister: newClaimTestSecretLister(t),
		clock:        systemTime{},
		config:       SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"},
		logger:       zap.NewNop(),
	}

	pod, err := svc.createNewPod(context.Background(), template, &ClaimRequest{
		TeamID: "team-a",
		UserID: "user-a",
	})
	if err != nil {
		t.Fatalf("createNewPod() error = %v", err)
	}
	container := sandboxRuntimeContainer(t, pod)
	assertQuantity(t, container.Resources.Limits[corev1.ResourceMemory], "1Gi")
	assertQuantity(t, container.Resources.Limits[corev1.ResourceCPU], "250m")
	assertQuantity(t, container.Resources.Requests[corev1.ResourceMemory], "256Mi")
	assertQuantity(t, container.Resources.Requests[corev1.ResourceCPU], "25m")
}

func TestClaimIdlePodAppliesTemplateResourcesByDefault(t *testing.T) {
	template := newSandboxResourceTestTemplate(t)
	idlePod := newSandboxResourceTestIdlePod(t, template, "idle-ready")
	node := newClaimTestNode("node-a", "10.0.0.1")
	node.Labels = map[string]string{dataplane.NodeDataPlaneReadyLabel: dataplane.ReadyLabelValue}
	idlePod.Spec.NodeName = node.Name
	client := fake.NewSimpleClientset(idlePod.DeepCopy(), node.DeepCopy())
	svc := &SandboxService{
		k8sClient:  client,
		podLister:  newClaimTestPodLister(t, idlePod),
		nodeLister: newClaimTestNodeLister(t, node),
		clock:      systemTime{},
		config:     SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"},
		logger:     zap.NewNop(),
	}

	pod, err := svc.claimIdlePod(context.Background(), template, &ClaimRequest{
		TeamID: "team-a",
		UserID: "user-a",
	})
	if err != nil {
		t.Fatalf("claimIdlePod() error = %v", err)
	}
	container := sandboxRuntimeContainer(t, pod)
	assertQuantity(t, container.Resources.Limits[corev1.ResourceMemory], "1Gi")
	assertQuantity(t, container.Resources.Limits[corev1.ResourceCPU], "250m")
	assertQuantity(t, container.Resources.Requests[corev1.ResourceMemory], "256Mi")
	assertQuantity(t, container.Resources.Requests[corev1.ResourceCPU], "25m")
	assertResizeSubresourceUpdate(t, client.Actions())
}

func TestClaimIdlePodAppliesMemoryOverride(t *testing.T) {
	template := newSandboxResourceTestTemplate(t)
	idlePod := newSandboxResourceTestIdlePod(t, template, "idle-ready")
	node := newClaimTestNode("node-a", "10.0.0.1")
	node.Labels = map[string]string{dataplane.NodeDataPlaneReadyLabel: dataplane.ReadyLabelValue}
	idlePod.Spec.NodeName = node.Name
	client := fake.NewSimpleClientset(idlePod.DeepCopy(), node.DeepCopy())
	svc := &SandboxService{
		k8sClient:  client,
		podLister:  newClaimTestPodLister(t, idlePod),
		nodeLister: newClaimTestNodeLister(t, node),
		clock:      systemTime{},
		config:     SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"},
		logger:     zap.NewNop(),
	}

	pod, err := svc.claimIdlePod(context.Background(), template, &ClaimRequest{
		TeamID: "team-a",
		UserID: "user-a",
		Config: &SandboxConfig{Resources: &SandboxResourceConfig{Memory: "2Gi"}},
	})
	if err != nil {
		t.Fatalf("claimIdlePod() error = %v", err)
	}
	container := sandboxRuntimeContainer(t, pod)
	assertQuantity(t, container.Resources.Limits[corev1.ResourceMemory], "2Gi")
	assertQuantity(t, container.Resources.Limits[corev1.ResourceCPU], "500m")
	assertResizeSubresourceUpdate(t, client.Actions())
}

func TestResizeSandboxPodResourcesRetriesConflictWithFreshPod(t *testing.T) {
	template := newSandboxResourceTestTemplate(t)
	pod := newSandboxResourceTestActivePod(t, template, "sandbox-1")
	client := fake.NewSimpleClientset(pod.DeepCopy())
	resizeUpdates := 0
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "resize" {
			return false, nil, nil
		}
		resizeUpdates++
		if resizeUpdates == 1 {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, pod.Name, errors.New("stale pod"))
		}
		return false, nil, nil
	})
	svc := &SandboxService{
		k8sClient: client,
		config:    SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"},
		logger:    zap.NewNop(),
	}
	quota, err := svc.effectiveSandboxResourceQuota(template, &SandboxConfig{
		Resources: &SandboxResourceConfig{Memory: "2Gi"},
	})
	if err != nil {
		t.Fatalf("effectiveSandboxResourceQuota() error = %v", err)
	}

	resized, err := svc.resizeSandboxPodResources(context.Background(), pod, quota)
	if err != nil {
		t.Fatalf("resizeSandboxPodResources() error = %v", err)
	}
	if resizeUpdates != 2 {
		t.Fatalf("resize update calls = %d, want 2", resizeUpdates)
	}
	container := sandboxRuntimeContainer(t, resized)
	assertQuantity(t, container.Resources.Limits[corev1.ResourceMemory], "2Gi")
	assertQuantity(t, container.Resources.Limits[corev1.ResourceCPU], "500m")
}

func TestClaimIdlePodRestoresIdlePodAfterResizeConflict(t *testing.T) {
	template := newSandboxResourceTestTemplate(t)
	idlePod := newSandboxResourceTestIdlePod(t, template, "idle-ready")
	node := newClaimTestNode("node-a", "10.0.0.1")
	node.Labels = map[string]string{dataplane.NodeDataPlaneReadyLabel: dataplane.ReadyLabelValue}
	idlePod.Spec.NodeName = node.Name
	client := fake.NewSimpleClientset(idlePod.DeepCopy(), node.DeepCopy())
	resizeUpdates := 0
	deletes := 0
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "resize" {
			return false, nil, nil
		}
		resizeUpdates++
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, idlePod.Name, errors.New("stale pod"))
	})
	client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deletes++
		return false, nil, nil
	})
	svc := &SandboxService{
		k8sClient:  client,
		podLister:  newClaimTestPodLister(t, idlePod),
		nodeLister: newClaimTestNodeLister(t, node),
		clock:      systemTime{},
		config:     SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"},
		logger:     zap.NewNop(),
	}

	pod, err := svc.claimIdlePod(context.Background(), template, &ClaimRequest{
		TeamID: "team-a",
		UserID: "user-a",
		Config: &SandboxConfig{Resources: &SandboxResourceConfig{Memory: "2Gi"}},
	})
	if err != nil {
		t.Fatalf("claimIdlePod() error = %v", err)
	}
	if pod != nil {
		t.Fatalf("claimIdlePod() = %s, want nil after resize conflict", pod.Name)
	}
	if resizeUpdates == 0 {
		t.Fatal("resize updates = 0, want at least one resize attempt")
	}
	if deletes != 0 {
		t.Fatalf("delete calls = %d, want 0", deletes)
	}
	stored, err := client.CoreV1().Pods(idlePod.Namespace).Get(context.Background(), idlePod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get stored pod: %v", err)
	}
	if got := stored.Labels[controller.LabelPoolType]; got != controller.PoolTypeIdle {
		t.Fatalf("pool-type = %q, want %q", got, controller.PoolTypeIdle)
	}
	if got := stored.Labels[controller.LabelSandboxID]; got != "" {
		t.Fatalf("sandbox label = %q, want empty", got)
	}
	if got := stored.Annotations[controller.AnnotationSandboxID]; got != "" {
		t.Fatalf("sandbox annotation = %q, want empty", got)
	}
}

func TestUpdateSandboxAppliesMemoryResourcesAndPersistsConfig(t *testing.T) {
	template := newSandboxResourceTestTemplate(t)
	pod := newSandboxResourceTestActivePod(t, template, "sandbox-1")
	client := fake.NewSimpleClientset(pod.DeepCopy())
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	if err := indexer.Add(pod.DeepCopy()); err != nil {
		t.Fatalf("add pod: %v", err)
	}
	svc := &SandboxService{
		k8sClient:      client,
		podLister:      corelisters.NewPodLister(indexer),
		templateLister: staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
		clock:          systemTime{},
		config:         SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"},
		logger:         zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	updated, err := svc.UpdateSandbox(ctx, "sandbox-1", &SandboxUpdateConfig{
		Resources: &SandboxResourceConfig{Memory: "2Gi"},
	})
	if err != nil {
		t.Fatalf("UpdateSandbox() error = %v", err)
	}
	if updated.Resources == nil || updated.Resources.Memory != "2Gi" {
		t.Fatalf("updated resources = %#v, want memory 2Gi", updated.Resources)
	}
	stored, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	container := sandboxRuntimeContainer(t, stored)
	assertQuantity(t, container.Resources.Limits[corev1.ResourceMemory], "2Gi")
	assertQuantity(t, container.Resources.Limits[corev1.ResourceCPU], "500m")
	if got := stored.Annotations[controller.AnnotationConfig]; !contains(got, `"resources":{"memory":"2Gi"}`) {
		t.Fatalf("config annotation = %s, want resources memory", got)
	}
	assertResizeSubresourceUpdate(t, client.Actions())
}

func TestUpdatePausedSandboxValidatesAndPersistsMemory(t *testing.T) {
	now := timeNow()
	record := &SandboxRecord{
		ID:           "sandbox-1",
		TeamID:       "team-a",
		TemplateID:   "default",
		Status:       SandboxStatusPaused,
		Config:       SandboxConfig{},
		ClaimedAt:    now,
		CreatedAt:    now,
		UpdatedAt:    now,
		TemplateSpec: newSandboxResourceTestTemplate(t).Spec,
	}
	store := &memorySandboxStore{records: map[string]*SandboxRecord{"sandbox-1": record}}
	svc := &SandboxService{sandboxStore: store, clock: fixedClock{now: now}, logger: zap.NewNop()}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	updated, err := svc.UpdateSandbox(ctx, "sandbox-1", &SandboxUpdateConfig{
		Resources: &SandboxResourceConfig{Memory: "2Gi"},
	})
	if err != nil {
		t.Fatalf("UpdateSandbox() error = %v", err)
	}
	if updated.Resources == nil || updated.Resources.Memory != "2Gi" {
		t.Fatalf("updated resources = %#v, want memory 2Gi", updated.Resources)
	}
	stored := store.records["sandbox-1"]
	if stored.Config.Resources == nil || stored.Config.Resources.Memory != "2Gi" {
		t.Fatalf("stored resources = %#v, want memory 2Gi", stored.Config.Resources)
	}
}

func TestUpdateSandboxMemoryQuotaUsesIncreaseOnly(t *testing.T) {
	template := newSandboxResourceTestTemplate(t)
	pod := newSandboxResourceTestActivePod(t, template, "sandbox-1")
	client := fake.NewSimpleClientset(pod.DeepCopy())
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	if err := indexer.Add(pod.DeepCopy()); err != nil {
		t.Fatalf("add pod: %v", err)
	}
	svc := &SandboxService{
		k8sClient:      client,
		podLister:      corelisters.NewPodLister(indexer),
		templateLister: staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{template}},
		quotaStore: fakeQuotaLimitStore{
			limit: &quota.Limit{TeamID: "team-a", Dimension: quota.DimensionMemory, LimitValue: 2048},
			usage: 1024,
		},
		clock:  systemTime{},
		config: SandboxServiceConfig{SandboxMemoryPerCPU: "4Gi"},
		logger: zap.NewNop(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := svc.UpdateSandbox(ctx, "sandbox-1", &SandboxUpdateConfig{
		Resources: &SandboxResourceConfig{Memory: "2Gi"},
	}); err != nil {
		t.Fatalf("UpdateSandbox() error = %v, want nil because only 1Gi increase is charged", err)
	}
}

func newSandboxResourceTestTemplate(t *testing.T) *v1alpha1.SandboxTemplate {
	t.Helper()
	namespace, err := naming.TemplateNamespaceForTeam("team-a")
	if err != nil {
		t.Fatalf("template namespace: %v", err)
	}
	return &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{
				Image: "busybox",
				Resources: v1alpha1.ResourceQuota{
					CPU:    resource.MustParse("250m"),
					Memory: resource.MustParse("1Gi"),
				},
			},
		},
	}
}

func newSandboxResourceTestIdlePod(t *testing.T, template *v1alpha1.SandboxTemplate, name string) *corev1.Pod {
	t.Helper()
	pod := newClaimTestPod(template.Namespace, name, template.Name, true)
	pod.Spec = v1alpha1.BuildIdlePodSpec(template)
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:   v1alpha1.SandboxPodReadinessConditionType,
		Status: corev1.ConditionTrue,
	})
	hash, err := controller.TemplateSpecHash(template)
	if err != nil {
		t.Fatalf("template hash: %v", err)
	}
	pod.Annotations[controller.AnnotationTemplateSpecHash] = hash
	return pod
}

func newSandboxResourceTestActivePod(t *testing.T, template *v1alpha1.SandboxTemplate, name string) *corev1.Pod {
	t.Helper()
	pod := newSandboxResourceTestIdlePod(t, template, name)
	pod.Spec = v1alpha1.BuildPodSpec(template)
	pod.Labels[controller.LabelPoolType] = controller.PoolTypeActive
	pod.Labels[controller.LabelSandboxID] = name
	pod.Annotations[controller.AnnotationSandboxID] = name
	pod.Annotations[controller.AnnotationTeamID] = "team-a"
	pod.Annotations[controller.AnnotationUserID] = "user-a"
	pod.Status.PodIP = "10.244.0.10"
	return pod
}

func sandboxRuntimeContainer(t *testing.T, pod *corev1.Pod) corev1.Container {
	t.Helper()
	if pod == nil {
		t.Fatal("pod is nil")
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == "procd" {
			return container
		}
	}
	t.Fatal("missing procd container")
	return corev1.Container{}
}

func assertQuantity(t *testing.T, got resource.Quantity, want string) {
	t.Helper()
	wantQuantity := resource.MustParse(want)
	if got.Cmp(wantQuantity) != 0 {
		t.Fatalf("quantity = %s, want %s", got.String(), wantQuantity.String())
	}
}

func assertResizePolicy(t *testing.T, policies []corev1.ContainerResizePolicy, resourceName corev1.ResourceName, want corev1.ResourceResizeRestartPolicy) {
	t.Helper()
	for _, policy := range policies {
		if policy.ResourceName == resourceName {
			if policy.RestartPolicy != want {
				t.Fatalf("resize policy %s = %s, want %s", resourceName, policy.RestartPolicy, want)
			}
			return
		}
	}
	t.Fatalf("missing resize policy for %s", resourceName)
}

func assertResizeSubresourceUpdate(t *testing.T, actions []k8stesting.Action) {
	t.Helper()
	for _, action := range actions {
		if action.Matches("update", "pods") && action.GetSubresource() == "resize" {
			return
		}
	}
	t.Fatalf("missing update pods/resize action; actions = %#v", actions)
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

func timeNow() time.Time {
	return time.Date(2026, time.June, 28, 12, 0, 0, 0, time.UTC)
}
