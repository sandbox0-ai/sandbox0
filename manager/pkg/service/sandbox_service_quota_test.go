package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type fakeQuotaLimitStore struct {
	limit    *quota.Limit
	usage    int64
	err      error
	usageErr error
}

func (f fakeQuotaLimitStore) GetLimit(context.Context, string, quota.Dimension) (*quota.Limit, error) {
	return f.limit, f.err
}

func (f fakeQuotaLimitStore) CurrentUsage(context.Context, string, quota.Dimension) (int64, error) {
	if f.usageErr != nil {
		return 0, f.usageErr
	}
	return f.usage, f.err
}

func TestEnforceActiveSandboxQuotaAllowsWhenNoLimit(t *testing.T) {
	svc := &SandboxService{quotaStore: fakeQuotaLimitStore{}}
	if err := svc.enforceActiveSandboxQuota(context.Background(), "team-1"); err != nil {
		t.Fatalf("enforceActiveSandboxQuota() error = %v, want nil", err)
	}
}

func TestEnforceActiveSandboxQuotaRejectsAtLimit(t *testing.T) {
	svc := &SandboxService{
		quotaStore: fakeQuotaLimitStore{
			limit: &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 1},
			usage: 1,
		},
	}

	err := svc.enforceActiveSandboxQuota(context.Background(), "team-1")
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("enforceActiveSandboxQuota() error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEnforceActiveSandboxQuotaAllowsBelowLimit(t *testing.T) {
	svc := &SandboxService{
		quotaStore: fakeQuotaLimitStore{
			limit: &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 2},
			usage: 1,
		},
	}

	if err := svc.enforceActiveSandboxQuota(context.Background(), "team-1"); err != nil {
		t.Fatalf("enforceActiveSandboxQuota() error = %v, want nil", err)
	}
}

func TestEnforceActiveSandboxQuotaUsesLivePodsWhenUsageStoreDisabled(t *testing.T) {
	svc := newQuotaTestServiceWithPods(
		fakeQuotaLimitStore{
			limit:    &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 1},
			usageErr: quota.ErrUsageStoreNotConfigured,
		},
		newQuotaTestPod("default", "sandbox-1", "team-1", corev1.PodRunning, "500m", "512Mi"),
		newQuotaTestPod("default", "sandbox-2", "team-2", corev1.PodRunning, "500m", "512Mi"),
	)

	err := svc.enforceActiveSandboxQuota(context.Background(), "team-1")
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("enforceActiveSandboxQuota() error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEnforceActiveSandboxQuotaIgnoresNonActiveLivePods(t *testing.T) {
	deleting := newQuotaTestPod("default", "deleting", "team-1", corev1.PodRunning, "500m", "512Mi")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now

	idle := newQuotaTestPod("default", "idle", "team-1", corev1.PodRunning, "500m", "512Mi")
	idle.Labels[controller.LabelPoolType] = controller.PoolTypeIdle

	failed := newQuotaTestPod("default", "failed", "team-1", corev1.PodFailed, "500m", "512Mi")

	svc := newQuotaTestServiceWithPods(
		fakeQuotaLimitStore{
			limit:    &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 1},
			usageErr: quota.ErrUsageStoreNotConfigured,
		},
		deleting,
		idle,
		failed,
	)

	if err := svc.enforceActiveSandboxQuota(context.Background(), "team-1"); err != nil {
		t.Fatalf("enforceActiveSandboxQuota() error = %v, want nil", err)
	}
}

func TestEnforceSandboxCPUQuotaRejectsWhenRequestedWouldExceedLimit(t *testing.T) {
	svc := &SandboxService{
		quotaStore: fakeQuotaLimitStore{
			limit: &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionCPU, LimitValue: 1000},
			usage: 750,
		},
	}
	template := newQuotaTestTemplate("default", "500m", "1Gi")

	err := svc.enforceSandboxCPUQuota(context.Background(), "team-1", template.Spec.MainContainer.Resources)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("enforceSandboxCPUQuota() error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEnforceSandboxCPUQuotaAllowsBelowLimit(t *testing.T) {
	svc := &SandboxService{
		quotaStore: fakeQuotaLimitStore{
			limit: &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionCPU, LimitValue: 1000},
			usage: 250,
		},
	}
	template := newQuotaTestTemplate("default", "500m", "1Gi")

	if err := svc.enforceSandboxCPUQuota(context.Background(), "team-1", template.Spec.MainContainer.Resources); err != nil {
		t.Fatalf("enforceSandboxCPUQuota() error = %v, want nil", err)
	}
}

func TestEnforceSandboxCPUQuotaUsesLivePodsWhenUsageStoreDisabled(t *testing.T) {
	svc := newQuotaTestServiceWithPods(
		fakeQuotaLimitStore{
			limit:    &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionCPU, LimitValue: 1000},
			usageErr: quota.ErrUsageStoreNotConfigured,
		},
		newQuotaTestPod("default", "sandbox-1", "team-1", corev1.PodRunning, "750m", "512Mi"),
	)
	template := newQuotaTestTemplate("default", "500m", "1Gi")

	err := svc.enforceSandboxCPUQuota(context.Background(), "team-1", template.Spec.MainContainer.Resources)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("enforceSandboxCPUQuota() error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEnforceSandboxMemoryQuotaRejectsWhenRequestedWouldExceedLimit(t *testing.T) {
	svc := &SandboxService{
		quotaStore: fakeQuotaLimitStore{
			limit: &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionMemory, LimitValue: 1024},
			usage: 768,
		},
	}
	template := newQuotaTestTemplate("default", "500m", "512Mi")

	err := svc.enforceSandboxMemoryQuota(context.Background(), "team-1", template.Spec.MainContainer.Resources)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("enforceSandboxMemoryQuota() error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEnforceSandboxMemoryQuotaAllowsBelowLimit(t *testing.T) {
	svc := &SandboxService{
		quotaStore: fakeQuotaLimitStore{
			limit: &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionMemory, LimitValue: 2048},
			usage: 768,
		},
	}
	template := newQuotaTestTemplate("default", "500m", "512Mi")

	if err := svc.enforceSandboxMemoryQuota(context.Background(), "team-1", template.Spec.MainContainer.Resources); err != nil {
		t.Fatalf("enforceSandboxMemoryQuota() error = %v, want nil", err)
	}
}

func TestEnforceSandboxMemoryQuotaUsesLivePodsWhenUsageStoreDisabled(t *testing.T) {
	svc := newQuotaTestServiceWithPods(
		fakeQuotaLimitStore{
			limit:    &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionMemory, LimitValue: 1024},
			usageErr: quota.ErrUsageStoreNotConfigured,
		},
		newQuotaTestPod("default", "sandbox-1", "team-1", corev1.PodRunning, "500m", "768Mi"),
	)
	template := newQuotaTestTemplate("default", "500m", "512Mi")

	err := svc.enforceSandboxMemoryQuota(context.Background(), "team-1", template.Spec.MainContainer.Resources)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("enforceSandboxMemoryQuota() error = %v, want ErrQuotaExceeded", err)
	}
}

func newQuotaTestTemplate(name, cpu, memory string) *v1alpha1.SandboxTemplate {
	return &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{
				Resources: v1alpha1.ResourceQuota{
					CPU:    resource.MustParse(cpu),
					Memory: resource.MustParse(memory),
				},
			},
		},
	}
}

func newQuotaTestServiceWithPods(quotaStore TeamQuotaLimitStore, pods ...*corev1.Pod) *SandboxService {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, pod := range pods {
		_ = indexer.Add(pod)
	}
	return &SandboxService{
		quotaStore: quotaStore,
		podLister:  corelisters.NewPodLister(indexer),
	}
}

func newQuotaTestPod(namespace, name, teamID string, phase corev1.PodPhase, cpu, memory string) *corev1.Pod {
	sandboxID := name
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels: map[string]string{
				controller.LabelPoolType:  controller.PoolTypeActive,
				controller.LabelSandboxID: sandboxID,
			},
			Annotations: map[string]string{
				controller.AnnotationSandboxID: sandboxID,
				controller.AnnotationTeamID:    teamID,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "main",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse(cpu),
						corev1.ResourceMemory: resource.MustParse(memory),
					},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}
