package service

import (
	"context"
	"errors"
	"testing"

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

type countingSandboxStore struct {
	*memorySandboxStore
	count int64
	calls int
	err   error
}

func (s *countingSandboxStore) CountActiveSandboxes(context.Context, string) (int64, error) {
	s.calls++
	return s.count, s.err
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

func TestEnforceActiveSandboxQuotaUsesSandboxStoreWhenUsageStoreDisabled(t *testing.T) {
	svc := &SandboxService{
		quotaStore: fakeQuotaLimitStore{
			limit:    &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 1},
			usageErr: quota.ErrUsageStoreNotConfigured,
		},
		sandboxStore: &memorySandboxStore{
			records: map[string]*SandboxRecord{
				"sandbox-1": {
					ID:     "sandbox-1",
					TeamID: "team-1",
					Status: SandboxStatusRunning,
				},
				"paused": {
					ID:     "paused",
					TeamID: "team-1",
					Status: SandboxStatusPaused,
				},
				"other-team": {
					ID:     "other-team",
					TeamID: "team-2",
					Status: SandboxStatusRunning,
				},
			},
		},
	}

	err := svc.enforceActiveSandboxQuota(context.Background(), "team-1")
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("enforceActiveSandboxQuota() error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEnforceActiveSandboxQuotaPrefersOperationalStoreOverUsageProjection(t *testing.T) {
	svc := &SandboxService{
		quotaStore: fakeQuotaLimitStore{
			limit: &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 1},
			usage: 0,
		},
		sandboxStore: &memorySandboxStore{
			records: map[string]*SandboxRecord{
				"sandbox-1": {
					ID:     "sandbox-1",
					TeamID: "team-1",
					Status: SandboxStatusRunning,
				},
			},
		},
	}

	err := svc.enforceActiveSandboxQuota(context.Background(), "team-1")
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("enforceActiveSandboxQuota() error = %v, want ErrQuotaExceeded", err)
	}
}

func TestEnforceActiveSandboxQuotaUsesOptimizedOperationalCount(t *testing.T) {
	store := &countingSandboxStore{
		memorySandboxStore: &memorySandboxStore{},
		count:              1,
	}
	svc := &SandboxService{
		quotaStore: fakeQuotaLimitStore{
			limit:    &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionActiveSandboxes, LimitValue: 2},
			usageErr: errors.New("usage projection must not be queried"),
		},
		sandboxStore: store,
	}

	if err := svc.enforceActiveSandboxQuota(context.Background(), "team-1"); err != nil {
		t.Fatalf("enforceActiveSandboxQuota() error = %v, want nil", err)
	}
	if store.calls != 1 {
		t.Fatalf("CountActiveSandboxes() calls = %d, want 1", store.calls)
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
