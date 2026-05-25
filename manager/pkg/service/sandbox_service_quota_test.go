package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fakeQuotaLimitStore struct {
	limit *quota.Limit
	usage int64
	err   error
}

func (f fakeQuotaLimitStore) GetLimit(context.Context, string, quota.Dimension) (*quota.Limit, error) {
	return f.limit, f.err
}

func (f fakeQuotaLimitStore) CurrentUsage(context.Context, string, quota.Dimension) (int64, error) {
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

func TestEnforceSandboxCPUQuotaRejectsWhenRequestedWouldExceedLimit(t *testing.T) {
	svc := &SandboxService{
		quotaStore: fakeQuotaLimitStore{
			limit: &quota.Limit{TeamID: "team-1", Dimension: quota.DimensionCPU, LimitValue: 1000},
			usage: 750,
		},
	}
	template := newQuotaTestTemplate("default", "500m", "1Gi")

	err := svc.enforceSandboxCPUQuota(context.Background(), "team-1", template)
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

	if err := svc.enforceSandboxCPUQuota(context.Background(), "team-1", template); err != nil {
		t.Fatalf("enforceSandboxCPUQuota() error = %v, want nil", err)
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

	err := svc.enforceSandboxMemoryQuota(context.Background(), "team-1", template)
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

	if err := svc.enforceSandboxMemoryQuota(context.Background(), "team-1", template); err != nil {
		t.Fatalf("enforceSandboxMemoryQuota() error = %v, want nil", err)
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
