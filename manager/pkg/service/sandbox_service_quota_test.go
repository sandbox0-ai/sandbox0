package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/quota"
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
