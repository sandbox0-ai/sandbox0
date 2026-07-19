package service

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/startlimiter"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

func TestSandboxServiceValidateTeamQuotaReadyFailsClosed(t *testing.T) {
	service := &SandboxService{}
	if err := service.ValidateTeamQuotaReady(); !errors.Is(err, ErrTeamQuotaUnavailable) {
		t.Fatalf("missing capacity store error = %v", err)
	}

	service.SetTeamQuotaStore(teamquota.NewRepository(nil))
	if err := service.ValidateTeamQuotaReady(); !errors.Is(err, ErrTeamQuotaUnavailable) {
		t.Fatalf("missing rate limiter error = %v", err)
	}

	service.SetTeamQuotaRateLimiter(readinessRateLimiter{})
	if err := service.ValidateTeamQuotaReady(); !errors.Is(err, ErrTeamQuotaUnavailable) {
		t.Fatalf("missing claim-start limiter error = %v", err)
	}
	service.SetClaimStartLimiter(&startlimiter.Limiter{})
	if err := service.ValidateTeamQuotaReady(); err != nil {
		t.Fatalf("ValidateTeamQuotaReady() error = %v", err)
	}
}

type readinessRateLimiter struct{}

func (readinessRateLimiter) Take(
	context.Context,
	string,
	teamquota.Key,
	int64,
) (tokenbucket.Decision, error) {
	return tokenbucket.Decision{Allowed: true}, nil
}
