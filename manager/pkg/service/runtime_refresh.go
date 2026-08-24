package service

import (
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

type RefreshRequest struct {
	Duration int32 `json:"duration,omitempty"`
}

type RefreshResponse struct {
	SandboxID     string     `json:"sandbox_id"`
	ExpiresAt     *time.Time `json:"expires_at"`
	HardExpiresAt *time.Time `json:"hard_expires_at"`
}

type sandboxRefreshPlan struct {
	expiresAt     time.Time
	hardExpiresAt time.Time
}

func buildSandboxRefreshPlan(config sandboxstore.SandboxConfig, defaultTTL time.Duration, now time.Time, request *RefreshRequest) (sandboxRefreshPlan, error) {
	var ttl *int32
	if request != nil {
		if request.Duration < 0 {
			return sandboxRefreshPlan{}, fmt.Errorf("%w: duration must be >= 0", ErrInvalidClaimRequest)
		}
		if request.Duration > 0 {
			ttl = cloneInt32Ptr(&request.Duration)
		}
	}
	if ttl == nil && config.TTL != nil {
		ttl = cloneInt32Ptr(config.TTL)
	} else if ttl == nil && defaultTTL > 0 {
		seconds := int32(defaultTTL / time.Second)
		ttl = &seconds
	}
	if err := validateSandboxConfigLifecycle(ttl, config.HardTTL); err != nil {
		return sandboxRefreshPlan{}, err
	}
	plan := sandboxRefreshPlan{}
	if ttl != nil && *ttl > 0 {
		plan.expiresAt = refreshDeadline(now, *ttl)
	}
	if config.HardTTL != nil && *config.HardTTL > 0 {
		plan.hardExpiresAt = refreshDeadline(now, *config.HardTTL)
	}
	return plan, nil
}

func refreshDeadline(now time.Time, seconds int32) time.Time {
	return now.UTC().Truncate(time.Second).Add(time.Duration(seconds) * time.Second)
}
