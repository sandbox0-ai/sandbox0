package proxy

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	teamquotadistributed "github.com/sandbox0-ai/sandbox0/pkg/teamquota/distributed"
	teamquotanetwork "github.com/sandbox0-ai/sandbox0/pkg/teamquota/network"
	teamquotarate "github.com/sandbox0-ai/sandbox0/pkg/teamquota/rate"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
)

// TeamNetworkQuota adapts the shared network limiter to netd's immediate
// operation admission and direction-aware byte paths.
type TeamNetworkQuota struct {
	*teamquotanetwork.Limiter
}

// NewTeamNetworkQuota creates a fail-closed region-shared network limiter.
func NewTeamNetworkQuota(
	regionID string,
	resolver teamquotarate.PolicyResolver,
	marker teamquotadistributed.AtomicAdmissionMarker,
	bucket tokenbucket.GuardedBucket,
	policyCacheTTL time.Duration,
) (*TeamNetworkQuota, error) {
	limiter, err := teamquotanetwork.New(
		resolver,
		marker,
		bucket,
		teamquotanetwork.Config{
			RegionID:       regionID,
			PolicyCacheTTL: policyCacheTTL,
		},
	)
	if err != nil {
		return nil, err
	}
	return &TeamNetworkQuota{Limiter: limiter}, nil
}

func (q *TeamNetworkQuota) waitN(
	ctx context.Context,
	teamID string,
	direction bandwidthDirection,
	bytes int,
) error {
	key, err := networkQuotaKey(direction)
	if err != nil {
		return err
	}
	if q == nil || q.Limiter == nil {
		return networkQuotaUnavailable(
			"take network rate tokens",
			"network team quota is not configured",
		)
	}
	return q.WaitN(ctx, teamID, key, bytes)
}

func (q *TeamNetworkQuota) maxChunkBytes(
	ctx context.Context,
	teamID string,
	direction bandwidthDirection,
) (int, error) {
	key, err := networkQuotaKey(direction)
	if err != nil {
		return 0, err
	}
	if q == nil || q.Limiter == nil {
		return 0, networkQuotaUnavailable(
			"resolve network rate policy",
			"network team quota is not configured",
		)
	}
	return q.MaxChunkBytes(ctx, teamID, key)
}

func (q *TeamNetworkQuota) takeOperation(
	ctx context.Context,
	teamID string,
) error {
	if q == nil || q.Limiter == nil {
		return networkQuotaUnavailable(
			"take network operation rate tokens",
			"network team quota is not configured",
		)
	}
	decision, err := q.Take(
		ctx,
		teamID,
		teamquota.KeyNetworkOperations,
		1,
	)
	if err != nil {
		return err
	}
	if decision.Allowed {
		return nil
	}
	return &teamquota.RateExceededError{
		TeamID:     teamID,
		Key:        teamquota.KeyNetworkOperations,
		Remaining:  decision.Remaining,
		RetryAfter: decision.RetryAfter,
	}
}

func networkQuotaKey(direction bandwidthDirection) (teamquota.Key, error) {
	switch direction {
	case bandwidthEgress:
		return teamquota.KeyNetworkEgressBytes, nil
	case bandwidthIngress:
		return teamquota.KeyNetworkIngressBytes, nil
	default:
		return "", fmt.Errorf("unknown bandwidth direction %q", direction)
	}
}

func networkQuotaUnavailable(operation, message string) error {
	return &teamquota.UnavailableError{
		Operation: operation,
		Err:       fmt.Errorf("%s", message),
	}
}
