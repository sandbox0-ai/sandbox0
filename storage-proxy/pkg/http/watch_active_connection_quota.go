package http

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/activeconnections"
)

const volumeWatchLeaseReleaseTimeout = 2 * time.Second

func (s *Server) acquireVolumeWatchLease(
	ctx context.Context,
	teamID string,
) (activeconnections.Lease, error) {
	if s == nil || s.activeConnections == nil {
		return nil, &teamquota.UnavailableError{
			Operation: "acquire volume watch subscription quota",
			Err:       errors.New("active connection quota is not configured"),
		}
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return nil, &teamquota.UnavailableError{
			Operation: "acquire volume watch subscription quota",
			Err:       errors.New("team_id is required"),
		}
	}
	return s.activeConnections.Acquire(ctx, teamID)
}

func (s *Server) releaseVolumeWatchLease(
	lease activeconnections.Lease,
	teamID string,
) {
	if lease == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), volumeWatchLeaseReleaseTimeout)
	defer cancel()
	if err := lease.Release(ctx); err != nil && s != nil && s.logger != nil {
		s.logger.WithError(err).WithField("team_id", teamID).Error("Volume watch Team Quota lease release failed")
	}
}

func volumeWatchLeaseLossError(lease activeconnections.Lease) error {
	if lease == nil {
		return fmt.Errorf("active connection lease is nil")
	}
	select {
	case <-lease.Done():
		return lease.Err()
	default:
		return nil
	}
}
