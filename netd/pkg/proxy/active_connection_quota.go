package proxy

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/activeconnections"
	"go.uber.org/zap"
)

const activeConnectionLeaseReleaseTimeout = 2 * time.Second

func (s *Server) acquireActiveConnectionLease(
	ctx context.Context,
	compiled *policy.CompiledPolicy,
) (activeconnections.Lease, error) {
	if s == nil || s.activeConnections == nil {
		return nil, &teamquota.UnavailableError{
			Operation: "acquire active network connection quota",
			Err:       fmt.Errorf("active connection quota is not configured"),
		}
	}
	if ctx == nil {
		return nil, &teamquota.UnavailableError{
			Operation: "acquire active network connection quota",
			Err:       fmt.Errorf("context is required"),
		}
	}
	teamID := ""
	if compiled != nil {
		teamID = strings.TrimSpace(compiled.TeamID)
	}
	if teamID == "" {
		return nil, &teamquota.UnavailableError{
			Operation: "acquire active network connection quota",
			Err:       fmt.Errorf("resolved policy team_id is required"),
		}
	}
	return s.activeConnections.Acquire(ctx, teamID)
}

func (s *Server) releaseActiveConnectionLease(
	lease activeconnections.Lease,
	transport string,
	compiled *policy.CompiledPolicy,
) {
	if lease == nil {
		return
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		activeConnectionLeaseReleaseTimeout,
	)
	defer cancel()
	if err := lease.Release(ctx); err != nil {
		s.logActiveConnectionQuota(
			"Network Team Quota lease release failed",
			transport,
			compiled,
			err,
		)
	}
}

func (s *Server) logActiveConnectionQuota(
	message string,
	transport string,
	compiled *policy.CompiledPolicy,
	err error,
) {
	fields := []zap.Field{
		zap.String("transport", transport),
		zap.Error(err),
	}
	if compiled != nil {
		fields = append(
			fields,
			zap.String("team_id", compiled.TeamID),
			zap.String("sandbox_id", compiled.SandboxID),
		)
	}
	if teamquota.IsConcurrencyExceeded(err) {
		s.logger.Info(message, fields...)
		return
	}
	s.logger.Error(message, fields...)
}

func leaseLost(lease activeconnections.Lease) bool {
	if lease == nil {
		return false
	}
	select {
	case <-lease.Done():
		return lease.Err() != nil
	default:
		return false
	}
}

func (s *Server) closeTCPConnectionOnLeaseLoss(
	ctx context.Context,
	lease activeconnections.Lease,
	conn net.Conn,
	compiled *policy.CompiledPolicy,
	cancel context.CancelFunc,
) {
	if lease == nil || conn == nil {
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-lease.Done():
	}
	if err := lease.Err(); err != nil {
		s.logActiveConnectionQuota(
			"TCP connection Team Quota lease lost",
			"tcp",
			compiled,
			err,
		)
		if cancel != nil {
			cancel()
		}
		_ = conn.Close()
	}
}
