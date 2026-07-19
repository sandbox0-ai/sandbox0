package proxy

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
)

func (s *Server) admitNetworkOperation(
	ctx context.Context,
	compiled *policy.CompiledPolicy,
	transport string,
) bool {
	if s == nil {
		return false
	}
	if compiled == nil || strings.TrimSpace(compiled.TeamID) == "" {
		s.metrics.RecordNetworkOperationAdmission(transport, "unavailable")
		networkQuotaLogger(s).Warn(
			"Network operation rejected because resolved team policy is missing",
			zap.String("transport", transport),
		)
		return false
	}
	if s.teamNetworkQuota == nil {
		s.metrics.RecordNetworkOperationAdmission(transport, "unavailable")
		networkQuotaLogger(s).Error(
			"Network operation rejected because Team Quota is unavailable",
			zap.String("transport", transport),
			zap.String("team_id", compiled.TeamID),
		)
		return false
	}
	err := s.teamNetworkQuota.takeOperation(ctx, compiled.TeamID)
	if err == nil {
		s.metrics.RecordNetworkOperationAdmission(transport, "allowed")
		return true
	}
	outcome := "unavailable"
	if teamquota.IsRateExceeded(err) {
		outcome = "denied"
	}
	s.metrics.RecordNetworkOperationAdmission(transport, outcome)
	networkQuotaLogger(s).Info(
		"Network operation rejected by Team Quota",
		zap.String("transport", transport),
		zap.String("team_id", compiled.TeamID),
		zap.String("quota_key", string(teamquota.KeyNetworkOperations)),
		zap.Error(fmt.Errorf("admit network operation: %w", err)),
	)
	return false
}

func networkQuotaLogger(s *Server) *zap.Logger {
	if s == nil || s.logger == nil {
		return zap.NewNop()
	}
	return s.logger
}
