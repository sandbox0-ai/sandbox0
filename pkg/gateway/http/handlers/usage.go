package handlers

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"go.uber.org/zap"
)

// UsageWindowReader defines the team-scoped usage query exposed publicly.
type UsageWindowReader interface {
	ListTeamWindows(ctx context.Context, teamID string, windowType string, cursor string, limit int) ([]*metering.Window, string, error)
}

// UsageWindow is the stable public representation of a metering window.
type UsageWindow struct {
	WindowID    string    `json:"window_id"`
	RegionID    string    `json:"region_id,omitempty"`
	ClusterID   string    `json:"cluster_id,omitempty"`
	WindowType  string    `json:"window_type"`
	SubjectType string    `json:"subject_type"`
	SubjectID   string    `json:"subject_id"`
	SandboxID   string    `json:"sandbox_id,omitempty"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Value       int64     `json:"value"`
	Unit        string    `json:"unit"`
	RecordedAt  time.Time `json:"recorded_at"`
}

// ListUsageWindows returns usage windows belonging to the authenticated team.
func (h *MeteringHandler) ListUsageWindows(c *gin.Context) {
	reader, ok := h.repo.(UsageWindowReader)
	if h.repo == nil || !ok {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "usage is unavailable")
		return
	}

	authCtx := authn.FromContext(c.Request.Context())
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	teamID := strings.TrimSpace(authCtx.TeamID)
	if teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
		return
	}

	cursor, limit, parsed := parseMeteringCursor(c)
	if !parsed {
		return
	}
	windowType := strings.TrimSpace(c.Query("window_type"))

	windows, nextCursor, err := reader.ListTeamWindows(c.Request.Context(), teamID, windowType, cursor, limit)
	if err != nil {
		h.logger.Error("Failed to list usage windows", zap.Error(err), zap.String("team_id", teamID))
		if strings.Contains(err.Error(), "invalid cursor") {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid cursor")
			return
		}
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "usage is unavailable")
		return
	}

	publicWindows := make([]UsageWindow, 0, len(windows))
	for _, window := range windows {
		if window == nil {
			continue
		}
		publicWindows = append(publicWindows, UsageWindow{
			WindowID:    window.WindowID,
			RegionID:    window.RegionID,
			ClusterID:   window.ClusterID,
			WindowType:  window.WindowType,
			SubjectType: window.SubjectType,
			SubjectID:   window.SubjectID,
			SandboxID:   window.SandboxID,
			WindowStart: window.WindowStart,
			WindowEnd:   window.WindowEnd,
			Value:       window.Value,
			Unit:        window.Unit,
			RecordedAt:  window.RecordedAt,
		})
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"windows":     publicWindows,
		"next_cursor": nextCursor,
	})
}
