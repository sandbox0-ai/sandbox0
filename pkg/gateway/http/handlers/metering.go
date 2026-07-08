package handlers

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"go.uber.org/zap"
)

// MeteringReader defines the read-only metering contract shared by gateway surfaces.
type MeteringReader interface {
	GetStatus(ctx context.Context, fallbackRegionID string) (*metering.Status, error)
	ListEvents(ctx context.Context, cursor string, limit int) ([]*metering.Event, string, error)
	ListWindows(ctx context.Context, cursor string, limit int) ([]*metering.Window, string, error)
}

// MeteringHandler serves region-scoped metering export endpoints.
type MeteringHandler struct {
	repo     MeteringReader
	regionID string
	logger   *zap.Logger
}

// NewMeteringHandler creates a metering export handler.
func NewMeteringHandler(repo MeteringReader, regionID string, logger *zap.Logger) *MeteringHandler {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &MeteringHandler{
		repo:     repo,
		regionID: strings.TrimSpace(regionID),
		logger:   logger,
	}
}

// GetStatus returns the export stream status for the current region.
func (h *MeteringHandler) GetStatus(c *gin.Context) {
	if h.repo == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "metering is unavailable")
		return
	}

	status, err := h.repo.GetStatus(c.Request.Context(), h.regionID)
	if err != nil {
		h.logger.Error("Failed to load metering status", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to load metering status")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, status)
}

// ListEvents returns raw usage events after the requested cursor.
func (h *MeteringHandler) ListEvents(c *gin.Context) {
	if h.repo == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "metering is unavailable")
		return
	}

	cursor, limit, ok := parseMeteringCursor(c)
	if !ok {
		return
	}

	events, nextCursor, err := h.repo.ListEvents(c.Request.Context(), cursor, limit)
	if err != nil {
		h.logger.Error("Failed to list metering events", zap.Error(err))
		if strings.Contains(err.Error(), "invalid cursor") {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid cursor")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list metering events")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"events":      events,
		"next_cursor": nextCursor,
	})
}

// ListWindows returns derived usage windows after the requested cursor.
func (h *MeteringHandler) ListWindows(c *gin.Context) {
	if h.repo == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "metering is unavailable")
		return
	}

	cursor, limit, ok := parseMeteringCursor(c)
	if !ok {
		return
	}

	windows, nextCursor, err := h.repo.ListWindows(c.Request.Context(), cursor, limit)
	if err != nil {
		h.logger.Error("Failed to list metering windows", zap.Error(err))
		if strings.Contains(err.Error(), "invalid cursor") {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid cursor")
			return
		}
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list metering windows")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"windows":     windows,
		"next_cursor": nextCursor,
	})
}

func parseMeteringCursor(c *gin.Context) (string, int, bool) {
	cursor := strings.TrimSpace(c.Query("cursor"))
	limit := 100
	if value := c.Query("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid limit")
			return "", 0, false
		}
		if parsed > 1000 {
			parsed = 1000
		}
		limit = parsed
	}

	return cursor, limit, true
}
