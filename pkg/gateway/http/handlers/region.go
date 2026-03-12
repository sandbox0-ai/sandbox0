package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"go.uber.org/zap"
)

type regionRepository interface {
	ListRegions(ctx context.Context) ([]*tenantdir.Region, error)
	GetRegion(ctx context.Context, regionID string) (*tenantdir.Region, error)
	CreateRegion(ctx context.Context, region *tenantdir.Region) error
	UpdateRegion(ctx context.Context, region *tenantdir.Region) error
	DeleteRegion(ctx context.Context, regionID string) error
}

// RegionHandler manages the global region directory.
type RegionHandler struct {
	repo   regionRepository
	logger *zap.Logger
}

// CreateRegionRequest creates a new region directory entry.
type CreateRegionRequest struct {
	ID                string `json:"id" binding:"required"`
	DisplayName       string `json:"display_name"`
	EdgeGatewayURL    string `json:"edge_gateway_url" binding:"required"`
	MeteringExportURL string `json:"metering_export_url"`
	Enabled           *bool  `json:"enabled"`
}

// UpdateRegionRequest updates an existing region directory entry.
type UpdateRegionRequest struct {
	DisplayName       string  `json:"display_name"`
	EdgeGatewayURL    string  `json:"edge_gateway_url"`
	MeteringExportURL *string `json:"metering_export_url"`
	Enabled           *bool   `json:"enabled"`
}

// NewRegionHandler creates a new region handler.
func NewRegionHandler(repo regionRepository, logger *zap.Logger) *RegionHandler {
	return &RegionHandler{repo: repo, logger: logger}
}

// ListRegions lists all configured regions.
func (h *RegionHandler) ListRegions(c *gin.Context) {
	if !requireSystemAdmin(c) {
		return
	}

	regions, err := h.repo.ListRegions(c.Request.Context())
	if err != nil {
		h.logger.Error("Failed to list regions", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list regions")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"regions": regions})
}

// CreateRegion creates a new region.
func (h *RegionHandler) CreateRegion(c *gin.Context) {
	if !requireSystemAdmin(c) {
		return
	}

	var req CreateRegionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	region := &tenantdir.Region{
		ID:                strings.TrimSpace(req.ID),
		DisplayName:       strings.TrimSpace(req.DisplayName),
		EdgeGatewayURL:    strings.TrimSpace(req.EdgeGatewayURL),
		MeteringExportURL: strings.TrimSpace(req.MeteringExportURL),
		Enabled:           enabled,
	}
	if region.ID == "" || region.EdgeGatewayURL == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "region id and edge gateway url are required")
		return
	}

	if err := h.repo.CreateRegion(c.Request.Context(), region); err != nil {
		if errors.Is(err, tenantdir.ErrRegionAlreadyExists) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "region already exists")
			return
		}
		h.logger.Error("Failed to create region", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create region")
		return
	}
	spec.JSONSuccess(c, http.StatusCreated, region)
}

// GetRegion returns a region directory entry.
func (h *RegionHandler) GetRegion(c *gin.Context) {
	if !requireSystemAdmin(c) {
		return
	}

	region, err := h.repo.GetRegion(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, tenantdir.ErrRegionNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "region not found")
			return
		}
		h.logger.Error("Failed to get region", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get region")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, region)
}

// UpdateRegion updates a region directory entry.
func (h *RegionHandler) UpdateRegion(c *gin.Context) {
	if !requireSystemAdmin(c) {
		return
	}

	region, err := h.repo.GetRegion(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, tenantdir.ErrRegionNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "region not found")
			return
		}
		h.logger.Error("Failed to get region", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get region")
		return
	}

	var req UpdateRegionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if req.DisplayName != "" {
		region.DisplayName = strings.TrimSpace(req.DisplayName)
	}
	if req.EdgeGatewayURL != "" {
		region.EdgeGatewayURL = strings.TrimSpace(req.EdgeGatewayURL)
	}
	if req.MeteringExportURL != nil {
		region.MeteringExportURL = strings.TrimSpace(*req.MeteringExportURL)
	}
	if req.Enabled != nil {
		region.Enabled = *req.Enabled
	}
	if region.EdgeGatewayURL == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "edge gateway url is required")
		return
	}

	if err := h.repo.UpdateRegion(c.Request.Context(), region); err != nil {
		if errors.Is(err, tenantdir.ErrRegionNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "region not found")
			return
		}
		h.logger.Error("Failed to update region", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update region")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, region)
}

// DeleteRegion deletes a region directory entry.
func (h *RegionHandler) DeleteRegion(c *gin.Context) {
	if !requireSystemAdmin(c) {
		return
	}

	if err := h.repo.DeleteRegion(c.Request.Context(), c.Param("id")); err != nil {
		if errors.Is(err, tenantdir.ErrRegionNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "region not found")
			return
		}
		h.logger.Error("Failed to delete region", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete region")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "region deleted"})
}

func requireSystemAdmin(c *gin.Context) bool {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return false
	}
	if !authCtx.IsSystemAdmin {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "system admin access required")
		return false
	}
	return true
}
