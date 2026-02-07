package http

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"github.com/sandbox0-ai/infra/pkg/template"
	"github.com/sandbox0-ai/infra/pkg/template/store"
	"go.uber.org/zap"
)

// ClusterStore provides cluster lookup for delete warnings.
type ClusterStore interface {
	GetCluster(ctx context.Context, clusterID string) (*template.Cluster, error)
}

// Reconciler triggers template reconciliation.
type Reconciler interface {
	TriggerReconcile(ctx context.Context)
}

// Handler provides template HTTP handlers backed by a template store.
type Handler struct {
	Store           store.TemplateStore
	AllocationStore store.AllocationStore
	ClusterStore    ClusterStore
	Reconciler      Reconciler
	Logger          *zap.Logger
}

// TemplateRequest represents the request body for updating a template.
type TemplateRequest struct {
	Spec         v1alpha1.SandboxTemplateSpec `json:"spec"`
}

// ListTemplates lists all templates.
func (h *Handler) ListTemplates(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	if claims.TeamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for custom templates")
		return
	}

	templates, err := h.Store.ListVisibleTemplates(c.Request.Context(), claims.TeamID)
	if err != nil {
		h.Logger.Error("Failed to list templates", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list templates")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"templates": templates,
		"count":     len(templates),
	})
}

// GetTemplate gets a template by ID.
func (h *Handler) GetTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}
	canonicalTemplateID, err := naming.CanonicalTemplateID(templateID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	templateID = canonicalTemplateID

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	if claims.TeamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for custom templates")
		return
	}

	tpl, err := h.Store.GetTemplateForTeam(c.Request.Context(), claims.TeamID, templateID)
	if err != nil {
		h.Logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get template")
		return
	}

	if tpl == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, tpl)
}

// CreateTemplate creates a new template.
func (h *Handler) CreateTemplate(c *gin.Context) {
	var req struct {
		TemplateID string                       `json:"template_id"`
		Spec         v1alpha1.SandboxTemplateSpec `json:"spec"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body: "+err.Error())
		return
	}

	canonicalTemplateID, err := naming.CanonicalTemplateID(req.TemplateID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	req.TemplateID = canonicalTemplateID

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	if claims.TeamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for custom templates")
		return
	}

	scope := "team"
	teamID := claims.TeamID
	templateID := req.TemplateID

	existing, err := h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		h.Logger.Error("Failed to check existing template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}
	if existing != nil {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "template already exists")
		return
	}

	tpl := &template.Template{
		TemplateID: templateID,
		Scope:      scope,
		TeamID:     teamID,
		UserID:     claims.UserID,
		Spec:       req.Spec,
	}

	if err := h.Store.CreateTemplate(c.Request.Context(), tpl); err != nil {
		h.Logger.Error("Failed to create template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}

	h.Logger.Info("Template created",
		zap.String("template_id", templateID),
		zap.String("scope", scope),
		zap.String("team_id", teamID),
	)

	h.triggerReconcile()

	created, _ := h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if created != nil {
		spec.JSONSuccess(c, http.StatusCreated, created)
	} else {
		spec.JSONSuccess(c, http.StatusCreated, tpl)
	}
}

// UpdateTemplate updates an existing template.
func (h *Handler) UpdateTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}
	canonicalTemplateID, err := naming.CanonicalTemplateID(templateID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	templateID = canonicalTemplateID

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	if claims.TeamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for custom templates")
		return
	}

	var req TemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body: "+err.Error())
		return
	}

	scope := "team"
	teamID := claims.TeamID

	existing, err := h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		h.Logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update template")
		return
	}
	if existing == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}

	tpl := &template.Template{
		TemplateID: templateID,
		Scope:      scope,
		TeamID:     teamID,
		UserID:     claims.UserID,
		Spec:       req.Spec,
	}

	if err := h.Store.UpdateTemplate(c.Request.Context(), tpl); err != nil {
		h.Logger.Error("Failed to update template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update template")
		return
	}

	h.Logger.Info("Template updated",
		zap.String("template_id", templateID),
		zap.String("scope", scope),
		zap.String("team_id", teamID),
	)

	h.triggerReconcile()

	updated, _ := h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if updated != nil {
		spec.JSONSuccess(c, http.StatusOK, updated)
	} else {
		spec.JSONSuccess(c, http.StatusOK, tpl)
	}
}

// DeleteTemplate deletes a template.
func (h *Handler) DeleteTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}
	canonicalTemplateID, err := naming.CanonicalTemplateID(templateID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	templateID = canonicalTemplateID

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	if claims.TeamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for custom templates")
		return
	}

	scope := "team"
	teamID := claims.TeamID

	existing, err := h.Store.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		h.Logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
		return
	}
	if existing == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}

	var cleanupErrors []string
	if h.AllocationStore != nil {
		allocations, err := h.AllocationStore.ListAllocationsByTemplate(c.Request.Context(), scope, teamID, templateID)
		if err != nil {
			h.Logger.Error("Failed to get template allocations", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
			return
		}

		if h.ClusterStore != nil {
			for _, alloc := range allocations {
				cluster, err := h.ClusterStore.GetCluster(c.Request.Context(), alloc.ClusterID)
				if err != nil {
					h.Logger.Warn("Failed to get cluster info for cleanup",
						zap.String("cluster_id", alloc.ClusterID),
						zap.Error(err),
					)
					cleanupErrors = append(cleanupErrors, alloc.ClusterID+": failed to get cluster info")
					continue
				}
				if cluster == nil {
					h.Logger.Warn("Cluster not found for cleanup",
						zap.String("cluster_id", alloc.ClusterID),
					)
					continue
				}

				h.Logger.Info("Template will be cleaned from cluster via reconcile",
					zap.String("cluster_id", alloc.ClusterID),
					zap.String("template_id", templateID),
				)
			}
		}

		if err := h.AllocationStore.DeleteAllocationsByTemplate(c.Request.Context(), scope, teamID, templateID); err != nil {
			h.Logger.Error("Failed to delete template allocations", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
			return
		}
	}

	if err := h.Store.DeleteTemplate(c.Request.Context(), scope, teamID, templateID); err != nil {
		h.Logger.Error("Failed to delete template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
		return
	}

	h.Logger.Info("Template deleted from database",
		zap.String("template_id", templateID),
		zap.String("scope", scope),
		zap.String("team_id", teamID),
	)

	h.triggerReconcile()

	response := gin.H{"message": "template deleted"}
	if len(cleanupErrors) > 0 {
		response["cleanup_warnings"] = cleanupErrors
	}
	spec.JSONSuccess(c, http.StatusOK, response)
}

// GetTemplateAllocations gets allocations for a template.
func (h *Handler) GetTemplateAllocations(c *gin.Context) {
	if h.AllocationStore == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "allocations not supported")
		return
	}

	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}
	canonicalTemplateID, err := naming.CanonicalTemplateID(templateID)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	templateID = canonicalTemplateID

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}
	if claims.TeamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for custom templates")
		return
	}

	scope := "team"
	teamID := claims.TeamID

	allocations, err := h.AllocationStore.ListAllocationsByTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		h.Logger.Error("Failed to get template allocations", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get template allocations")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"allocations": allocations,
		"count":       len(allocations),
	})
}

func (h *Handler) triggerReconcile() {
	if h.Reconciler == nil {
		return
	}
	go h.Reconciler.TriggerReconcile(context.Background())
}
