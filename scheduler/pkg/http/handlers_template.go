package http

import (
	"context"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"github.com/sandbox0-ai/infra/pkg/template"
	"go.uber.org/zap"
)

// TemplateRequest represents the request body for updating a template
type TemplateRequest struct {
	TemplateName *string                      `json:"template_name,omitempty"`
	Public       *bool                        `json:"public,omitempty"`
	Spec         v1alpha1.SandboxTemplateSpec `json:"spec"`
}

// listTemplates lists all templates
func (s *Server) listTemplates(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	var templates []*template.Template
	var err error

	templates, err = s.templateStore.ListVisibleTemplates(c.Request.Context(), claims.TeamID)

	if err != nil {
		s.logger.Error("Failed to list templates", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list templates")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"templates": templates,
		"count":     len(templates),
	})
}

// getTemplate gets a template by ID
func (s *Server) getTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	// Optional: force public template
	if v := c.Query("public"); v != "" {
		public, err := strconv.ParseBool(v)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid public query param")
			return
		}
		if public {
			template, err := s.templateStore.GetTemplate(c.Request.Context(), "public", "", templateID)
			if err != nil {
				s.logger.Error("Failed to get template", zap.Error(err))
				spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get template")
				return
			}
			if template == nil {
				spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
				return
			}
			spec.JSONSuccess(c, http.StatusOK, template)
			return
		}
	}

	template, err := s.templateStore.GetTemplateForTeam(c.Request.Context(), claims.TeamID, templateID)
	if err != nil {
		s.logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get template")
		return
	}

	if template == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, template)
}

// createTemplate creates a new template
func (s *Server) createTemplate(c *gin.Context) {
	var req struct {
		TemplateName string                       `json:"template_name"`
		Public       bool                         `json:"public,omitempty"`
		Spec         v1alpha1.SandboxTemplateSpec `json:"spec"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body: "+err.Error())
		return
	}

	if err := naming.ValidateTemplateName(req.TemplateName); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	scope := "team"
	teamID := claims.TeamID
	if req.Public {
		if !internalauth.HasPermission(c.Request.Context(), "*") {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "system-admin required for public templates")
			return
		}
		scope = "public"
		teamID = ""
	} else if teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for private templates")
		return
	}

	templateID, err := naming.TemplateIDFromName(req.TemplateName)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	// Check if template already exists
	existing, err := s.templateStore.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		s.logger.Error("Failed to check existing template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}
	if existing != nil {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "template already exists")
		return
	}

	template := &template.Template{
		TemplateID:   templateID,
		TemplateName: req.TemplateName,
		Scope:        scope,
		TeamID:       teamID,
		UserID:       claims.UserID,
		Spec:         req.Spec,
	}

	if err := s.templateStore.CreateTemplate(c.Request.Context(), template); err != nil {
		s.logger.Error("Failed to create template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}

	s.logger.Info("Template created",
		zap.String("template_id", templateID),
		zap.String("template_name", req.TemplateName),
		zap.String("scope", scope),
		zap.String("team_id", teamID),
	)

	// Trigger immediate reconciliation to sync to clusters
	if s.reconciler != nil {
		go s.reconciler.TriggerReconcile(context.Background())
	}

	// Get the created template to return with timestamps
	created, _ := s.templateStore.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if created != nil {
		spec.JSONSuccess(c, http.StatusCreated, created)
	} else {
		spec.JSONSuccess(c, http.StatusCreated, template)
	}
}

// updateTemplate updates an existing template
func (s *Server) updateTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	var req TemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body: "+err.Error())
		return
	}

	scope := "team"
	teamID := claims.TeamID
	if req.Public != nil && *req.Public {
		if !internalauth.HasPermission(c.Request.Context(), "*") {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "system-admin required for public templates")
			return
		}
		scope = "public"
		teamID = ""
	} else if teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for private templates")
		return
	}

	// Check if template exists
	existing, err := s.templateStore.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		s.logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update template")
		return
	}
	if existing == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}

	if req.TemplateName == nil || *req.TemplateName == "" {
		req.TemplateName = &existing.TemplateName
	}
	if err := naming.ValidateTemplateName(*req.TemplateName); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	template := &template.Template{
		TemplateID:   templateID,
		TemplateName: *req.TemplateName,
		Scope:        scope,
		TeamID:       teamID,
		UserID:       claims.UserID,
		Spec:         req.Spec,
	}

	if err := s.templateStore.UpdateTemplate(c.Request.Context(), template); err != nil {
		s.logger.Error("Failed to update template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update template")
		return
	}

	s.logger.Info("Template updated",
		zap.String("template_id", templateID),
		zap.String("template_name", *req.TemplateName),
		zap.String("scope", scope),
		zap.String("team_id", teamID),
	)

	// Trigger immediate reconciliation to sync changes to clusters
	if s.reconciler != nil {
		go s.reconciler.TriggerReconcile(context.Background())
	}

	// Get the updated template to return with timestamps
	updated, _ := s.templateStore.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if updated != nil {
		spec.JSONSuccess(c, http.StatusOK, updated)
	} else {
		spec.JSONSuccess(c, http.StatusOK, template)
	}
}

// deleteTemplate deletes a template
func (s *Server) deleteTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	public := false
	if v := c.Query("public"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid public query param")
			return
		}
		public = b
	}

	scope := "team"
	teamID := claims.TeamID
	if public {
		if !internalauth.HasPermission(c.Request.Context(), "*") {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "system-admin required for public templates")
			return
		}
		scope = "public"
		teamID = ""
	} else if teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for private templates")
		return
	}

	// Check if template exists
	existing, err := s.templateStore.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		s.logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
		return
	}
	if existing == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}

	// Get all allocations to clean up from clusters
	allocations, err := s.allocationStore.ListAllocationsByTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		s.logger.Error("Failed to get template allocations", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
		return
	}

	// First, delete template from all clusters (best-effort)
	// We log failures but continue with database deletion
	var cleanupErrors []string
	for _, alloc := range allocations {
		cluster, err := s.repo.GetCluster(c.Request.Context(), alloc.ClusterID)
		if err != nil {
			s.logger.Warn("Failed to get cluster info for cleanup",
				zap.String("cluster_id", alloc.ClusterID),
				zap.Error(err),
			)
			cleanupErrors = append(cleanupErrors, alloc.ClusterID+": failed to get cluster info")
			continue
		}
		if cluster == nil {
			s.logger.Warn("Cluster not found for cleanup",
				zap.String("cluster_id", alloc.ClusterID),
			)
			continue
		}

		// Note: DeleteTemplate needs to be added to server struct (via dependency injection)
		// For now, we'll trigger reconcile which will handle orphan cleanup
		s.logger.Info("Template will be cleaned from cluster via reconcile",
			zap.String("cluster_id", alloc.ClusterID),
			zap.String("template_id", templateID),
		)
	}

	// Delete allocations from database
	if err := s.allocationStore.DeleteAllocationsByTemplate(c.Request.Context(), scope, teamID, templateID); err != nil {
		s.logger.Error("Failed to delete template allocations", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
		return
	}

	// Delete template from database
	if err := s.templateStore.DeleteTemplate(c.Request.Context(), scope, teamID, templateID); err != nil {
		s.logger.Error("Failed to delete template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
		return
	}

	s.logger.Info("Template deleted from database",
		zap.String("template_id", templateID),
		zap.String("scope", scope),
		zap.String("team_id", teamID),
		zap.Int("affected_clusters", len(allocations)),
	)

	// Trigger immediate reconciliation to clean up clusters
	// Reconcile will detect orphaned templates and remove them
	if s.reconciler != nil {
		go s.reconciler.TriggerReconcile(context.Background())
	}

	response := gin.H{"message": "template deleted"}
	if len(cleanupErrors) > 0 {
		response["cleanup_warnings"] = cleanupErrors
	}
	spec.JSONSuccess(c, http.StatusOK, response)
}

// getTemplateAllocations gets the allocations for a template
func (s *Server) getTemplateAllocations(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	scope := "team"
	teamID := claims.TeamID
	if v := c.Query("public"); v != "" {
		public, err := strconv.ParseBool(v)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid public query param")
			return
		}
		if public {
			scope = "public"
			teamID = ""
		}
	}

	// Default behavior: private-only for allocations unless public=true is explicitly requested.
	if scope == "team" && teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for private templates")
		return
	}

	allocations, err := s.allocationStore.ListAllocationsByTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		s.logger.Error("Failed to get template allocations", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get template allocations")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"allocations": allocations,
		"count":       len(allocations),
	})
}
