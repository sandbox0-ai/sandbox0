package http

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/naming"
	"go.uber.org/zap"
)

// NOTE: In multi-cluster mode, scheduler syncs templates via manager's
// /api/v1/templates endpoints. Keep legacy handlers for that path when
// TemplateStoreEnabled is false.

// listTemplates lists all templates.
func (s *Server) listTemplates(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.listTemplatesLegacy(c)
		return
	}
	s.templateHandler.ListTemplates(c)
}

// getTemplate gets a template by ID.
func (s *Server) getTemplate(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.getTemplateLegacy(c)
		return
	}
	s.templateHandler.GetTemplate(c)
}

// createTemplate creates a new template.
func (s *Server) createTemplate(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.createTemplateLegacy(c)
		return
	}
	s.templateHandler.CreateTemplate(c)
}

// updateTemplate updates an existing template.
func (s *Server) updateTemplate(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.updateTemplateLegacy(c)
		return
	}
	s.templateHandler.UpdateTemplate(c)
}

// deleteTemplate deletes a template.
func (s *Server) deleteTemplate(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.deleteTemplateLegacy(c)
		return
	}
	s.templateHandler.DeleteTemplate(c)
}

// WarmPoolRequest represents the request body for warming the pool.
type WarmPoolRequest struct {
	Count int32 `json:"count"`
}

// warmPool warms the pool for a template.
func (s *Server) warmPool(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.warmPoolLegacy(c)
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

	var req WarmPoolRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request: "+err.Error())
		return
	}

	scope := "team"
	teamID := claims.TeamID

	tpl, err := s.templateStore.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if err != nil {
		s.logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to warm pool")
		return
	}
	if tpl == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "template not found")
		return
	}

	if tpl.Spec.Pool.MinIdle < req.Count {
		tpl.Spec.Pool.MinIdle = req.Count
		if tpl.Spec.Pool.MaxIdle < req.Count {
			tpl.Spec.Pool.MaxIdle = req.Count
		}
		if err := s.templateStore.UpdateTemplate(c.Request.Context(), tpl); err != nil {
			s.logger.Error("Failed to update template pool settings", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to warm pool")
			return
		}
	}

	s.triggerTemplateReconcile()
	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "pool warming triggered"})
}

// Legacy handlers: apply templates directly to K8s CRDs (scheduler-managed mode).
func (s *Server) listTemplatesLegacy(c *gin.Context) {
	templates, err := s.templateService.ListTemplates(c.Request.Context())
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

func (s *Server) getTemplateLegacy(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	template, err := s.templateService.GetTemplate(c.Request.Context(), templateID)
	if err != nil {
		s.logger.Error("Failed to get template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get template")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, template)
}

func (s *Server) createTemplateLegacy(c *gin.Context) {
	var template v1alpha1.SandboxTemplate
	if err := c.ShouldBindJSON(&template); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request: "+err.Error())
		return
	}

	created, err := s.templateService.CreateTemplate(c.Request.Context(), &template)
	if err != nil {
		s.logger.Error("Failed to create template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create template")
		return
	}

	spec.JSONSuccess(c, http.StatusCreated, created)
}

func (s *Server) updateTemplateLegacy(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	var template v1alpha1.SandboxTemplate
	if err := c.ShouldBindJSON(&template); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request: "+err.Error())
		return
	}

	if template.Name != "" && template.Name != templateID {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id in path does not match body")
		return
	}
	template.Name = templateID

	updated, err := s.templateService.UpdateTemplate(c.Request.Context(), &template)
	if err != nil {
		s.logger.Error("Failed to update template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update template")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, updated)
}

func (s *Server) deleteTemplateLegacy(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	if err := s.templateService.DeleteTemplate(c.Request.Context(), templateID); err != nil {
		s.logger.Error("Failed to delete template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "template deleted"})
}

func (s *Server) warmPoolLegacy(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id is required")
		return
	}

	var req WarmPoolRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request: "+err.Error())
		return
	}

	if err := s.templateService.WarmPool(c.Request.Context(), templateID, req.Count); err != nil {
		s.logger.Error("Failed to warm pool", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to warm pool")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "pool warming triggered"})
}

func (s *Server) triggerTemplateReconcile() {
	if s.templateReconciler == nil {
		return
	}
	go s.templateReconciler.TriggerReconcile(context.Background())
}
