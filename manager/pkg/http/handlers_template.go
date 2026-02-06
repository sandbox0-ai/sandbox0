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

// TemplateRequest represents the request body for updating a template.
type TemplateRequest struct {
	TemplateName *string                      `json:"template_name,omitempty"`
	Public       *bool                        `json:"public,omitempty"`
	Spec         v1alpha1.SandboxTemplateSpec `json:"spec"`
}

// listTemplates lists all templates.
func (s *Server) listTemplates(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.listTemplatesLegacy(c)
		return
	}
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	templates, err := s.templateStore.ListVisibleTemplates(c.Request.Context(), claims.TeamID)
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

// getTemplate gets a template by ID.
func (s *Server) getTemplate(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.getTemplateLegacy(c)
		return
	}
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

// createTemplate creates a new template.
func (s *Server) createTemplate(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.createTemplateLegacy(c)
		return
	}
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

	tpl := &template.Template{
		TemplateID:   templateID,
		TemplateName: req.TemplateName,
		Scope:        scope,
		TeamID:       teamID,
		UserID:       claims.UserID,
		Spec:         req.Spec,
	}

	if err := s.templateStore.CreateTemplate(c.Request.Context(), tpl); err != nil {
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

	s.triggerTemplateReconcile()

	created, _ := s.templateStore.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if created != nil {
		spec.JSONSuccess(c, http.StatusCreated, created)
	} else {
		spec.JSONSuccess(c, http.StatusCreated, tpl)
	}
}

// updateTemplate updates an existing template.
func (s *Server) updateTemplate(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.updateTemplateLegacy(c)
		return
	}
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

	tpl := &template.Template{
		TemplateID:   templateID,
		TemplateName: *req.TemplateName,
		Scope:        scope,
		TeamID:       teamID,
		UserID:       claims.UserID,
		Spec:         req.Spec,
	}

	if err := s.templateStore.UpdateTemplate(c.Request.Context(), tpl); err != nil {
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

	s.triggerTemplateReconcile()

	updated, _ := s.templateStore.GetTemplate(c.Request.Context(), scope, teamID, templateID)
	if updated != nil {
		spec.JSONSuccess(c, http.StatusOK, updated)
	} else {
		spec.JSONSuccess(c, http.StatusOK, tpl)
	}
}

// deleteTemplate deletes a template.
func (s *Server) deleteTemplate(c *gin.Context) {
	if !s.templateStoreEnabled {
		s.deleteTemplateLegacy(c)
		return
	}
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

	if err := s.templateStore.DeleteTemplate(c.Request.Context(), scope, teamID, templateID); err != nil {
		s.logger.Error("Failed to delete template", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete template")
		return
	}

	s.logger.Info("Template deleted from database",
		zap.String("template_id", templateID),
		zap.String("scope", scope),
		zap.String("team_id", teamID),
	)

	s.triggerTemplateReconcile()

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "template deleted"})
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

	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

	var req WarmPoolRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request: "+err.Error())
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

	if scope == "team" && teamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required for private templates")
		return
	}

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

func (s *Server) triggerTemplateReconcile() {
	if s.templateReconciler == nil {
		return
	}
	go s.templateReconciler.TriggerReconcile(context.Background())
}

// Legacy handlers: apply templates directly to K8s CRDs (scheduler-managed mode).
func (s *Server) listTemplatesLegacy(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "missing authentication")
		return
	}

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
	var tpl v1alpha1.SandboxTemplate
	if err := c.ShouldBindJSON(&tpl); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request: "+err.Error())
		return
	}

	created, err := s.templateService.CreateTemplate(c.Request.Context(), &tpl)
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

	var tpl v1alpha1.SandboxTemplate
	if err := c.ShouldBindJSON(&tpl); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request: "+err.Error())
		return
	}

	if tpl.Name != "" && tpl.Name != templateID {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "template_id in path does not match body")
		return
	}
	tpl.Name = templateID

	updated, err := s.templateService.UpdateTemplate(c.Request.Context(), &tpl)
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
