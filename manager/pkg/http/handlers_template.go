package http

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	templatehttp "github.com/sandbox0-ai/sandbox0/pkg/template/http"
	"go.uber.org/zap"
)

// NOTE: Scheduler syncs templates via manager's internal API.

// listTemplates lists all templates.
func (s *Server) listTemplates(c *gin.Context) {
	if s.templateHandler == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "template store is disabled")
		return
	}
	s.templateHandler.ListTemplates(c)
}

// getTemplate gets a template by ID.
func (s *Server) getTemplate(c *gin.Context) {
	if s.templateHandler == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "template store is disabled")
		return
	}
	s.templateHandler.GetTemplate(c)
}

// createTemplate creates a new template.
func (s *Server) createTemplate(c *gin.Context) {
	if s.templateHandler == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "template store is disabled")
		return
	}
	s.templateHandler.CreateTemplate(c)
}

// updateTemplate updates an existing template.
func (s *Server) updateTemplate(c *gin.Context) {
	if s.templateHandler == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "template store is disabled")
		return
	}
	s.templateHandler.UpdateTemplate(c)
}

// deleteTemplate deletes a template.
func (s *Server) deleteTemplate(c *gin.Context) {
	if s.templateHandler == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "template store is disabled")
		return
	}
	s.templateHandler.DeleteTemplate(c)
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

type clusterTemplateStatsProvider struct {
	clusterService *service.ClusterService
}

func (p *clusterTemplateStatsProvider) GetTemplateStats(ctx context.Context) (*templatehttp.TemplateStats, error) {
	if p == nil || p.clusterService == nil {
		return nil, nil
	}

	stats, err := p.clusterService.GetTemplateStats(ctx)
	if err != nil {
		return nil, err
	}
	if stats == nil || len(stats.Templates) == 0 {
		return &templatehttp.TemplateStats{Templates: nil}, nil
	}

	templates := make([]templatehttp.TemplateStat, 0, len(stats.Templates))
	for _, stat := range stats.Templates {
		templates = append(templates, templatehttp.TemplateStat{
			TemplateID:  stat.TemplateID,
			Namespace:   stat.Namespace,
			IdleCount:   stat.IdleCount,
			ActiveCount: stat.ActiveCount,
		})
	}
	return &templatehttp.TemplateStats{Templates: templates}, nil
}
