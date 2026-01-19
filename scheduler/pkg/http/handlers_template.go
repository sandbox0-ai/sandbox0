package http

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/scheduler/pkg/db"
	"go.uber.org/zap"
)

// TemplateRequest represents the request body for creating/updating a template
type TemplateRequest struct {
	Namespace string                       `json:"namespace"`
	Spec      v1alpha1.SandboxTemplateSpec `json:"spec"`
}

// listTemplates lists all templates
func (s *Server) listTemplates(c *gin.Context) {
	namespace := c.Query("namespace")

	var templates []*db.Template
	var err error

	if namespace != "" {
		templates, err = s.repo.ListTemplatesByNamespace(c.Request.Context(), namespace)
	} else {
		templates, err = s.repo.ListTemplates(c.Request.Context())
	}

	if err != nil {
		s.logger.Error("Failed to list templates", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to list templates",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"templates": templates,
		"count":     len(templates),
	})
}

// getTemplate gets a template by ID
func (s *Server) getTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template_id is required"})
		return
	}

	namespace := c.Query("namespace")
	if namespace == "" {
		namespace = "sandbox0" // Default namespace
	}

	template, err := s.repo.GetTemplate(c.Request.Context(), templateID, namespace)
	if err != nil {
		s.logger.Error("Failed to get template", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to get template",
		})
		return
	}

	if template == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "template not found"})
		return
	}

	c.JSON(http.StatusOK, template)
}

// createTemplate creates a new template
func (s *Server) createTemplate(c *gin.Context) {
	var req struct {
		Name      string                       `json:"name"`
		Namespace string                       `json:"namespace"`
		Spec      v1alpha1.SandboxTemplateSpec `json:"spec"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}

	if req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}

	if req.Namespace == "" {
		req.Namespace = "sandbox0" // Default namespace
	}

	// Check if template already exists
	existing, err := s.repo.GetTemplate(c.Request.Context(), req.Name, req.Namespace)
	if err != nil {
		s.logger.Error("Failed to check existing template", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create template"})
		return
	}
	if existing != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "template already exists"})
		return
	}

	template := &db.Template{
		TemplateID: req.Name,
		Namespace:  req.Namespace,
		Spec:       req.Spec,
	}

	if err := s.repo.CreateTemplate(c.Request.Context(), template); err != nil {
		s.logger.Error("Failed to create template", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to create template",
		})
		return
	}

	s.logger.Info("Template created",
		zap.String("template_id", req.Name),
		zap.String("namespace", req.Namespace),
	)

	// Trigger immediate reconciliation to sync to clusters
	if s.reconciler != nil {
		go s.reconciler.TriggerReconcile(context.Background())
	}

	// Get the created template to return with timestamps
	created, _ := s.repo.GetTemplate(c.Request.Context(), req.Name, req.Namespace)
	if created != nil {
		c.JSON(http.StatusCreated, created)
	} else {
		c.JSON(http.StatusCreated, template)
	}
}

// updateTemplate updates an existing template
func (s *Server) updateTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template_id is required"})
		return
	}

	var req TemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}

	if req.Namespace == "" {
		req.Namespace = "sandbox0" // Default namespace
	}

	// Check if template exists
	existing, err := s.repo.GetTemplate(c.Request.Context(), templateID, req.Namespace)
	if err != nil {
		s.logger.Error("Failed to get template", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update template"})
		return
	}
	if existing == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "template not found"})
		return
	}

	template := &db.Template{
		TemplateID: templateID,
		Namespace:  req.Namespace,
		Spec:       req.Spec,
	}

	if err := s.repo.UpdateTemplate(c.Request.Context(), template); err != nil {
		s.logger.Error("Failed to update template", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to update template",
		})
		return
	}

	s.logger.Info("Template updated",
		zap.String("template_id", templateID),
		zap.String("namespace", req.Namespace),
	)

	// Trigger immediate reconciliation to sync changes to clusters
	if s.reconciler != nil {
		go s.reconciler.TriggerReconcile(context.Background())
	}

	// Get the updated template to return with timestamps
	updated, _ := s.repo.GetTemplate(c.Request.Context(), templateID, req.Namespace)
	if updated != nil {
		c.JSON(http.StatusOK, updated)
	} else {
		c.JSON(http.StatusOK, template)
	}
}

// deleteTemplate deletes a template
func (s *Server) deleteTemplate(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template_id is required"})
		return
	}

	namespace := c.Query("namespace")
	if namespace == "" {
		namespace = "sandbox0" // Default namespace
	}

	// Check if template exists
	existing, err := s.repo.GetTemplate(c.Request.Context(), templateID, namespace)
	if err != nil {
		s.logger.Error("Failed to get template", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete template"})
		return
	}
	if existing == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "template not found"})
		return
	}

	// Get all allocations to clean up from clusters
	allocations, err := s.repo.ListAllocationsByTemplate(c.Request.Context(), templateID, namespace)
	if err != nil {
		s.logger.Error("Failed to get template allocations", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete template"})
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
	if err := s.repo.DeleteAllocationsByTemplate(c.Request.Context(), templateID, namespace); err != nil {
		s.logger.Error("Failed to delete template allocations", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete template"})
		return
	}

	// Delete template from database
	if err := s.repo.DeleteTemplate(c.Request.Context(), templateID, namespace); err != nil {
		s.logger.Error("Failed to delete template", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to delete template",
		})
		return
	}

	s.logger.Info("Template deleted from database",
		zap.String("template_id", templateID),
		zap.String("namespace", namespace),
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
	c.JSON(http.StatusOK, response)
}

// getTemplateAllocations gets the allocations for a template
func (s *Server) getTemplateAllocations(c *gin.Context) {
	templateID := c.Param("id")
	if templateID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template_id is required"})
		return
	}

	namespace := c.Query("namespace")
	if namespace == "" {
		namespace = "sandbox0" // Default namespace
	}

	allocations, err := s.repo.ListAllocationsByTemplate(c.Request.Context(), templateID, namespace)
	if err != nil {
		s.logger.Error("Failed to get template allocations", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to get template allocations",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"allocations": allocations,
		"count":       len(allocations),
	})
}
