package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/scheduler/pkg/db"
	"go.uber.org/zap"
)

// ClusterRequest represents the request body for creating/updating a cluster
type ClusterRequest struct {
	ClusterID          string `json:"cluster_id"`
	InternalGatewayURL string `json:"internal_gateway_url"`
	Weight             int    `json:"weight"`
	Enabled            bool   `json:"enabled"`
}

// listClusters lists all clusters
func (s *Server) listClusters(c *gin.Context) {
	enabledOnly := c.Query("enabled") == "true"

	var clusters []*db.Cluster
	var err error

	if enabledOnly {
		clusters, err = s.repo.ListEnabledClusters(c.Request.Context())
	} else {
		clusters, err = s.repo.ListClusters(c.Request.Context())
	}

	if err != nil {
		s.logger.Error("Failed to list clusters", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to list clusters",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"clusters": clusters,
		"count":    len(clusters),
	})
}

// getCluster gets a cluster by ID
func (s *Server) getCluster(c *gin.Context) {
	clusterID := c.Param("id")
	if clusterID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cluster_id is required"})
		return
	}

	cluster, err := s.repo.GetCluster(c.Request.Context(), clusterID)
	if err != nil {
		s.logger.Error("Failed to get cluster", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to get cluster",
		})
		return
	}

	if cluster == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "cluster not found"})
		return
	}

	c.JSON(http.StatusOK, cluster)
}

// createCluster creates a new cluster
func (s *Server) createCluster(c *gin.Context) {
	var req ClusterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}

	if req.ClusterID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cluster_id is required"})
		return
	}

	if req.InternalGatewayURL == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "internal_gateway_url is required"})
		return
	}

	// Set default weight if not provided
	if req.Weight <= 0 {
		req.Weight = 100
	}

	// Check if cluster already exists
	existing, err := s.repo.GetCluster(c.Request.Context(), req.ClusterID)
	if err != nil {
		s.logger.Error("Failed to check existing cluster", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create cluster"})
		return
	}
	if existing != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "cluster already exists"})
		return
	}

	cluster := &db.Cluster{
		ClusterID:          req.ClusterID,
		InternalGatewayURL: req.InternalGatewayURL,
		Weight:             req.Weight,
		Enabled:            req.Enabled,
	}

	if err := s.repo.CreateCluster(c.Request.Context(), cluster); err != nil {
		s.logger.Error("Failed to create cluster", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to create cluster",
		})
		return
	}

	s.logger.Info("Cluster created",
		zap.String("cluster_id", req.ClusterID),
		zap.String("internal_gateway_url", req.InternalGatewayURL),
		zap.Int("weight", req.Weight),
		zap.Bool("enabled", req.Enabled),
	)

	// Get the created cluster to return with timestamps
	created, _ := s.repo.GetCluster(c.Request.Context(), req.ClusterID)
	if created != nil {
		c.JSON(http.StatusCreated, created)
	} else {
		c.JSON(http.StatusCreated, cluster)
	}
}

// updateCluster updates an existing cluster
func (s *Server) updateCluster(c *gin.Context) {
	clusterID := c.Param("id")
	if clusterID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cluster_id is required"})
		return
	}

	var req ClusterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
		return
	}

	if req.InternalGatewayURL == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "internal_gateway_url is required"})
		return
	}

	// Check if cluster exists
	existing, err := s.repo.GetCluster(c.Request.Context(), clusterID)
	if err != nil {
		s.logger.Error("Failed to get cluster", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update cluster"})
		return
	}
	if existing == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "cluster not found"})
		return
	}

	// Set default weight if not provided
	if req.Weight <= 0 {
		req.Weight = existing.Weight
	}

	cluster := &db.Cluster{
		ClusterID:          clusterID,
		InternalGatewayURL: req.InternalGatewayURL,
		Weight:             req.Weight,
		Enabled:            req.Enabled,
	}

	if err := s.repo.UpdateCluster(c.Request.Context(), cluster); err != nil {
		s.logger.Error("Failed to update cluster", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to update cluster",
		})
		return
	}

	s.logger.Info("Cluster updated",
		zap.String("cluster_id", clusterID),
		zap.String("internal_gateway_url", req.InternalGatewayURL),
		zap.Int("weight", req.Weight),
		zap.Bool("enabled", req.Enabled),
	)

	// Get the updated cluster to return with timestamps
	updated, _ := s.repo.GetCluster(c.Request.Context(), clusterID)
	if updated != nil {
		c.JSON(http.StatusOK, updated)
	} else {
		c.JSON(http.StatusOK, cluster)
	}
}

// deleteCluster deletes a cluster
func (s *Server) deleteCluster(c *gin.Context) {
	clusterID := c.Param("id")
	if clusterID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cluster_id is required"})
		return
	}

	// Check if cluster exists
	existing, err := s.repo.GetCluster(c.Request.Context(), clusterID)
	if err != nil {
		s.logger.Error("Failed to get cluster", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete cluster"})
		return
	}
	if existing == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "cluster not found"})
		return
	}

	// Note: Allocations will be cascade deleted due to foreign key constraint

	if err := s.repo.DeleteCluster(c.Request.Context(), clusterID); err != nil {
		s.logger.Error("Failed to delete cluster", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to delete cluster",
		})
		return
	}

	s.logger.Info("Cluster deleted", zap.String("cluster_id", clusterID))

	c.JSON(http.StatusOK, gin.H{"message": "cluster deleted"})
}
