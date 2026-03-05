package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/scheduler/pkg/db"
	"go.uber.org/zap"
)

// ClusterRequest represents the request body for creating/updating a cluster
type ClusterRequest struct {
	ClusterName        string `json:"cluster_name"`
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
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list clusters")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"clusters": clusters,
		"count":    len(clusters),
	})
}

// getCluster gets a cluster by ID
func (s *Server) getCluster(c *gin.Context) {
	clusterID := c.Param("id")
	if clusterID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "cluster_id is required")
		return
	}

	cluster, err := s.repo.GetCluster(c.Request.Context(), clusterID)
	if err != nil {
		s.logger.Error("Failed to get cluster", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get cluster")
		return
	}

	if cluster == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "cluster not found")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, cluster)
}

// createCluster creates a new cluster
func (s *Server) createCluster(c *gin.Context) {
	var req ClusterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body: "+err.Error())
		return
	}

	if err := naming.ValidateClusterName(req.ClusterName); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	if req.InternalGatewayURL == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "internal_gateway_url is required")
		return
	}

	// Set default weight if not provided
	if req.Weight <= 0 {
		req.Weight = 100
	}

	clusterID, err := naming.ClusterIDFromName(req.ClusterName)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	// Check if cluster already exists
	existing, err := s.repo.GetCluster(c.Request.Context(), clusterID)
	if err != nil {
		s.logger.Error("Failed to check existing cluster", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create cluster")
		return
	}
	if existing != nil {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "cluster already exists")
		return
	}

	cluster := &db.Cluster{
		ClusterID:          clusterID,
		ClusterName:        req.ClusterName,
		InternalGatewayURL: req.InternalGatewayURL,
		Weight:             req.Weight,
		Enabled:            req.Enabled,
	}

	if err := s.repo.CreateCluster(c.Request.Context(), cluster); err != nil {
		s.logger.Error("Failed to create cluster", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create cluster")
		return
	}

	s.logger.Info("Cluster created",
		zap.String("cluster_id", clusterID),
		zap.String("cluster_name", req.ClusterName),
		zap.String("internal_gateway_url", req.InternalGatewayURL),
		zap.Int("weight", req.Weight),
		zap.Bool("enabled", req.Enabled),
	)

	// Get the created cluster to return with timestamps
	created, _ := s.repo.GetCluster(c.Request.Context(), clusterID)
	if created != nil {
		spec.JSONSuccess(c, http.StatusCreated, created)
	} else {
		spec.JSONSuccess(c, http.StatusCreated, cluster)
	}
}

// updateCluster updates an existing cluster
func (s *Server) updateCluster(c *gin.Context) {
	clusterID := c.Param("id")
	if clusterID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "cluster_id is required")
		return
	}

	var req ClusterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.InternalGatewayURL == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "internal_gateway_url is required")
		return
	}

	// Check if cluster exists
	existing, err := s.repo.GetCluster(c.Request.Context(), clusterID)
	if err != nil {
		s.logger.Error("Failed to get cluster", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update cluster")
		return
	}
	if existing == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "cluster not found")
		return
	}

	// Set default weight if not provided
	if req.Weight <= 0 {
		req.Weight = existing.Weight
	}
	if req.ClusterName == "" {
		req.ClusterName = existing.ClusterName
	}
	if err := naming.ValidateClusterName(req.ClusterName); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	cluster := &db.Cluster{
		ClusterID:          clusterID,
		ClusterName:        req.ClusterName,
		InternalGatewayURL: req.InternalGatewayURL,
		Weight:             req.Weight,
		Enabled:            req.Enabled,
	}

	if err := s.repo.UpdateCluster(c.Request.Context(), cluster); err != nil {
		s.logger.Error("Failed to update cluster", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update cluster")
		return
	}

	s.logger.Info("Cluster updated",
		zap.String("cluster_id", clusterID),
		zap.String("cluster_name", req.ClusterName),
		zap.String("internal_gateway_url", req.InternalGatewayURL),
		zap.Int("weight", req.Weight),
		zap.Bool("enabled", req.Enabled),
	)

	// Get the updated cluster to return with timestamps
	updated, _ := s.repo.GetCluster(c.Request.Context(), clusterID)
	if updated != nil {
		spec.JSONSuccess(c, http.StatusOK, updated)
	} else {
		spec.JSONSuccess(c, http.StatusOK, cluster)
	}
}

// deleteCluster deletes a cluster
func (s *Server) deleteCluster(c *gin.Context) {
	clusterID := c.Param("id")
	if clusterID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "cluster_id is required")
		return
	}

	// Check if cluster exists
	existing, err := s.repo.GetCluster(c.Request.Context(), clusterID)
	if err != nil {
		s.logger.Error("Failed to get cluster", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete cluster")
		return
	}
	if existing == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "cluster not found")
		return
	}

	// Note: Allocations will be cascade deleted due to foreign key constraint

	if err := s.repo.DeleteCluster(c.Request.Context(), clusterID); err != nil {
		s.logger.Error("Failed to delete cluster", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete cluster")
		return
	}

	s.logger.Info("Cluster deleted", zap.String("cluster_id", clusterID))

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "cluster deleted"})
}
