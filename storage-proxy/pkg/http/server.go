package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/db"
	"github.com/sirupsen/logrus"
)

// Server provides HTTP management API for health checks and metrics
type Server struct {
	logger *logrus.Logger
	mux    *http.ServeMux
	repo   *db.Repository
}

// NewServer creates a new HTTP server
func NewServer(logger *logrus.Logger, repo *db.Repository) *Server {
	s := &Server{
		logger: logger,
		mux:    http.NewServeMux(),
		repo:   repo,
	}

	// Register handlers
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/ready", s.handleReady)
	s.mux.Handle("/metrics", promhttp.Handler())

	// Sandbox Volume handlers
	s.mux.HandleFunc("POST /sandboxvolumes", s.createSandboxVolume)
	s.mux.HandleFunc("GET /sandboxvolumes", s.listSandboxVolumes)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}", s.getSandboxVolume)

	return s
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// handleHealth handles health check requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
	})
}

// handleReady handles readiness check requests
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
	})
}
