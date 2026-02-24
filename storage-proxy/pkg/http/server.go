package http

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/auth"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/infra/storage-proxy/pkg/snapshot"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

// Server provides HTTP management API for health checks and metrics
type Server struct {
	logger        *logrus.Logger
	mux           *http.ServeMux
	repo          *db.Repository
	authenticator *auth.HTTPAuthenticator
	snapshotMgr   *snapshot.Manager
}

// NewServer creates a new HTTP server
func NewServer(logger *logrus.Logger, repo *db.Repository, authenticator *auth.HTTPAuthenticator, snapshotMgr *snapshot.Manager) *Server {
	s := &Server{
		logger:        logger,
		mux:           http.NewServeMux(),
		repo:          repo,
		authenticator: authenticator,
		snapshotMgr:   snapshotMgr,
	}

	// Register handlers
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/readyz", s.handleReady)
	s.mux.Handle("/metrics", promhttp.Handler())

	// Sandbox Volume handlers
	s.mux.HandleFunc("POST /sandboxvolumes", s.createSandboxVolume)
	s.mux.HandleFunc("GET /sandboxvolumes", s.listSandboxVolumes)
	s.mux.HandleFunc("GET /sandboxvolumes/{id}", s.getSandboxVolume)
	s.mux.HandleFunc("DELETE /sandboxvolumes/{id}", s.deleteSandboxVolume)
	s.mux.HandleFunc("POST /sandboxvolumes/{id}/fork", s.forkVolume)

	// Snapshot handlers
	s.mux.HandleFunc("POST /sandboxvolumes/{volume_id}/snapshots", s.createSnapshot)
	s.mux.HandleFunc("GET /sandboxvolumes/{volume_id}/snapshots", s.listSnapshots)
	s.mux.HandleFunc("GET /sandboxvolumes/{volume_id}/snapshots/{snapshot_id}", s.getSnapshot)
	s.mux.HandleFunc("POST /sandboxvolumes/{volume_id}/snapshots/{snapshot_id}/restore", s.restoreSnapshot)
	s.mux.HandleFunc("DELETE /sandboxvolumes/{volume_id}/snapshots/{snapshot_id}", s.deleteSnapshot)

	return s
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Skip logging for health check, readiness check and metrics
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
		s.serve(w, r)
		return
	}

	start := time.Now()
	wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

	s.serve(wrapped, r)

	fields := logrus.Fields{
		"method":   r.Method,
		"path":     r.URL.Path,
		"status":   wrapped.statusCode,
		"duration": time.Since(start),
		"remote":   r.RemoteAddr,
	}

	spanCtx := trace.SpanFromContext(r.Context()).SpanContext()
	if spanCtx.IsValid() {
		fields["trace_id"] = spanCtx.TraceID().String()
		fields["span_id"] = spanCtx.SpanID().String()
	}

	s.logger.WithFields(fields).Info("HTTP request")
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if s.authenticator != nil {
		s.authenticator.HealthCheckMiddleware(s.mux).ServeHTTP(w, r)
	} else {
		s.mux.ServeHTTP(w, r)
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// handleHealth handles health check requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
	})
}

// handleReady handles readiness check requests
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{
		"status":    "ready",
		"timestamp": time.Now().Unix(),
	})
}
