package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/sandbox0-ai/sandbox0/egress-broker/pkg/resolver"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

// Server serves egress-broker HTTP endpoints.
type Server struct {
	cfg      *config.EgressBrokerConfig
	logger   *zap.Logger
	server   *http.Server
	resolver *resolver.Service
}

// NewServer creates a new egress-broker HTTP server.
func NewServer(cfg *config.EgressBrokerConfig, logger *zap.Logger, bindingStore egressauth.BindingStore) *Server {
	if cfg == nil {
		cfg = &config.EgressBrokerConfig{}
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	mux := http.NewServeMux()
	s := &Server{
		cfg:      cfg,
		logger:   logger,
		resolver: resolver.NewService(cfg, bindingStore, logger),
		server: &http.Server{
			Addr:              ":" + formatHTTPPort(resolveHTTPPort(cfg)),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/api/v1/resolve", s.handleResolve)
	return s
}

// Start starts the HTTP server and stops it when ctx is canceled.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		return s.Shutdown(context.Background())
	case err := <-errCh:
		return err
	}
}

// Shutdown stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		_ = spec.WriteError(w, http.StatusMethodNotAllowed, spec.CodeBadRequest, "method not allowed")
		return
	}

	var req egressauth.ResolveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	if req.AuthRef == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "authRef is required")
		return
	}

	s.logger.Info("Resolve request received",
		zap.String("sandbox_id", req.SandboxID),
		zap.String("team_id", req.TeamID),
		zap.String("auth_ref", req.AuthRef),
		zap.String("destination", req.Destination),
		zap.String("protocol", req.Protocol),
	)
	resp, err := s.resolver.Resolve(r.Context(), &req)
	if err != nil {
		statusCode, code, message := mapResolveError(err)
		_ = spec.WriteError(w, statusCode, code, message)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, resp)
}

func resolveHTTPPort(cfg *config.EgressBrokerConfig) int {
	if cfg == nil || cfg.HTTPPort == 0 {
		return 8082
	}
	return cfg.HTTPPort
}

func mapResolveError(err error) (int, string, string) {
	if err == nil {
		return http.StatusOK, "", ""
	}
	if errors.Is(err, resolver.ErrAuthRefNotFound) {
		return http.StatusNotFound, spec.CodeNotFound, "authRef not found"
	}

	var unsupported *resolver.UnsupportedProviderError
	if errors.As(err, &unsupported) {
		return http.StatusConflict, spec.CodeConflict, err.Error()
	}
	return http.StatusInternalServerError, spec.CodeInternal, "resolve authRef failed"
}

func formatHTTPPort(port int) string {
	return strconv.Itoa(port)
}
