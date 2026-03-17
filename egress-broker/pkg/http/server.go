package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

// Server serves egress-broker HTTP endpoints.
type Server struct {
	cfg          *config.EgressBrokerConfig
	logger       *zap.Logger
	server       *http.Server
	authMap      map[string]config.StaticEgressAuthConfig
	bindingStore egressauth.BindingStore
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
		cfg:          cfg,
		logger:       logger,
		authMap:      buildStaticAuthMap(cfg),
		bindingStore: bindingStore,
		server: &http.Server{
			Addr:              fmt.Sprintf(":%d", resolveHTTPPort(cfg)),
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
	resp, statusCode, code, message := s.resolveRequest(r.Context(), &req)
	if statusCode != http.StatusOK {
		_ = spec.WriteError(w, statusCode, code, message)
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, resp)
}

func (s *Server) resolveRequest(ctx context.Context, req *egressauth.ResolveRequest) (*egressauth.ResolveResponse, int, string, string) {
	if binding := s.lookupBinding(ctx, req); binding != nil {
		resp, err := s.responseFromBinding(binding)
		if err != nil {
			s.logger.Warn("Unsupported credential binding provider",
				zap.String("sandbox_id", req.SandboxID),
				zap.String("auth_ref", req.AuthRef),
				zap.String("provider", binding.Provider),
				zap.Error(err),
			)
			return nil, http.StatusConflict, spec.CodeConflict, err.Error()
		}
		return resp, http.StatusOK, "", ""
	}

	entry, ok := s.lookupStaticAuth(req.AuthRef)
	if !ok {
		return nil, http.StatusNotFound, spec.CodeNotFound, "authRef not found"
	}
	expiresAt := time.Now().UTC().Add(entry.TTL.Duration)
	return &egressauth.ResolveResponse{
		AuthRef:   entry.AuthRef,
		Headers:   cloneHeaders(entry.Headers),
		ExpiresAt: &expiresAt,
	}, http.StatusOK, "", ""
}

func (s *Server) lookupBinding(ctx context.Context, req *egressauth.ResolveRequest) *egressauth.CredentialBinding {
	if s == nil || s.bindingStore == nil || req == nil {
		return nil
	}
	clusterID := strings.TrimSpace(s.cfg.ClusterID)
	if clusterID == "" || strings.TrimSpace(req.SandboxID) == "" {
		return nil
	}
	record, err := s.bindingStore.GetBindings(ctx, clusterID, req.SandboxID)
	if err != nil {
		s.logger.Warn("Failed to load credential bindings",
			zap.String("cluster_id", clusterID),
			zap.String("sandbox_id", req.SandboxID),
			zap.Error(err),
		)
		return nil
	}
	if record == nil {
		return nil
	}
	authRef := strings.TrimSpace(req.AuthRef)
	for idx := range record.Bindings {
		if strings.TrimSpace(record.Bindings[idx].Ref) == authRef {
			return &record.Bindings[idx]
		}
	}
	return nil
}

func (s *Server) responseFromBinding(binding *egressauth.CredentialBinding) (*egressauth.ResolveResponse, error) {
	if binding == nil {
		return nil, fmt.Errorf("credential binding is required")
	}
	provider := strings.TrimSpace(strings.ToLower(binding.Provider))
	switch provider {
	case "static":
		expiresAt := time.Now().UTC().Add(s.cfg.DefaultResolveTTL.Duration)
		return &egressauth.ResolveResponse{
			AuthRef:   binding.Ref,
			Headers:   cloneHeaders(binding.Headers),
			ExpiresAt: &expiresAt,
		}, nil
	default:
		return nil, fmt.Errorf("credential binding provider %q is not supported", binding.Provider)
	}
}

func resolveHTTPPort(cfg *config.EgressBrokerConfig) int {
	if cfg == nil || cfg.HTTPPort == 0 {
		return 8082
	}
	return cfg.HTTPPort
}

func buildStaticAuthMap(cfg *config.EgressBrokerConfig) map[string]config.StaticEgressAuthConfig {
	if cfg == nil || len(cfg.StaticAuth) == 0 {
		return nil
	}
	out := make(map[string]config.StaticEgressAuthConfig, len(cfg.StaticAuth))
	for _, entry := range cfg.StaticAuth {
		authRef := strings.TrimSpace(entry.AuthRef)
		if authRef == "" {
			continue
		}
		out[authRef] = entry
	}
	return out
}

func (s *Server) lookupStaticAuth(authRef string) (config.StaticEgressAuthConfig, bool) {
	if s == nil || len(s.authMap) == 0 {
		return config.StaticEgressAuthConfig{}, false
	}
	entry, ok := s.authMap[strings.TrimSpace(authRef)]
	return entry, ok
}

func cloneHeaders(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
