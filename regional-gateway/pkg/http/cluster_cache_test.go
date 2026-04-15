package http

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetClusterGatewayURLForClusterRefreshesCacheWithSystemToken(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             internalauth.ServiceScheduler,
		PublicKey:          publicKey,
		AllowedCallers:     []string{internalauth.ServiceRegionalGateway},
		ClockSkewTolerance: 5 * time.Second,
	})

	scheduler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, err := validator.Validate(r.Header.Get(internalauth.DefaultTokenHeader))
		if err != nil {
			t.Fatalf("validate scheduler token: %v", err)
		}
		if !claims.IsSystem {
			t.Fatalf("expected system token, got claims: %+v", claims)
		}
		if got := r.Header.Get(internalauth.TeamIDHeader); got != "" {
			t.Fatalf("team header = %q, want empty", got)
		}
		if got := r.Header.Get(internalauth.UserIDHeader); got != "" {
			t.Fatalf("user header = %q, want empty", got)
		}
		if err := spec.WriteSuccess(w, http.StatusOK, schedulerClusterListResponse{
			Clusters: []schedulerCluster{{
				ClusterID:         "cluster-a",
				ClusterGatewayURL: "http://cluster-a:18080",
				Enabled:           true,
			}},
			Count: 1,
		}); err != nil {
			t.Fatalf("write response: %v", err)
		}
	}))
	defer scheduler.Close()

	server := &Server{
		cfg: &config.RegionalGatewayConfig{
			SchedulerURL:    scheduler.URL,
			ClusterCacheTTL: metav1.Duration{Duration: -time.Second},
			ProxyTimeout:    metav1.Duration{Duration: time.Second},
		},
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		clusterCache: make(map[string]string),
		logger:       zap.NewNop(),
	}

	got, err := server.getClusterGatewayURLForCluster(context.Background(), "cluster-a", nil)
	if err != nil {
		t.Fatalf("getClusterGatewayURLForCluster() error = %v", err)
	}
	if got != "http://cluster-a:18080" {
		t.Fatalf("cluster gateway url = %q, want %q", got, "http://cluster-a:18080")
	}
	if cached := server.getClusterFromCache("cluster-a"); cached != got {
		t.Fatalf("cached cluster gateway url = %q, want %q", cached, got)
	}
}

func TestGenerateTemplateInternalTokenUsesSystemScopeForSystemAdminAPIKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	server := &Server{
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
	}

	token, err := server.generateTemplateInternalToken(&authn.AuthContext{
		AuthMethod:    authn.AuthMethodAPIKey,
		TeamID:        "team-1",
		UserID:        "admin-user",
		IsSystemAdmin: true,
		Permissions:   []string{authn.PermTemplateCreate},
	}, internalauth.ServiceScheduler)
	if err != nil {
		t.Fatalf("generateTemplateInternalToken: %v", err)
	}

	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    internalauth.ServiceScheduler,
		PublicKey: publicKey,
	}).Validate(token)
	if err != nil {
		t.Fatalf("validate token: %v", err)
	}
	if !claims.IsSystemToken() {
		t.Fatalf("expected system token, got team_id=%q", claims.TeamID)
	}
	if claims.TeamID != "" {
		t.Fatalf("TeamID = %q, want empty", claims.TeamID)
	}
	if claims.UserID != "admin-user" {
		t.Fatalf("UserID = %q, want admin-user", claims.UserID)
	}
}

func TestGenerateTemplateInternalTokenKeepsSelectedTeamForSystemAdminJWT(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	server := &Server{
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
	}

	token, err := server.generateTemplateInternalToken(&authn.AuthContext{
		AuthMethod:    authn.AuthMethodJWT,
		TeamID:        "team-1",
		UserID:        "admin-user",
		IsSystemAdmin: true,
		Permissions:   []string{authn.PermTemplateCreate},
	}, internalauth.ServiceScheduler)
	if err != nil {
		t.Fatalf("generateTemplateInternalToken: %v", err)
	}

	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    internalauth.ServiceScheduler,
		PublicKey: publicKey,
	}).Validate(token)
	if err != nil {
		t.Fatalf("validate token: %v", err)
	}
	if claims.IsSystemToken() {
		t.Fatalf("selected-team JWT should remain team scoped")
	}
	if claims.TeamID != "team-1" {
		t.Fatalf("TeamID = %q, want team-1", claims.TeamID)
	}
}

func TestGenerateInternalTokenKeepsSystemAdminAPIKeyTeamScopedByDefault(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	server := &Server{
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
	}

	token, err := server.generateInternalToken(&authn.AuthContext{
		AuthMethod:    authn.AuthMethodAPIKey,
		TeamID:        "team-1",
		UserID:        "admin-user",
		IsSystemAdmin: true,
		Permissions:   []string{authn.PermSandboxCreate},
	}, internalauth.ServiceClusterGateway)
	if err != nil {
		t.Fatalf("generateInternalToken: %v", err)
	}

	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    internalauth.ServiceClusterGateway,
		PublicKey: publicKey,
	}).Validate(token)
	if err != nil {
		t.Fatalf("validate token: %v", err)
	}
	if claims.IsSystemToken() {
		t.Fatalf("default internal token should remain team scoped")
	}
	if claims.TeamID != "team-1" {
		t.Fatalf("TeamID = %q, want team-1", claims.TeamID)
	}
}

func TestGenerateInternalTokenRejectsTeamlessNonAdmin(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	server := &Server{
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
	}

	_, err = server.generateInternalToken(&authn.AuthContext{
		AuthMethod: authn.AuthMethodJWT,
		UserID:     "user-1",
	}, internalauth.ServiceScheduler)
	if !errors.Is(err, errInternalTokenTeamIDRequired) {
		t.Fatalf("error = %v, want %v", err, errInternalTokenTeamIDRequired)
	}
}
