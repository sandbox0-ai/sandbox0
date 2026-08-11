package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

func TestGenerateManagerTokenUsesSystemTokenForTeamlessSystemAdmin(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute})}

	token, err := server.generateManagerToken(&authn.AuthContext{IsSystemAdmin: true}, nil, []string{authn.PermTemplateCreate})
	if err != nil {
		t.Fatalf("generateManagerToken: %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{Target: "manager", PublicKey: publicKey}).Validate(token)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !claims.IsSystemToken() {
		t.Fatalf("expected system token, got team_id=%q", claims.TeamID)
	}
	if claims.TeamID != "" {
		t.Fatalf("TeamID = %q, want empty", claims.TeamID)
	}
}

func TestGenerateManagerTokenUsesSystemTokenForPlatformAPIKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute})}

	token, err := server.generateManagerToken(&authn.AuthContext{
		AuthMethod:    authn.AuthMethodAPIKey,
		TeamID:        "team-1",
		UserID:        "user-1",
		APIKeyID:      "key-1",
		IsSystemAdmin: true,
		Permissions:   []string{"*"},
	}, nil, []string{authn.PermTemplateCreate})
	if err != nil {
		t.Fatalf("generateManagerToken: %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{Target: "manager", PublicKey: publicKey}).Validate(token)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !claims.IsSystemToken() {
		t.Fatalf("expected system token, got team_id=%q", claims.TeamID)
	}
	if claims.TeamID != "" {
		t.Fatalf("TeamID = %q, want empty", claims.TeamID)
	}
	if len(claims.Permissions) != 1 || claims.Permissions[0] != authn.PermTemplateCreate {
		t.Fatalf("Permissions = %v, want [%s]", claims.Permissions, authn.PermTemplateCreate)
	}
}

func TestProxyInternalSystemPauseRequestUsesSystemTokenForJWTSystemAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	var receivedToken string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		receivedToken = request.Header.Get(internalauth.DefaultTokenHeader)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"data":{"requested":1}}`))
	}))
	defer target.Close()
	managerRouter, err := proxy.NewRouter(target.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	server := &Server{
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceClusterGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		proxy2Mgr: managerRouter,
		logger:    zap.NewNop(),
	}
	router := gin.New()
	router.POST("/internal/v1/teams/:team_id/pause-running-sandboxes", func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod:    authn.AuthMethodJWT,
			TeamID:        "system",
			IsSystemAdmin: true,
		})
		server.proxyInternalSystemPauseRequest(c)
	})

	request := httptest.NewRequest(
		http.MethodPost,
		"/internal/v1/teams/11111111-1111-4111-8111-111111111111/pause-running-sandboxes",
		nil,
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    internalauth.ServiceManager,
		PublicKey: publicKey,
	}).Validate(receivedToken)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !claims.IsSystemToken() {
		t.Fatalf("expected system token, got team_id=%q", claims.TeamID)
	}
}
