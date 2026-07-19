package http

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/schedulerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
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
		if err := spec.WriteSuccess(w, http.StatusOK, schedulerapi.ListClustersResponse{
			Clusters: []schedulerapi.Cluster{{
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

func TestGenerateInternalTokenUsesSelectedTeamForPlatformAPIKey(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "regional-gateway", PrivateKey: privateKey, TTL: time.Minute})}

	token, err := server.generateInternalToken(&authn.AuthContext{
		AuthMethod:    authn.AuthMethodAPIKey,
		TeamID:        "team-1",
		UserID:        "user-1",
		APIKeyID:      "key-1",
		IsSystemAdmin: true,
		Permissions:   []string{"*"},
	}, "scheduler")
	if err != nil {
		t.Fatalf("generateInternalToken: %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{Target: "scheduler", PublicKey: publicKey}).Validate(token)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if claims.IsSystemToken() {
		t.Fatalf("expected team-scoped token, got system token")
	}
	if claims.TeamID != "team-1" {
		t.Fatalf("TeamID = %q, want team-1", claims.TeamID)
	}
	if len(claims.Permissions) != 1 || claims.Permissions[0] != "*" {
		t.Fatalf("Permissions = %v, want [*]", claims.Permissions)
	}
	if claims.Audit == nil || claims.Audit.Actor.Kind != string(authn.PrincipalKindAPIKey) || claims.Audit.Actor.ID != "key-1" || claims.Audit.Actor.APIKeyID != "key-1" {
		t.Fatalf("Audit actor = %#v, want API key key-1", claims.Audit)
	}
	if claims.Audit.OperationID == "" || claims.Audit.RequestID == "" {
		t.Fatalf("Audit correlation = %#v, want generated IDs", claims.Audit)
	}
}

func TestGenerateInternalTokenUsesSystemTokenWithoutSelectedTeam(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "regional-gateway", PrivateKey: privateKey, TTL: time.Minute})}

	token, err := server.generateInternalToken(&authn.AuthContext{
		AuthMethod:    authn.AuthMethodAPIKey,
		UserID:        "user-1",
		APIKeyID:      "key-1",
		IsSystemAdmin: true,
		Permissions:   []string{"*"},
	}, "scheduler")
	if err != nil {
		t.Fatalf("generateInternalToken: %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{Target: "scheduler", PublicKey: publicKey}).Validate(token)
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

func TestGenerateForwardingInternalTokenSignsOnlyAdmittedKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		teamQuotaController: newAllowingTeamQuotaController(zap.NewNop()),
	}
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxes?source=public",
		nil,
	)
	if err := gatewayteamquota.RecordAdmittedKeys(
		ginCtx,
		coreteamquota.KeyNetworkEgressBytes,
		coreteamquota.KeyAPIRequests,
	); err != nil {
		t.Fatalf("RecordAdmittedKeys() error = %v", err)
	}
	authCtx := &authn.AuthContext{
		AuthMethod:  authn.AuthMethodAPIKey,
		TeamID:      "team-a",
		UserID:      "user-a",
		OperationID: "operation-a",
		RequestID:   "request-a",
	}
	token, err := server.generateForwardingInternalToken(
		authCtx,
		internalauth.ServiceScheduler,
		ginCtx.Request,
	)
	if err != nil {
		t.Fatalf("generateForwardingInternalToken() error = %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    internalauth.ServiceScheduler,
		PublicKey: publicKey,
	}).Validate(token)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if claims.QuotaAdmissionProof == nil {
		t.Fatal("forwarding token omitted quota admission proof")
	}
	wantKeys := []coreteamquota.Key{
		coreteamquota.KeyAPIRequests,
		coreteamquota.KeyNetworkEgressBytes,
	}
	if !reflect.DeepEqual(
		claims.QuotaAdmissionProof.Keys,
		wantKeys,
	) {
		t.Fatalf(
			"keys = %v, want %v",
			claims.QuotaAdmissionProof.Keys,
			wantKeys,
		)
	}
	changedQuery := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxes?source=scheduler",
		nil,
	)
	if claims.QuotaAdmissionProof.MatchesRequest(claims, changedQuery) {
		t.Fatal("query-only rewrite unexpectedly retained forwarding proof")
	}
}

func TestGenerateForwardingInternalTokenFailsClosedWhenProofVersionIsUnavailable(
	t *testing.T,
) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	controller := gatewayteamquota.NewController(
		nil,
		nil,
		nil,
		nil,
		zap.NewNop(),
		gatewayteamquota.WithAdmissionProofConsumer(failingProofVersionConsumer{}),
	)
	server := &Server{
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		teamQuotaController: controller,
		logger:              zap.NewNop(),
	}
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", nil)
	if err := gatewayteamquota.RecordAdmittedKeys(
		ginCtx,
		coreteamquota.KeyAPIRequests,
	); err != nil {
		t.Fatalf("RecordAdmittedKeys() error = %v", err)
	}
	authCtx := &authn.AuthContext{
		TeamID:      "team-a",
		OperationID: "operation-a",
		RequestID:   "request-a",
	}
	_, err = server.generateForwardingInternalToken(
		authCtx,
		internalauth.ServiceScheduler,
		ginCtx.Request,
	)
	if !coreteamquota.IsUnavailable(err) {
		t.Fatalf("generateForwardingInternalToken() error = %v, want unavailable", err)
	}
	server.abortForwardingTokenError(ginCtx, internalauth.ServiceScheduler, err)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
}

type failingProofVersionConsumer struct{}

func (failingProofVersionConsumer) CurrentVersion(
	context.Context,
) (guard.Version, error) {
	return guard.Version{}, errors.New("policy guard unavailable")
}

func (failingProofVersionConsumer) Consume(
	context.Context,
	string,
	string,
	int64,
	int64,
	guard.Version,
) (bool, error) {
	return false, nil
}

func (failingProofVersionConsumer) Close() error { return nil }

func TestGenerateInternalTokenDoesNotInventQuotaAdmissionProof(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	server := &Server{
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
	}
	token, err := server.generateInternalToken(
		&authn.AuthContext{
			AuthMethod: authn.AuthMethodInternal,
			TeamID:     "team-a",
		},
		internalauth.ServiceScheduler,
	)
	if err != nil {
		t.Fatalf("generateInternalToken() error = %v", err)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:    internalauth.ServiceScheduler,
		PublicKey: publicKey,
	}).Validate(token)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if claims.QuotaAdmissionProof != nil {
		t.Fatalf("background token invented proof: %+v", claims.QuotaAdmissionProof)
	}
}
