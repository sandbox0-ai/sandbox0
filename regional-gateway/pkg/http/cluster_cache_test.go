package http

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
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
