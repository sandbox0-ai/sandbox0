package http

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"go.uber.org/zap"
)

func TestClusterGatewayClosesOwnedAPIKeyRepository(t *testing.T) {
	writer := &clusterLifecycleUsageWriter{}
	now := time.Now().UTC()
	repository := apikey.NewRepository(
		nil,
		apikey.WithAuthenticationLookup(func(context.Context, string) (*apikey.APIKey, error) {
			return &apikey.APIKey{
				ID:        "11111111-1111-4111-8111-111111111111",
				TeamID:    "team-1",
				CreatedBy: "user-1",
				Scope:     apikey.ScopeTeam,
				Roles:     []string{"viewer"},
				IsActive:  true,
				ExpiresAt: now.Add(time.Hour),
			}, nil
		}),
		apikey.WithUsageRecorderConfig(apikey.UsageRecorderConfig{
			FlushInterval: time.Hour,
			FlushTimeout:  time.Second,
			CloseTimeout:  2 * time.Second,
			QueueSize:     8,
			MaxPending:    8,
		}),
		apikey.WithUsageBatchWriter(writer),
	)
	server := &Server{
		publicAPIKeyRepo:     repository,
		ownsPublicAPIKeyRepo: true,
	}
	if _, err := repository.ValidateAPIKey(
		context.Background(),
		"s0_aws-us-east-1_"+strings.Repeat("a", 48),
	); err != nil {
		t.Fatalf("ValidateAPIKey() error = %v", err)
	}
	if err := server.closeAPIKeyRepository(); err != nil {
		t.Fatalf("first close error = %v", err)
	}
	if err := server.closeAPIKeyRepository(); err != nil {
		t.Fatalf("second close error = %v", err)
	}
	if total := writer.totalUsage(); total != 1 {
		t.Fatalf("flushed usage = %d, want 1", total)
	}
}

func TestClusterGatewayConstructionFailureClosesOwnedAPIKeyRepository(t *testing.T) {
	privateKeyPEM, _, err := internalauth.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("generate internal key: %v", err)
	}
	privateKeyPath := t.TempDir() + "/internal-private.pem"
	if err := os.WriteFile(privateKeyPath, privateKeyPEM, 0o600); err != nil {
		t.Fatalf("write internal private key: %v", err)
	}
	originalPrivateKeyPath := internalauth.DefaultInternalJWTPrivateKeyPath
	internalauth.DefaultInternalJWTPrivateKeyPath = privateKeyPath
	t.Cleanup(func() {
		internalauth.DefaultInternalJWTPrivateKeyPath = originalPrivateKeyPath
	})

	pool, err := pgxpool.New(
		context.Background(),
		"postgres://sandbox0:sandbox0@127.0.0.1:1/sandbox0?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)
	logger := zap.NewNop()
	obsProvider, err := observability.New(observability.Config{
		ServiceName:    "cluster-gateway-api-key-lifecycle-test",
		Logger:         logger,
		DisableTracing: true,
		DisableMetrics: true,
		DisableLogging: true,
	})
	if err != nil {
		t.Fatalf("create observability provider: %v", err)
	}
	t.Cleanup(func() { _ = obsProvider.Shutdown(context.Background()) })

	writer := &clusterLifecycleUsageWriter{}
	now := time.Now().UTC()
	repository := apikey.NewRepository(
		nil,
		apikey.WithAuthenticationLookup(func(context.Context, string) (*apikey.APIKey, error) {
			return &apikey.APIKey{
				ID:        "11111111-1111-4111-8111-111111111111",
				TeamID:    "team-1",
				CreatedBy: "user-1",
				Scope:     apikey.ScopeTeam,
				Roles:     []string{"viewer"},
				IsActive:  true,
				ExpiresAt: now.Add(time.Hour),
			}, nil
		}),
		apikey.WithUsageRecorderConfig(apikey.UsageRecorderConfig{
			FlushInterval: time.Hour,
			FlushTimeout:  time.Second,
			CloseTimeout:  2 * time.Second,
			QueueSize:     8,
			MaxPending:    8,
		}),
		apikey.WithUsageBatchWriter(writer),
	)
	_, err = NewServer(
		&config.ClusterGatewayConfig{
			AuthMode: authModePublic,
			GatewayConfig: config.GatewayConfig{
				JWTSecret: "test-secret",
			},
		},
		pool,
		logger,
		obsProvider,
		WithTeamQuotaController(&gatewayteamquota.Controller{}),
		withAPIKeyRepositoryFactoryForTest(func(*pgxpool.Pool) *apikey.Repository {
			return repository
		}),
	)
	if err == nil || !strings.Contains(err.Error(), "shared overload guard requires redis URL") {
		t.Fatalf("NewServer() error = %v, want overload guard failure", err)
	}
	if _, err := repository.ValidateAPIKey(
		context.Background(),
		"s0_aws-us-east-1_"+strings.Repeat("b", 48),
	); err != nil {
		t.Fatalf("ValidateAPIKey() after construction failure error = %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatalf("second repository Close() error = %v", err)
	}
	if total := writer.totalUsage(); total != 0 {
		t.Fatalf("usage recorded after owned repository was closed = %d, want 0", total)
	}
}

type clusterLifecycleUsageWriter struct {
	mu    sync.Mutex
	total int64
}

func (w *clusterLifecycleUsageWriter) WriteAPIKeyUsageBatch(
	_ context.Context,
	batch []apikey.APIKeyUsage,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, usage := range batch {
		w.total += usage.Count
	}
	return nil
}

func (w *clusterLifecycleUsageWriter) totalUsage() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.total
}
