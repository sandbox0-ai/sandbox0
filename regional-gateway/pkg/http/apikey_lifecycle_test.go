package http

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"go.uber.org/zap"
)

func TestRegionalGatewayClosesOwnedAPIKeyRepository(t *testing.T) {
	writer := &lifecycleUsageWriter{}
	repository := lifecycleAPIKeyRepository(writer)
	server := &Server{
		apiKeyRepo:     repository,
		ownsAPIKeyRepo: true,
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

func TestRegionalGatewayConstructionFailureClosesOwnedAPIKeyRepository(t *testing.T) {
	writer := &lifecycleUsageWriter{}
	repository := lifecycleAPIKeyRepository(writer)
	_, err := NewServer(
		&config.RegionalGatewayConfig{
			AuthMode:         edgeAuthModeSelfHosted,
			SchedulerEnabled: true,
			SchedulerURL:     "http://scheduler.invalid",
		},
		nil,
		zap.NewNop(),
		nil,
		withAPIKeyRepositoryFactoryForTest(func(*pgxpool.Pool) *apikey.Repository {
			return repository
		}),
	)
	if err == nil || !strings.Contains(err.Error(), "license_file") {
		t.Fatalf("NewServer() error = %v, want license_file failure", err)
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

func lifecycleAPIKeyRepository(writer apikey.UsageBatchWriter) *apikey.Repository {
	now := time.Now().UTC()
	return apikey.NewRepository(
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
}

type lifecycleUsageWriter struct {
	mu    sync.Mutex
	total int64
}

func (w *lifecycleUsageWriter) WriteAPIKeyUsageBatch(
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

func (w *lifecycleUsageWriter) totalUsage() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.total
}
