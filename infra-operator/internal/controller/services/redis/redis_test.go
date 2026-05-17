package redis

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
)

func TestApplyGatewayRateLimitConfigUsesMemoryWithoutRedis(t *testing.T) {
	client := newTestClient(t)
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
	}
	cfg := &apiconfig.GatewayConfig{}

	if err := ApplyGatewayRateLimitConfig(context.Background(), client, infra, cfg); err != nil {
		t.Fatalf("ApplyGatewayRateLimitConfig() error = %v", err)
	}
	if cfg.RateLimitBackend != ratelimit.BackendMemory {
		t.Fatalf("RateLimitBackend = %q, want memory", cfg.RateLimitBackend)
	}
	if cfg.RateLimitRedisURL != "" {
		t.Fatalf("RateLimitRedisURL = %q, want empty", cfg.RateLimitRedisURL)
	}
}

func TestApplyGatewayRateLimitConfigUsesBuiltinRedis(t *testing.T) {
	client := newTestClient(t)
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Redis: &infrav1alpha1.RedisConfig{
				Type:             infrav1alpha1.RedisTypeBuiltin,
				KeyPrefix:        "sandbox0:ratelimit:test",
				OperationTimeout: metav1.Duration{Duration: 250 * time.Millisecond},
			},
		},
	}
	cfg := &apiconfig.GatewayConfig{}

	if err := ApplyGatewayRateLimitConfig(context.Background(), client, infra, cfg); err != nil {
		t.Fatalf("ApplyGatewayRateLimitConfig() error = %v", err)
	}
	if cfg.RateLimitBackend != ratelimit.BackendRedis {
		t.Fatalf("RateLimitBackend = %q, want redis", cfg.RateLimitBackend)
	}
	if cfg.RateLimitRedisURL != "redis://demo-redis.sandbox0-system.svc:6379/0" {
		t.Fatalf("RateLimitRedisURL = %q", cfg.RateLimitRedisURL)
	}
	if cfg.RateLimitRedisKeyPrefix != "sandbox0:ratelimit:test" {
		t.Fatalf("RateLimitRedisKeyPrefix = %q", cfg.RateLimitRedisKeyPrefix)
	}
	if cfg.RateLimitRedisTimeout.Duration != 250*time.Millisecond {
		t.Fatalf("RateLimitRedisTimeout = %s", cfg.RateLimitRedisTimeout.Duration)
	}
	if !cfg.RateLimitFailOpen {
		t.Fatal("RateLimitFailOpen = false, want true by default")
	}
}

func TestApplyGatewayRateLimitConfigUsesExternalRedisURLSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-url", Namespace: "sandbox0-system"},
		Data: map[string][]byte{
			"url": []byte("rediss://:password@redis.example:6379/0"),
		},
	}
	client := newTestClient(t, secret)
	failOpen := false
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Redis: &infrav1alpha1.RedisConfig{
				Type: infrav1alpha1.RedisTypeExternal,
				External: &infrav1alpha1.ExternalRedisConfig{
					URLSecret: infrav1alpha1.RedisURLSecretRef{Name: "redis-url"},
				},
				FailOpen: &failOpen,
			},
		},
	}
	cfg := &apiconfig.GatewayConfig{}

	if err := ApplyGatewayRateLimitConfig(context.Background(), client, infra, cfg); err != nil {
		t.Fatalf("ApplyGatewayRateLimitConfig() error = %v", err)
	}
	if cfg.RateLimitBackend != ratelimit.BackendRedis {
		t.Fatalf("RateLimitBackend = %q, want redis", cfg.RateLimitBackend)
	}
	if cfg.RateLimitRedisURL != "rediss://:password@redis.example:6379/0" {
		t.Fatalf("RateLimitRedisURL = %q", cfg.RateLimitRedisURL)
	}
	if cfg.RateLimitRedisKeyPrefix != ratelimit.DefaultRedisKeyPrefix {
		t.Fatalf("RateLimitRedisKeyPrefix = %q", cfg.RateLimitRedisKeyPrefix)
	}
	if cfg.RateLimitRedisTimeout.Duration != ratelimit.DefaultRedisTimeout {
		t.Fatalf("RateLimitRedisTimeout = %s", cfg.RateLimitRedisTimeout.Duration)
	}
	if cfg.RateLimitFailOpen {
		t.Fatal("RateLimitFailOpen = true, want false")
	}
}

func newTestClient(t *testing.T, objects ...runtime.Object) ctrlclient.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
}
