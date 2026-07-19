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
)

func TestApplyTeamQuotaDistributedEnforcementConfigClearsRuntimeRedisWithoutRedis(t *testing.T) {
	client := newTestClient(t)
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
	}
	cfg := &apiconfig.TeamQuotaDistributedEnforcementConfig{
		RedisURL:       "redis://stale",
		RedisKeyPrefix: "stale",
		RedisTimeout:   metav1.Duration{Duration: time.Second},
	}

	if err := ApplyTeamQuotaDistributedEnforcementConfig(context.Background(), client, infra, cfg); err != nil {
		t.Fatalf("ApplyTeamQuotaDistributedEnforcementConfig() error = %v", err)
	}
	if cfg.RedisURL != "" || cfg.RedisKeyPrefix != "" || cfg.RedisTimeout.Duration != 0 {
		t.Fatalf("runtime Redis config = %#v, want empty", cfg)
	}
}

func TestApplyTeamQuotaDistributedEnforcementConfigUsesBuiltinRedis(t *testing.T) {
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
	cfg := &apiconfig.TeamQuotaDistributedEnforcementConfig{}

	if err := ApplyTeamQuotaDistributedEnforcementConfig(context.Background(), client, infra, cfg); err != nil {
		t.Fatalf("ApplyTeamQuotaDistributedEnforcementConfig() error = %v", err)
	}
	if cfg.RedisURL != "redis://demo-redis.sandbox0-system.svc:6379/0" {
		t.Fatalf("RedisURL = %q", cfg.RedisURL)
	}
	if cfg.RedisKeyPrefix != "sandbox0:ratelimit:test:teamquota" {
		t.Fatalf("RedisKeyPrefix = %q, want Team Quota namespace", cfg.RedisKeyPrefix)
	}
	if cfg.RedisTimeout.Duration != 250*time.Millisecond {
		t.Fatalf("RedisTimeout = %s", cfg.RedisTimeout.Duration)
	}
}

func TestApplyTeamQuotaDistributedEnforcementConfigUsesExternalRedisURLSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-url", Namespace: "sandbox0-system"},
		Data: map[string][]byte{
			"url": []byte("rediss://:password@redis.example:6379/0"),
		},
	}
	client := newTestClient(t, secret)
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Redis: &infrav1alpha1.RedisConfig{
				Type: infrav1alpha1.RedisTypeExternal,
				External: &infrav1alpha1.ExternalRedisConfig{
					URLSecret: infrav1alpha1.RedisURLSecretRef{Name: "redis-url"},
				},
			},
		},
	}
	cfg := &apiconfig.TeamQuotaDistributedEnforcementConfig{}

	if err := ApplyTeamQuotaDistributedEnforcementConfig(context.Background(), client, infra, cfg); err != nil {
		t.Fatalf("ApplyTeamQuotaDistributedEnforcementConfig() error = %v", err)
	}
	if cfg.RedisURL != "rediss://:password@redis.example:6379/0" {
		t.Fatalf("RedisURL = %q", cfg.RedisURL)
	}
	if cfg.RedisKeyPrefix != "sandbox0:teamquota" {
		t.Fatalf("RedisKeyPrefix = %q, want Team Quota namespace", cfg.RedisKeyPrefix)
	}
	if cfg.RedisTimeout.Duration != 100*time.Millisecond {
		t.Fatalf("RedisTimeout = %s", cfg.RedisTimeout.Duration)
	}
}

func TestApplyOverloadGuardConfigUsesDedicatedPrefix(t *testing.T) {
	client := newTestClient(t)
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Redis: &infrav1alpha1.RedisConfig{
				Type:             infrav1alpha1.RedisTypeBuiltin,
				KeyPrefix:        "sandbox0:test",
				OperationTimeout: metav1.Duration{Duration: 250 * time.Millisecond},
			},
		},
	}
	cfg := &apiconfig.OverloadGuardConfig{}

	if err := ApplyOverloadGuardConfig(
		context.Background(),
		client,
		infra,
		"regional-gateway:aws-us-east-1",
		cfg,
	); err != nil {
		t.Fatalf("ApplyOverloadGuardConfig() error = %v", err)
	}
	if cfg.RedisURL != "redis://demo-redis.sandbox0-system.svc:6379/0" {
		t.Fatalf("RedisURL = %q", cfg.RedisURL)
	}
	if cfg.RedisKeyPrefix != "sandbox0:test:overload-guard:regional-gateway:aws-us-east-1" {
		t.Fatalf("RedisKeyPrefix = %q", cfg.RedisKeyPrefix)
	}
	if cfg.RedisTimeout.Duration != 250*time.Millisecond {
		t.Fatalf("RedisTimeout = %s", cfg.RedisTimeout.Duration)
	}

	globalCfg := &apiconfig.OverloadGuardConfig{}
	if err := ApplyOverloadGuardConfig(
		context.Background(),
		client,
		infra,
		"global-gateway",
		globalCfg,
	); err != nil {
		t.Fatalf("ApplyOverloadGuardConfig(global) error = %v", err)
	}
	if globalCfg.RedisKeyPrefix != "sandbox0:test:overload-guard:global-gateway" {
		t.Fatalf("global RedisKeyPrefix = %q", globalCfg.RedisKeyPrefix)
	}
	if globalCfg.RedisKeyPrefix == cfg.RedisKeyPrefix {
		t.Fatalf("service namespaces collided at %q", cfg.RedisKeyPrefix)
	}
}

func TestGetRuntimeRedisConfigPreservesCustomBasePrefix(t *testing.T) {
	client := newTestClient(t)
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Redis: &infrav1alpha1.RedisConfig{
				Type:      infrav1alpha1.RedisTypeBuiltin,
				KeyPrefix: "sandbox0:custom",
			},
		},
	}

	cfg, ok, err := GetRuntimeRedisConfig(context.Background(), client, infra)
	if err != nil {
		t.Fatalf("GetRuntimeRedisConfig() error = %v", err)
	}
	if !ok {
		t.Fatal("GetRuntimeRedisConfig() ok = false, want true")
	}
	if cfg.KeyPrefix != "sandbox0:custom" {
		t.Fatalf("KeyPrefix = %q, want sandbox0:custom", cfg.KeyPrefix)
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
