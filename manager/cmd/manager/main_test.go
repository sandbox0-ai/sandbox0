package main

import (
	"testing"

	"k8s.io/client-go/rest"
)

func TestConfigureK8sClientRateLimiterUsesConfiguredValues(t *testing.T) {
	cfg := &rest.Config{}

	configureK8sClientRateLimiter(cfg, 25, 50)

	if cfg.QPS != 25 {
		t.Fatalf("qps = %v, want 25", cfg.QPS)
	}
	if cfg.Burst != 50 {
		t.Fatalf("burst = %d, want 50", cfg.Burst)
	}
	if cfg.RateLimiter == nil {
		t.Fatal("expected shared rate limiter")
	}
}

func TestConfigureK8sClientRateLimiterDefaultsWhenUnset(t *testing.T) {
	cfg := &rest.Config{}

	configureK8sClientRateLimiter(cfg, 0, 0)

	if cfg.QPS != rest.DefaultQPS {
		t.Fatalf("qps = %v, want %v", cfg.QPS, rest.DefaultQPS)
	}
	if cfg.Burst != rest.DefaultBurst {
		t.Fatalf("burst = %d, want %d", cfg.Burst, rest.DefaultBurst)
	}
	if cfg.RateLimiter == nil {
		t.Fatal("expected shared rate limiter")
	}
}
