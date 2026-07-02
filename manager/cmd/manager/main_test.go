package main

import (
	"testing"

	s0k8s "github.com/sandbox0-ai/sandbox0/pkg/k8s"
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

	if cfg.QPS != s0k8s.DefaultClientQPS {
		t.Fatalf("qps = %v, want %v", cfg.QPS, s0k8s.DefaultClientQPS)
	}
	if cfg.Burst != s0k8s.DefaultClientBurst {
		t.Fatalf("burst = %d, want %d", cfg.Burst, s0k8s.DefaultClientBurst)
	}
	if cfg.RateLimiter == nil {
		t.Fatal("expected shared rate limiter")
	}
}
