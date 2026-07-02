package k8s

import (
	"testing"

	"k8s.io/client-go/rest"
)

func TestApplyDefaultRateLimitUsesSandbox0DefaultsWhenUnset(t *testing.T) {
	cfg := &rest.Config{}

	ApplyDefaultRateLimit(cfg)

	if cfg.QPS != DefaultClientQPS {
		t.Fatalf("qps = %v, want %v", cfg.QPS, DefaultClientQPS)
	}
	if cfg.Burst != DefaultClientBurst {
		t.Fatalf("burst = %d, want %d", cfg.Burst, DefaultClientBurst)
	}
}

func TestApplyDefaultRateLimitPreservesConfiguredValues(t *testing.T) {
	cfg := &rest.Config{QPS: 25, Burst: 50}

	ApplyDefaultRateLimit(cfg)

	if cfg.QPS != 25 {
		t.Fatalf("qps = %v, want 25", cfg.QPS)
	}
	if cfg.Burst != 50 {
		t.Fatalf("burst = %d, want 50", cfg.Burst)
	}
}
