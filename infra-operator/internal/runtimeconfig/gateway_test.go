package runtimeconfig

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestApplyGatewayConfigCopiesJWTKeyFields(t *testing.T) {
	src := infrav1alpha1.GlobalGatewayConfig{
		GatewayConfig: infrav1alpha1.GatewayConfig{
			JWTIssuer:         "https://api.sandbox0.ai",
			JWTPrivateKeyPEM:  "private-pem",
			JWTPublicKeyPEM:   "public-pem",
			JWTPrivateKeyFile: "/runtime/secrets/jwt_private_key.pem",
			JWTPublicKeyFile:  "/runtime/secrets/jwt_public_key.pem",
			JWTAccessTokenTTL: metav1.Duration{Duration: 15 * time.Minute},
			OverloadGuard: infrav1alpha1.OverloadGuardConfig{
				RequestsPerSecond:      321,
				Burst:                  654,
				LocalRequestsPerSecond: 777,
				LocalBurst:             888,
				MaxInFlight:            222,
				CleanupInterval:        metav1.Duration{Duration: 2 * time.Minute},
			},
			IdentityResourceGuard: infrav1alpha1.IdentityResourceGuardConfig{
				MaxTeamsOwnedPerUser:          7,
				MaxActiveRefreshTokensPerUser: 11,
				SessionCleanupInterval:        metav1.Duration{Duration: 45 * time.Second},
				SessionCleanupBatchSize:       321,
			},
		},
	}

	cfg := ToGlobalGateway(&src)
	if cfg.JWTIssuer != src.JWTIssuer {
		t.Fatalf("expected jwt issuer %q, got %q", src.JWTIssuer, cfg.JWTIssuer)
	}
	if cfg.JWTPrivateKeyPEM != src.JWTPrivateKeyPEM {
		t.Fatalf("expected private PEM to be copied")
	}
	if cfg.JWTPublicKeyPEM != src.JWTPublicKeyPEM {
		t.Fatalf("expected public PEM to be copied")
	}
	if cfg.JWTPrivateKeyFile != src.JWTPrivateKeyFile {
		t.Fatalf("expected private key file to be copied")
	}
	if cfg.JWTPublicKeyFile != src.JWTPublicKeyFile {
		t.Fatalf("expected public key file to be copied")
	}
	if cfg.IdentityResourceGuard.MaxTeamsOwnedPerUser != 7 ||
		cfg.IdentityResourceGuard.MaxActiveRefreshTokensPerUser != 11 {
		t.Fatalf("identity resource guard was not copied: %+v", cfg.IdentityResourceGuard)
	}
	if cfg.IdentityResourceGuard.SessionCleanupInterval.Duration != 45*time.Second ||
		cfg.IdentityResourceGuard.SessionCleanupBatchSize != 321 {
		t.Fatalf("identity cleanup config was not copied: %+v", cfg.IdentityResourceGuard)
	}
	if cfg.OverloadGuard.RequestsPerSecond != 321 ||
		cfg.OverloadGuard.Burst != 654 ||
		cfg.OverloadGuard.LocalRequestsPerSecond != 777 ||
		cfg.OverloadGuard.LocalBurst != 888 ||
		cfg.OverloadGuard.MaxInFlight != 222 ||
		cfg.OverloadGuard.CleanupInterval.Duration != 2*time.Minute {
		t.Fatalf("overload guard was not copied: %+v", cfg.OverloadGuard)
	}
}

func TestPublicGatewayConfigsCopySharedOverloadGuard(t *testing.T) {
	shared := infrav1alpha1.GatewayConfig{
		OverloadGuard: infrav1alpha1.OverloadGuardConfig{
			RequestsPerSecond:      75,
			Burst:                  125,
			LocalRequestsPerSecond: 375,
			LocalBurst:             625,
			MaxInFlight:            222,
			CleanupInterval:        metav1.Duration{Duration: 3 * time.Minute},
		},
	}

	regional := ToRegionalGateway(&infrav1alpha1.RegionalGatewayConfig{
		GatewayConfig: shared,
	})
	cluster := ToClusterGateway(&infrav1alpha1.ClusterGatewayConfig{
		GatewayConfig: shared,
	})
	for name, cfg := range map[string]struct {
		rps         int
		burst       int
		localRPS    int
		localBurst  int
		maxInFlight int
		cleanup     time.Duration
	}{
		"regional": {
			rps:         regional.OverloadGuard.RequestsPerSecond,
			burst:       regional.OverloadGuard.Burst,
			localRPS:    regional.OverloadGuard.LocalRequestsPerSecond,
			localBurst:  regional.OverloadGuard.LocalBurst,
			maxInFlight: regional.OverloadGuard.MaxInFlight,
			cleanup:     regional.OverloadGuard.CleanupInterval.Duration,
		},
		"cluster": {
			rps:         cluster.OverloadGuard.RequestsPerSecond,
			burst:       cluster.OverloadGuard.Burst,
			localRPS:    cluster.OverloadGuard.LocalRequestsPerSecond,
			localBurst:  cluster.OverloadGuard.LocalBurst,
			maxInFlight: cluster.OverloadGuard.MaxInFlight,
			cleanup:     cluster.OverloadGuard.CleanupInterval.Duration,
		},
	} {
		if cfg.rps != 75 ||
			cfg.burst != 125 ||
			cfg.localRPS != 375 ||
			cfg.localBurst != 625 ||
			cfg.maxInFlight != 222 ||
			cfg.cleanup != 3*time.Minute {
			t.Fatalf("%s overload guard = %+v", name, cfg)
		}
	}
}

func TestToClusterGatewayLeavesSandboxObservabilityDisabled(t *testing.T) {
	src := infrav1alpha1.ClusterGatewayConfig{
		HTTPPort: 9443,
	}

	cfg := ToClusterGateway(&src)
	if cfg.HTTPPort != 9443 {
		t.Fatalf("http port = %d, want 9443", cfg.HTTPPort)
	}
	if cfg.SandboxObservability.BackendType() != "disabled" {
		t.Fatalf("sandbox observability backend = %q, want disabled", cfg.SandboxObservability.BackendType())
	}
}
