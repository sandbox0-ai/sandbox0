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
}
