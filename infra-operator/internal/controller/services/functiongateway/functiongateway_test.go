package functiongateway

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestBuildConfigUsesSharedGlobalJWTVerifierForFederatedRegionalAuth(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add infra scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s0cp",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.RegionalGatewayConfig{
						AuthMode: "federated_global",
					},
				},
				FunctionGateway: &infrav1alpha1.FunctionGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	compiled := infraplan.Compile(infra)

	cfg, err := reconciler.buildConfig(context.Background(), compiled)
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.JWTIssuer != compiled.DefaultFederatedGlobalJWTIssuer() {
		t.Fatalf("expected issuer %q, got %q", compiled.DefaultFederatedGlobalJWTIssuer(), cfg.JWTIssuer)
	}
	if strings.TrimSpace(cfg.JWTPublicKeyPEM) == "" {
		t.Fatal("expected function-gateway to receive a global JWT public key")
	}
	if cfg.JWTSecret != "" || cfg.JWTPrivateKeyPEM != "" || cfg.JWTPrivateKeyFile != "" {
		t.Fatalf("expected verifier-only JWT config, got secret=%q private_pem=%q private_file=%q", cfg.JWTSecret, cfg.JWTPrivateKeyPEM, cfg.JWTPrivateKeyFile)
	}
}
