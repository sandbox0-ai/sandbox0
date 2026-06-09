package registry

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

func TestBuiltinPushRegistryUsesServiceEndpointByDefault(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{}
	infra.Name = "demo"
	infra.Namespace = "sandbox0-system"

	host := builtinPushRegistry(infra, infrav1alpha1.BuiltinRegistryConfig{Port: 5000})
	if host != "demo-registry.sandbox0-system.svc:5000" {
		t.Fatalf("unexpected registry host: %q", host)
	}
}

func TestBuiltinPushRegistryUsesIngressHostWhenEnabled(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{}
	infra.Name = "demo"
	infra.Namespace = "sandbox0-system"

	host := builtinPushRegistry(infra, infrav1alpha1.BuiltinRegistryConfig{
		Port: 5000,
		Ingress: &infrav1alpha1.IngressConfig{
			Enabled: true,
			Host:    "registry.example.com",
		},
	})
	if host != "registry.example.com" {
		t.Fatalf("unexpected registry host: %q", host)
	}
}

func TestBuiltinPushRegistryUsesExplicitPushEndpoint(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{}
	infra.Name = "demo"
	infra.Namespace = "sandbox0-system"

	host := builtinPushRegistry(infra, infrav1alpha1.BuiltinRegistryConfig{
		Port:         5000,
		PushEndpoint: "http://registry-push.example.com:5443",
		Ingress: &infrav1alpha1.IngressConfig{
			Enabled: true,
			Host:    "registry.example.com",
		},
	})
	if host != "registry-push.example.com:5443" {
		t.Fatalf("unexpected registry host: %q", host)
	}
}

func TestResolveBuiltinRegistryConfigPreservesIngress(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Registry: &infrav1alpha1.RegistryConfig{
				Builtin: &infrav1alpha1.BuiltinRegistryConfig{
					Enabled: true,
					Service: &infrav1alpha1.ServiceNetworkConfig{
						Type: corev1.ServiceTypeClusterIP,
						Port: 5000,
					},
					Ingress: &infrav1alpha1.IngressConfig{
						Enabled:   true,
						ClassName: "nginx",
						Host:      "registry.example.com",
						TLSSecret: "registry-tls",
					},
				},
			},
		},
	}

	cfg := resolveBuiltinRegistryConfig(infra)
	if cfg.Ingress == nil || !cfg.Ingress.Enabled || cfg.Ingress.Host != "registry.example.com" {
		t.Fatalf("expected ingress configuration to be preserved, got %#v", cfg.Ingress)
	}
}

func TestReconcileRegistryAuthSecretSkipsNoopUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
	}
	credentials := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-registry-credentials",
			Namespace: infra.Namespace,
		},
		Data: map[string][]byte{
			"username": []byte("sandbox0"),
			"password": []byte("stable-password"),
		},
	}

	updateCount := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(infra, credentials).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, client ctrlclient.WithWatch, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
				updateCount++
				return client.Update(ctx, obj, opts...)
			},
		}).
		Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if _, _, err := reconciler.reconcileRegistryAuthSecret(context.Background(), infra, infrav1alpha1.BuiltinRegistryConfig{}); err != nil {
		t.Fatalf("reconcile auth secret: %v", err)
	}
	auth := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-registry-auth", Namespace: infra.Namespace}, auth); err != nil {
		t.Fatalf("get auth secret: %v", err)
	}
	firstHtpasswd := string(auth.Data["htpasswd"])
	if !registryHtpasswdMatches(firstHtpasswd, "sandbox0", "stable-password") {
		t.Fatalf("expected htpasswd to match generated credentials")
	}

	updateCount = 0
	if _, _, err := reconciler.reconcileRegistryAuthSecret(context.Background(), infra, infrav1alpha1.BuiltinRegistryConfig{}); err != nil {
		t.Fatalf("reconcile unchanged auth secret: %v", err)
	}
	if updateCount != 0 {
		t.Fatalf("expected unchanged auth secret to skip update, got %d updates", updateCount)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-registry-auth", Namespace: infra.Namespace}, auth); err != nil {
		t.Fatalf("get auth secret after reconcile: %v", err)
	}
	if got := string(auth.Data["htpasswd"]); got != firstHtpasswd {
		t.Fatalf("expected htpasswd to be reused, got %q want %q", got, firstHtpasswd)
	}
}
