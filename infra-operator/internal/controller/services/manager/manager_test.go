package manager

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestCompilePlanDefaultsToNoopNetworkPolicyProviderWhenNetdIsDisabled(t *testing.T) {
	t.Run("defaults to noop when netd is disabled", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{}
		if got := infraplan.Compile(infra).Manager.NetworkPolicyProvider; got != "noop" {
			t.Fatalf("expected noop provider, got %q", got)
		}
	})

	t.Run("uses netd when netd is enabled", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Services: &infrav1alpha1.ServicesConfig{
					Netd: &infrav1alpha1.NetdServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{
							Enabled: true,
						},
					},
				},
			},
		}
		if got := infraplan.Compile(infra).Manager.NetworkPolicyProvider; got != "netd" {
			t.Fatalf("expected netd provider, got %q", got)
		}
	})
}

func TestCompilePlanSandboxPodPlacementPrefersSharedPlacement(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxNodePlacement: &infrav1alpha1.SandboxNodePlacementConfig{
				NodeSelector: map[string]string{
					"sandbox0.ai/node-role": "shared",
				},
				Tolerations: []corev1.Toleration{
					{
						Key:      "sandbox0.ai/sandbox",
						Operator: corev1.TolerationOpEqual,
						Value:    "true",
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Netd: &infrav1alpha1.NetdServiceConfig{
					NodeSelector: map[string]string{
						"sandbox0.ai/node-role": "legacy",
					},
				},
			},
		},
	}

	placement := infraplan.Compile(infra).Manager.SandboxPodPlacement
	if got := placement.NodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared placement to win, got %q", got)
	}
	if len(placement.Tolerations) != 1 || placement.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared tolerations, got %#v", placement.Tolerations)
	}
}

func TestBuildConfigPropagatesNetdMITMCASecretName(t *testing.T) {
	t.Run("uses explicit secret name", func(t *testing.T) {
		reconciler := newManagerTestReconciler(t)
		if err := reconciler.Resources.Client.Create(context.Background(), newValidMITMCASecret(t, "sandbox0-system", "custom-netd-ca")); err != nil {
			t.Fatalf("seed explicit netd mitm ca secret: %v", err)
		}
		infra := &infrav1alpha1.Sandbox0Infra{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo",
				Namespace: "sandbox0-system",
			},
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Database: &infrav1alpha1.DatabaseConfig{
					Type: infrav1alpha1.DatabaseTypeBuiltin,
					Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
						Enabled:  true,
						Port:     5432,
						Username: "sandbox0",
						Database: "sandbox0",
						SSLMode:  "disable",
					},
				},
				Services: &infrav1alpha1.ServicesConfig{
					Netd: &infrav1alpha1.NetdServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						MITMCASecretName:     "custom-netd-ca",
					},
				},
			},
		}

		cfg, err := reconciler.buildConfig(context.Background(), infra, "sandbox0/manager", "test", infraplan.Compile(infra))
		if err != nil {
			t.Fatalf("buildConfig returned error: %v", err)
		}
		if cfg.NetdMITMCASecretName != "custom-netd-ca" {
			t.Fatalf("netd mitm ca secret = %q, want custom-netd-ca", cfg.NetdMITMCASecretName)
		}
		if cfg.NetdMITMCASecretNamespace != "sandbox0-system" {
			t.Fatalf("netd mitm ca secret namespace = %q, want sandbox0-system", cfg.NetdMITMCASecretNamespace)
		}
	})

	t.Run("derives managed secret name when netd is enabled", func(t *testing.T) {
		reconciler := newManagerTestReconciler(t)
		infra := &infrav1alpha1.Sandbox0Infra{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo",
				Namespace: "sandbox0-system",
			},
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Database: &infrav1alpha1.DatabaseConfig{
					Type: infrav1alpha1.DatabaseTypeBuiltin,
					Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
						Enabled:  true,
						Port:     5432,
						Username: "sandbox0",
						Database: "sandbox0",
						SSLMode:  "disable",
					},
				},
				Services: &infrav1alpha1.ServicesConfig{
					Netd: &infrav1alpha1.NetdServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		}

		cfg, err := reconciler.buildConfig(context.Background(), infra, "sandbox0/manager", "test", infraplan.Compile(infra))
		if err != nil {
			t.Fatalf("buildConfig returned error: %v", err)
		}
		if cfg.NetdMITMCASecretName != "demo-netd-mitm-ca" {
			t.Fatalf("netd mitm ca secret = %q, want demo-netd-mitm-ca", cfg.NetdMITMCASecretName)
		}
		if cfg.NetdMITMCASecretNamespace != "sandbox0-system" {
			t.Fatalf("netd mitm ca secret namespace = %q, want sandbox0-system", cfg.NetdMITMCASecretNamespace)
		}

		secret := &corev1.Secret{}
		if err := reconciler.Resources.Client.Get(context.Background(), types.NamespacedName{
			Namespace: "sandbox0-system",
			Name:      "demo-netd-mitm-ca",
		}, secret); err != nil {
			t.Fatalf("expected managed netd mitm ca secret to be created: %v", err)
		}
	})
}

func newManagerTestReconciler(t *testing.T) *Reconciler {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "demo-sandbox0-database-credentials",
				Namespace: "sandbox0-system",
			},
			Data: map[string][]byte{
				"username": []byte("sandbox0"),
				"password": []byte("db-password"),
				"database": []byte("sandbox0"),
				"port":     []byte("5432"),
			},
		}).
		Build()
	return NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
}

func newValidMITMCASecret(t *testing.T, namespace, name string) *corev1.Secret {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generate serial: %v", err)
	}

	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   "test-netd-mitm-ca",
			Organization: []string{"sandbox0"},
		},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"ca.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			"ca.key": pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)}),
		},
	}
}
