package sshgateway

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/internalauth"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestBuildConfigUsesManagerServiceURL(t *testing.T) {
	scheme := newTestScheme(t)
	infra := newTestSSHGatewayInfra()
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(newExternalDatabasePasswordSecret(), newDataPlaneKeySecret(infra)).Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	compiled := infraplan.Compile(infra)

	cfg, err := reconciler.buildConfig(context.Background(), infra, compiled)
	if err != nil {
		t.Fatalf("buildConfig() error = %v", err)
	}
	if got, want := cfg.ManagerURL, compiled.Services.Manager.URL; got != want {
		t.Fatalf("ManagerURL = %q, want %q", got, want)
	}
	if got := cfg.DatabaseURL; !strings.Contains(got, "sandbox0:secret@tcp") && !strings.Contains(got, "sandbox0:secret@db.example.com") && !strings.Contains(got, "secret") {
		t.Fatalf("DatabaseURL = %q, want secret-backed DSN", got)
	}
	if cfg.SSHPort != 2222 {
		t.Fatalf("SSHPort = %d, want 2222", cfg.SSHPort)
	}
	if cfg.InternalAuthCaller != "ssh-gateway" {
		t.Fatalf("InternalAuthCaller = %q, want ssh-gateway", cfg.InternalAuthCaller)
	}
}

func TestReconcileCreatesSSHGatewayResources(t *testing.T) {
	scheme := newTestScheme(t)
	infra := newTestSSHGatewayInfra()
	infra.Spec.Services.SSHGateway.Replicas = 0
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(newExternalDatabasePasswordSecret(), newDataPlaneKeySecret(infra)).Build()
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0/test", "dev", infraplan.Compile(infra)); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	deployment := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-ssh-gateway", Namespace: infra.Namespace}, deployment); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got := deployment.Spec.Template.Spec.Containers[0].Env[0].Value; got != "ssh-gateway" {
		t.Fatalf("SERVICE env = %q, want ssh-gateway", got)
	}
	secretName, _, _ := internalauth.GetDataPlaneKeyRefs(infra)
	if got := deployment.Spec.Template.Spec.Volumes[1].Secret.SecretName; got != secretName {
		t.Fatalf("internal auth secret = %q, want %q", got, secretName)
	}
	if deployment.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort != 2222 {
		t.Fatalf("container port = %d, want 2222", deployment.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
	}

	service := &corev1.Service{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-ssh-gateway", Namespace: infra.Namespace}, service); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if service.Spec.Ports[0].Port != 2222 {
		t.Fatalf("service port = %d, want 2222", service.Spec.Ports[0].Port)
	}

	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-ssh-gateway", Namespace: infra.Namespace}, configMap); err != nil {
		t.Fatalf("get configmap: %v", err)
	}
	configYAML := configMap.Data["config.yaml"]
	if !strings.Contains(configYAML, "manager_url: http://demo-manager.sandbox0-system.svc.cluster.local:18080") {
		t.Fatalf("config.yaml missing manager_url: %s", configYAML)
	}
	if !strings.Contains(configYAML, "ssh_port: 2222") {
		t.Fatalf("config.yaml missing ssh_port: %s", configYAML)
	}

	hostKeySecret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-ssh-gateway-host-key", Namespace: infra.Namespace}, hostKeySecret); err != nil {
		t.Fatalf("get host key secret: %v", err)
	}
	if len(hostKeySecret.Data[sshHostPrivateKeyKey]) == 0 && len(hostKeySecret.StringData[sshHostPrivateKeyKey]) == 0 {
		t.Fatal("expected ssh host private key to be generated")
	}
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("appsv1.AddToScheme() error = %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme() error = %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("infra.AddToScheme() error = %v", err)
	}
	return scheme
}

func newTestSSHGatewayInfra() *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:           "db.example.com",
					Port:           5432,
					Database:       "sandbox0",
					Username:       "sandbox0",
					PasswordSecret: infrav1alpha1.SecretKeyRef{Name: "regional-db", Key: "password"},
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true}, Replicas: 1},
					Config:                &infrav1alpha1.ManagerConfig{HTTPPort: 18080},
				},
				SSHGateway: &infrav1alpha1.SSHGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true}, Replicas: 1},
					Config:                &infrav1alpha1.SSHGatewayConfig{SSHPort: 2222},
				},
			},
		},
	}
}

func newExternalDatabasePasswordSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "regional-db", Namespace: "sandbox0-system"},
		Data:       map[string][]byte{"password": []byte("secret")},
	}
}

func newDataPlaneKeySecret(infra *infrav1alpha1.Sandbox0Infra) *corev1.Secret {
	name, privateKeyKey, publicKeyKey := internalauth.GetDataPlaneKeyRefs(infra)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
		Data: map[string][]byte{
			privateKeyKey: []byte("test-private-key"),
			publicKeyKey:  []byte("test-public-key"),
		},
	}
}
