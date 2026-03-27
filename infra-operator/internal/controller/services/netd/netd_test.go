package netd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestReconcileUsesSharedSandboxNodePlacement(t *testing.T) {
	sharedRuntime := "shared"
	legacyRuntime := "legacy"
	infra := newNetdTestInfra()
	infra.Spec.SandboxNodePlacement = &infrav1alpha1.SandboxNodePlacementConfig{
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
	}
	infra.Spec.Services.Netd.NodeSelector = map[string]string{
		"sandbox0.ai/node-role": "legacy",
	}
	infra.Spec.Services.Netd.Tolerations = []corev1.Toleration{
		{
			Key:      "sandbox.gke.io/runtime",
			Operator: corev1.TolerationOpEqual,
			Value:    legacyRuntime,
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	ds := reconcileNetdDaemonSet(t, infra)
	if got := ds.Spec.Template.Spec.NodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared node selector, got %q", got)
	}
	if len(ds.Spec.Template.Spec.Tolerations) != 1 || ds.Spec.Template.Spec.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared toleration, got %#v", ds.Spec.Template.Spec.Tolerations)
	}
	if ds.Spec.Template.Spec.RuntimeClassName == nil || *ds.Spec.Template.Spec.RuntimeClassName != sharedRuntime {
		t.Fatalf("expected runtimeClassName %q, got %#v", sharedRuntime, ds.Spec.Template.Spec.RuntimeClassName)
	}
}

func TestReconcileFallsBackToLegacyNetdPlacement(t *testing.T) {
	legacyRuntime := "legacy"
	infra := newNetdTestInfra()
	infra.Spec.Services.Netd.NodeSelector = map[string]string{
		"sandbox0.ai/node-role": "legacy",
	}
	infra.Spec.Services.Netd.Tolerations = []corev1.Toleration{
		{
			Key:      "sandbox.gke.io/runtime",
			Operator: corev1.TolerationOpEqual,
			Value:    legacyRuntime,
			Effect:   corev1.TaintEffectNoSchedule,
		},
	}

	ds := reconcileNetdDaemonSet(t, infra)
	if got := ds.Spec.Template.Spec.NodeSelector["sandbox0.ai/node-role"]; got != "legacy" {
		t.Fatalf("expected legacy node selector fallback, got %q", got)
	}
	if len(ds.Spec.Template.Spec.Tolerations) != 1 || ds.Spec.Template.Spec.Tolerations[0].Value != legacyRuntime {
		t.Fatalf("expected legacy toleration fallback, got %#v", ds.Spec.Template.Spec.Tolerations)
	}
}

func TestReconcileMountsExplicitMITMCASecret(t *testing.T) {
	infra := newNetdTestInfra()
	infra.Spec.Services.Netd.MITMCASecretName = "netd-mitm-ca"
	client, scheme := newNetdTestClient(t, infra.DeepCopy())

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest", nil); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	assertNetdMITMSecretMounted(t, client, infra, "netd-mitm-ca")
}

func TestReconcileAutoGeneratesManagedMITMCASecret(t *testing.T) {
	infra := newNetdTestInfra()
	client, scheme := newNetdTestClient(t, infra.DeepCopy())

	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest", nil); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	secretName := managedMITMCASecretName(infra)
	assertNetdMITMSecretMounted(t, client, infra, secretName)

	secret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      secretName,
		Namespace: infra.Namespace,
	}, secret); err != nil {
		t.Fatalf("expected managed secret: %v", err)
	}
	assertValidManagedMITMCASecret(t, secret)
	if len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].Name != infra.Name {
		t.Fatalf("expected secret to be owned by infra, got %#v", secret.OwnerReferences)
	}
}

func TestReconcileReusesManagedMITMCASecretAcrossReconciles(t *testing.T) {
	infra := newNetdTestInfra()
	client, scheme := newNetdTestClient(t, infra.DeepCopy())
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest", nil); err != nil {
		t.Fatalf("first reconcile returned error: %v", err)
	}

	secretName := managedMITMCASecretName(infra)
	secret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      secretName,
		Namespace: infra.Namespace,
	}, secret); err != nil {
		t.Fatalf("expected managed secret after first reconcile: %v", err)
	}
	firstCert := append([]byte(nil), secret.Data[mitmCACertKey]...)
	firstKey := append([]byte(nil), secret.Data[mitmCAKeyKey]...)

	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest", nil); err != nil {
		t.Fatalf("second reconcile returned error: %v", err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      secretName,
		Namespace: infra.Namespace,
	}, secret); err != nil {
		t.Fatalf("expected managed secret after second reconcile: %v", err)
	}
	if string(secret.Data[mitmCACertKey]) != string(firstCert) || string(secret.Data[mitmCAKeyKey]) != string(firstKey) {
		t.Fatalf("expected managed secret to be reused across reconciles")
	}
}

func TestReconcileRepairsInvalidManagedMITMCASecret(t *testing.T) {
	infra := newNetdTestInfra()
	invalidSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      managedMITMCASecretName(infra),
			Namespace: infra.Namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			mitmCACertKey: []byte("invalid cert"),
			mitmCAKeyKey:  []byte("invalid key"),
		},
	}
	client, scheme := newNetdTestClient(t, infra.DeepCopy(), invalidSecret)
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest", nil); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	secret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      managedMITMCASecretName(infra),
		Namespace: infra.Namespace,
	}, secret); err != nil {
		t.Fatalf("expected repaired secret: %v", err)
	}
	assertValidManagedMITMCASecret(t, secret)
}

func TestReconcileExplicitMITMCASecretSkipsManagedSecret(t *testing.T) {
	infra := newNetdTestInfra()
	infra.Spec.Services.Netd.MITMCASecretName = "custom-mitm-ca"
	client, scheme := newNetdTestClient(t, infra.DeepCopy())
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest", nil); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	assertNetdMITMSecretMounted(t, client, infra, "custom-mitm-ca")

	managedSecret := &corev1.Secret{}
	err := client.Get(context.Background(), types.NamespacedName{
		Name:      managedMITMCASecretName(infra),
		Namespace: infra.Namespace,
	}, managedSecret)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected no managed secret when explicit secret is configured, got %v", err)
	}
}

func reconcileNetdDaemonSet(t *testing.T, infra *infrav1alpha1.Sandbox0Infra) *appsv1.DaemonSet {
	t.Helper()

	client, scheme := newNetdTestClient(t, infra.DeepCopy())
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest", nil); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	ds := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      infra.Name + "-netd",
		Namespace: infra.Namespace,
	}, ds); err != nil {
		t.Fatalf("expected daemonset to be created: %v", err)
	}

	return ds
}

func TestReconcileUsesCompiledPlanForEgressAuthResolverURL(t *testing.T) {
	infra := newNetdTestInfra()
	client, scheme := newNetdTestClient(t, infra.DeepCopy())
	reconciler := NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	compiled := infraplan.Compile(infra)
	compiled.Netd.EgressAuthResolverURL = "http://planned-manager.sandbox0-system.svc.cluster.local:19090"

	if err := reconciler.Reconcile(context.Background(), infra, "ghcr.io/sandbox0-ai/sandbox0", "latest", compiled); err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}

	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      infra.Name + "-netd",
		Namespace: infra.Namespace,
	}, configMap); err != nil {
		t.Fatalf("expected configmap to be created: %v", err)
	}

	cfg := &apiconfig.NetdConfig{}
	if err := yaml.Unmarshal([]byte(configMap.Data["config.yaml"]), cfg); err != nil {
		t.Fatalf("failed to parse netd config: %v", err)
	}
	if got := cfg.EgressAuthResolverURL; got != compiled.Netd.EgressAuthResolverURL {
		t.Fatalf("expected resolver URL %q, got %q", compiled.Netd.EgressAuthResolverURL, got)
	}
}

func newNetdTestClient(t *testing.T, objects ...ctrlclient.Object) (ctrlclient.Client, *runtime.Scheme) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add appsv1 scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	objects = append(objects, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kube-dns",
			Namespace: "kube-system",
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "10.96.0.10",
		},
	})

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		Build()
	return client, scheme
}

func assertNetdMITMSecretMounted(t *testing.T, client ctrlclient.Client, infra *infrav1alpha1.Sandbox0Infra, secretName string) {
	t.Helper()

	ds := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      infra.Name + "-netd",
		Namespace: infra.Namespace,
	}, ds); err != nil {
		t.Fatalf("expected daemonset: %v", err)
	}
	foundVolume := false
	foundMount := false
	for _, volume := range ds.Spec.Template.Spec.Volumes {
		if volume.Name == "mitm-ca" && volume.Secret != nil && volume.Secret.SecretName == secretName {
			foundVolume = true
		}
	}
	for _, mount := range ds.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == "mitm-ca" && mount.MountPath == "/tls" && mount.ReadOnly {
			foundMount = true
		}
	}
	if !foundVolume || !foundMount {
		t.Fatalf("expected mitm-ca volume and mount, got volumes=%#v mounts=%#v", ds.Spec.Template.Spec.Volumes, ds.Spec.Template.Spec.Containers[0].VolumeMounts)
	}

	cm := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name:      infra.Name + "-netd",
		Namespace: infra.Namespace,
	}, cm); err != nil {
		t.Fatalf("expected configmap: %v", err)
	}
	if got := cm.Data["config.yaml"]; !containsMITMPaths(got) {
		t.Fatalf("expected mitm ca paths in config, got %q", got)
	}
}

func assertValidManagedMITMCASecret(t *testing.T, secret *corev1.Secret) {
	t.Helper()

	if err := validateMITMCASecret(secret); err != nil {
		t.Fatalf("expected valid managed MITM CA secret: %v", err)
	}
	if _, err := tls.X509KeyPair(secret.Data[mitmCACertKey], secret.Data[mitmCAKeyKey]); err != nil {
		t.Fatalf("expected managed secret keypair to load: %v", err)
	}

	block, _ := pem.Decode(secret.Data[mitmCACertKey])
	if block == nil {
		t.Fatalf("expected managed secret certificate PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse managed secret certificate: %v", err)
	}
	if !cert.IsCA {
		t.Fatalf("expected managed certificate to be a CA")
	}
}

func containsMITMPaths(config string) bool {
	return strings.Contains(config, "mitm_ca_cert_path: /tls/ca.crt") &&
		strings.Contains(config, "mitm_ca_key_path: /tls/ca.key")
}

func newNetdTestInfra() *infrav1alpha1.Sandbox0Infra {
	runtimeClass := "shared"
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Netd: &infrav1alpha1.NetdServiceConfig{
					EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{
						Enabled: true,
					},
					RuntimeClassName: &runtimeClass,
					Config:           &infrav1alpha1.NetdConfig{},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}
}
