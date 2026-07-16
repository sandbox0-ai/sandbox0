package netd

import (
	"context"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
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

func TestBuildRuntimeAssetsUsesCanonicalNetworkConfig(t *testing.T) {
	infra := newRuntimeTestInfra()
	infra.Spec.Network.MITMCASecretName = "custom-mitm-ca"
	infra.Spec.Network.Config = &infrav1alpha1.NetdConfig{
		NodeName:       "node-a",
		MetricsPort:    19091,
		HealthPort:     18081,
		ProxyHTTPPort:  28080,
		ProxyHTTPSPort: 28443,
	}
	infra.Spec.SandboxNodePlacement = &infrav1alpha1.SandboxNodePlacementConfig{
		NodeSelector: map[string]string{"sandbox0.ai/node-role": "shared"},
		Tolerations: []corev1.Toleration{{
			Key:      "sandbox0.ai/sandbox",
			Operator: corev1.TolerationOpEqual,
			Value:    "true",
			Effect:   corev1.TaintEffectNoSchedule,
		}},
	}
	explicitCA := newRuntimeTestMITMCASecret(t, infra, "custom-mitm-ca")

	assets, client := buildRuntimeAssets(t, infra, explicitCA)
	if assets.Config.NodeName != "node-a" || assets.Config.ClusterDNSCIDR != "10.96.0.10/32" {
		t.Fatalf("canonical network config was not preserved: %#v", assets.Config)
	}
	if assets.Config.ProxyHTTPPort != 28080 || assets.Config.ProxyHTTPSPort != 28443 {
		t.Fatalf("proxy ports = %d/%d, want 28080/28443", assets.Config.ProxyHTTPPort, assets.Config.ProxyHTTPSPort)
	}
	if got := containerPort(assets.Ports, "metrics"); got != 19091 {
		t.Fatalf("metrics port = %d, want 19091", got)
	}
	if got := containerPort(assets.Ports, "health"); got != 18081 {
		t.Fatalf("health port = %d, want 18081", got)
	}
	if len(assets.Ports) != 2 {
		t.Fatalf("container ports = %#v, want metrics and health only", assets.Ports)
	}

	if assets.ConfigRef.ConfigMapName == "" || assets.ConfigRef.Hash == "" {
		t.Fatalf("config ref is incomplete: %#v", assets.ConfigRef)
	}
	if got := assets.PodAnnotations[common.PodTemplateConfigHashAnnotation]; got != assets.ConfigRef.Hash {
		t.Fatalf("config hash annotation = %q, want %q", got, assets.ConfigRef.Hash)
	}
	configMap := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name: assets.ConfigRef.ConfigMapName, Namespace: infra.Namespace,
	}, configMap); err != nil {
		t.Fatalf("get runtime config map: %v", err)
	}
	persisted := &apiconfig.NetdConfig{}
	if err := yaml.Unmarshal([]byte(configMap.Data["config.yaml"]), persisted); err != nil {
		t.Fatalf("parse runtime config: %v", err)
	}
	if persisted.MetricsPort != 19091 || persisted.HealthPort != 18081 || persisted.ProxyHTTPPort != 28080 || persisted.ProxyHTTPSPort != 28443 {
		t.Fatalf("persisted listener ports do not match canonical config: %#v", persisted)
	}
	assertRuntimeMITMCA(t, assets, "custom-mitm-ca")

	managed := &corev1.Secret{}
	err := client.Get(context.Background(), types.NamespacedName{
		Name: managedMITMCASecretName(infra.Name), Namespace: infra.Namespace,
	}, managed)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("managed MITM CA should not be created for an explicit secret: %v", err)
	}

	withoutPlacement := infra.DeepCopy()
	withoutPlacement.Spec.SandboxNodePlacement = nil
	baseline, _ := buildRuntimeAssets(t, withoutPlacement, explicitCA.DeepCopy())
	if !reflect.DeepEqual(assets, baseline) {
		t.Fatalf("shared sandbox placement leaked into network runtime assets:\nwith placement: %#v\nwithout placement: %#v", assets, baseline)
	}
}

func TestBuildRuntimeAssetsCreatesManagedMITMCA(t *testing.T) {
	infra := newRuntimeTestInfra()
	assets, client := buildRuntimeAssets(t, infra)

	secretName := managedMITMCASecretName(infra.Name)
	assertRuntimeMITMCA(t, assets, secretName)
	secret := &corev1.Secret{}
	if err := client.Get(context.Background(), types.NamespacedName{
		Name: secretName, Namespace: infra.Namespace,
	}, secret); err != nil {
		t.Fatalf("get managed MITM CA: %v", err)
	}
	if err := validateMITMCASecret(secret); err != nil {
		t.Fatalf("managed MITM CA is invalid: %v", err)
	}
	if len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].Name != infra.Name {
		t.Fatalf("managed MITM CA owner references = %#v, want %q", secret.OwnerReferences, infra.Name)
	}
}

func TestBuildRuntimeAssetsReturnsNilWhenNetworkDisabled(t *testing.T) {
	infra := newRuntimeTestInfra()
	infra.Spec.Network = nil
	reconciler, _ := newRuntimeTestReconciler(t, infra)

	assets, err := reconciler.BuildRuntimeAssets(context.Background(), infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("BuildRuntimeAssets() error = %v", err)
	}
	if assets != nil {
		t.Fatal("BuildRuntimeAssets() returned non-nil assets when network is disabled")
	}
}

func buildRuntimeAssets(t *testing.T, infra *infrav1alpha1.Sandbox0Infra, objects ...ctrlclient.Object) (*RuntimeAssets, ctrlclient.Client) {
	t.Helper()
	reconciler, client := newRuntimeTestReconciler(t, infra, objects...)
	assets, err := reconciler.BuildRuntimeAssets(context.Background(), infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("BuildRuntimeAssets() error = %v", err)
	}
	if assets == nil {
		t.Fatal("BuildRuntimeAssets() returned nil assets")
	}
	return assets, client
}

func newRuntimeTestReconciler(t *testing.T, infra *infrav1alpha1.Sandbox0Infra, objects ...ctrlclient.Object) (*Reconciler, ctrlclient.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	allObjects := []ctrlclient.Object{
		infra.DeepCopy(),
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "kube-dns", Namespace: "kube-system"},
			Spec:       corev1.ServiceSpec{ClusterIP: "10.96.0.10"},
		},
	}
	allObjects = append(allObjects, objects...)
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjects...).Build()
	resources := common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{})
	return NewReconciler(resources), client
}

func newRuntimeTestInfra() *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		TypeMeta: metav1.TypeMeta{
			APIVersion: infrav1alpha1.GroupVersion.String(),
			Kind:       "Sandbox0Infra",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
			UID:       types.UID("demo-uid"),
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Region:  "test-region",
			Cluster: &infrav1alpha1.ClusterConfig{ID: "cluster-a"},
			Network: &infrav1alpha1.NetworkConfig{
				Config: &infrav1alpha1.NetdConfig{},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             1,
					},
				},
			},
		},
	}
}

func newRuntimeTestMITMCASecret(t *testing.T, infra *infrav1alpha1.Sandbox0Infra, name string) *corev1.Secret {
	t.Helper()
	cert, key, err := generateManagedMITMCA(infra.Name)
	if err != nil {
		t.Fatalf("generate MITM CA: %v", err)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
		Type:       corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			mitmCACertKey: cert,
			mitmCAKeyKey:  key,
		},
	}
}

func assertRuntimeMITMCA(t *testing.T, assets *RuntimeAssets, secretName string) {
	t.Helper()
	volume := runtimeVolume(assets.Volumes, "mitm-ca")
	if volume == nil || volume.Secret == nil || volume.Secret.SecretName != secretName {
		t.Fatalf("MITM CA volume = %#v, want secret %q", volume, secretName)
	}
	mount := runtimeMount(assets.VolumeMounts, "mitm-ca")
	if mount == nil || mount.MountPath != "/tls" || !mount.ReadOnly {
		t.Fatalf("MITM CA mount = %#v, want read-only /tls", mount)
	}
	if assets.Config.MITMCACertPath != "/tls/ca.crt" || assets.Config.MITMCAKeyPath != "/tls/ca.key" {
		t.Fatalf("MITM CA config paths = %q/%q", assets.Config.MITMCACertPath, assets.Config.MITMCAKeyPath)
	}
}

func runtimeVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

func runtimeMount(mounts []corev1.VolumeMount, name string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == name {
			return &mounts[i]
		}
	}
	return nil
}

func containerPort(ports []corev1.ContainerPort, name string) int32 {
	for i := range ports {
		if ports[i].Name == name {
			return ports[i].ContainerPort
		}
	}
	return 0
}
