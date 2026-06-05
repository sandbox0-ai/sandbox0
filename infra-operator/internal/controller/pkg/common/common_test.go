package common

import (
	"context"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestResolveSandboxNodePlacementFallsBackToNetdPlacement(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Netd: &infrav1alpha1.NetdServiceConfig{
					NodeSelector: map[string]string{
						"sandbox0.ai/node-role": "sandbox",
					},
					Tolerations: []corev1.Toleration{
						{
							Key:      "sandbox.gke.io/runtime",
							Operator: corev1.TolerationOpEqual,
							Value:    "gvisor",
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
				},
			},
		},
	}

	nodeSelector, tolerations := ResolveSandboxNodePlacement(infra)
	if got := nodeSelector["sandbox0.ai/node-role"]; got != "sandbox" {
		t.Fatalf("expected sandbox node selector, got %q", got)
	}
	if len(tolerations) != 1 || tolerations[0].Key != "sandbox.gke.io/runtime" {
		t.Fatalf("expected copied toleration, got %#v", tolerations)
	}

	infra.Spec.Services.Netd.NodeSelector["sandbox0.ai/node-role"] = "system"
	infra.Spec.Services.Netd.Tolerations[0].Value = "runc"

	if got := nodeSelector["sandbox0.ai/node-role"]; got != "sandbox" {
		t.Fatalf("expected copied node selector to remain unchanged, got %q", got)
	}
	if got := tolerations[0].Value; got != "gvisor" {
		t.Fatalf("expected copied toleration to remain unchanged, got %q", got)
	}
}

func TestResolveSSHEndpointUsesEndpointPortOverride(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			PublicExposure: &infrav1alpha1.PublicExposureConfig{
				RootDomain: "sandbox0.app",
				RegionID:   "ali-ue1",
			},
			Services: &infrav1alpha1.ServicesConfig{
				SSHGateway: &infrav1alpha1.SSHGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Type: corev1.ServiceTypeNodePort,
							Port: 30222,
						},
					},
					EndpointPort: 22,
				},
			},
		},
	}

	host, port, ok := ResolveSSHEndpoint(infra, 2222)
	if !ok {
		t.Fatal("expected SSH endpoint to resolve")
	}
	if host != "ali-ue1.ssh.sandbox0.app" {
		t.Fatalf("host = %q, want ali-ue1.ssh.sandbox0.app", host)
	}
	if port != 22 {
		t.Fatalf("port = %d, want 22", port)
	}
}

func TestResolveSSHEndpointFallsBackToServicePort(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			PublicExposure: &infrav1alpha1.PublicExposureConfig{
				RootDomain: "sandbox0.app",
				RegionID:   "ali-ue1",
			},
			Services: &infrav1alpha1.ServicesConfig{
				SSHGateway: &infrav1alpha1.SSHGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Type: corev1.ServiceTypeNodePort,
							Port: 30222,
						},
					},
				},
			},
		},
	}

	_, port, ok := ResolveSSHEndpoint(infra, 2222)
	if !ok {
		t.Fatal("expected SSH endpoint to resolve")
	}
	if port != 30222 {
		t.Fatalf("port = %d, want 30222", port)
	}
}

func TestResolveSandboxNodePlacementPrefersSharedPlacement(t *testing.T) {
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
					Tolerations: []corev1.Toleration{
						{
							Key:      "sandbox.gke.io/runtime",
							Operator: corev1.TolerationOpEqual,
							Value:    "gvisor",
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
				},
			},
		},
	}

	nodeSelector, tolerations := ResolveSandboxNodePlacement(infra)
	if got := nodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared node selector to win, got %q", got)
	}
	if len(tolerations) != 1 || tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared tolerations to win, got %#v", tolerations)
	}
}

func TestResolveSandboxNodePlacementFallsBackPerField(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxNodePlacement: &infrav1alpha1.SandboxNodePlacementConfig{
				NodeSelector: map[string]string{
					"sandbox0.ai/node-role": "shared",
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Netd: &infrav1alpha1.NetdServiceConfig{
					NodeSelector: map[string]string{
						"sandbox0.ai/node-role": "legacy",
					},
					Tolerations: []corev1.Toleration{
						{
							Key:      "sandbox.gke.io/runtime",
							Operator: corev1.TolerationOpEqual,
							Value:    "gvisor",
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
				},
			},
		},
	}

	nodeSelector, tolerations := ResolveSandboxNodePlacement(infra)
	if got := nodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared node selector to win, got %q", got)
	}
	if len(tolerations) != 1 || tolerations[0].Key != "sandbox.gke.io/runtime" {
		t.Fatalf("expected legacy tolerations fallback, got %#v", tolerations)
	}
}

func TestConfigHashAnnotationChangesWithConfig(t *testing.T) {
	sameA, err := ConfigHashAnnotation(map[string]any{
		"http_port": 8080,
		"metrics":   true,
	})
	if err != nil {
		t.Fatalf("hash annotation for config A: %v", err)
	}
	sameB, err := ConfigHashAnnotation(map[string]any{
		"http_port": 8080,
		"metrics":   true,
	})
	if err != nil {
		t.Fatalf("hash annotation for config B: %v", err)
	}
	changed, err := ConfigHashAnnotation(map[string]any{
		"http_port": 18080,
		"metrics":   true,
	})
	if err != nil {
		t.Fatalf("hash annotation for changed config: %v", err)
	}

	if !reflect.DeepEqual(sameA, sameB) {
		t.Fatalf("expected identical config to have identical hash annotation, got %#v vs %#v", sameA, sameB)
	}
	if sameA[PodTemplateConfigHashAnnotation] == changed[PodTemplateConfigHashAnnotation] {
		t.Fatalf("expected changed config to produce a different hash, got %q", changed[PodTemplateConfigHashAnnotation])
	}
}

func TestReconcileHashedServiceConfigMapCreatesImmutableContentAddressedConfigMap(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(infra.DeepCopy()).
		Build()
	manager := NewResourceManager(client, scheme, nil, LocalDevConfig{})

	ref, err := manager.ReconcileHashedServiceConfigMap(context.Background(), infra, "demo-manager", GetServiceLabels("demo", "manager"), map[string]any{
		"http_port": 8080,
	})
	if err != nil {
		t.Fatalf("reconcile hashed service configmap: %v", err)
	}
	if ref.ConfigMapName != HashedServiceConfigMapName("demo-manager", ref.Hash) {
		t.Fatalf("configmap name = %q, want hashed name for %q", ref.ConfigMapName, ref.Hash)
	}
	if ref.ConfigMapName == "demo-manager" {
		t.Fatal("expected content-addressed configmap name, got legacy service name")
	}
	if got := ref.PodAnnotations()[PodTemplateConfigHashAnnotation]; got != ref.Hash {
		t.Fatalf("pod config hash annotation = %q, want %q", got, ref.Hash)
	}

	cm := &corev1.ConfigMap{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: ref.ConfigMapName, Namespace: infra.Namespace}, cm); err != nil {
		t.Fatalf("get configmap: %v", err)
	}
	if cm.Immutable == nil || !*cm.Immutable {
		t.Fatalf("expected immutable configmap, got %#v", cm.Immutable)
	}
	if cm.Annotations[ServiceConfigBaseNameAnnotation] != "demo-manager" {
		t.Fatalf("base name annotation = %q", cm.Annotations[ServiceConfigBaseNameAnnotation])
	}
	if cm.Annotations[ServiceConfigHashAnnotation] != ref.Hash {
		t.Fatalf("hash annotation = %q, want %q", cm.Annotations[ServiceConfigHashAnnotation], ref.Hash)
	}
	if cm.Data["config.yaml"] == "" {
		t.Fatal("expected config.yaml data")
	}
	if len(cm.OwnerReferences) != 1 || cm.OwnerReferences[0].Name != infra.Name {
		t.Fatalf("expected owner reference to infra, got %#v", cm.OwnerReferences)
	}
}

func TestReconcileHashedServiceConfigMapRetainsLivePodConfigAndCleansUnusedConfig(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
	}
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(infra.DeepCopy()).
		Build()
	manager := NewResourceManager(client, scheme, nil, LocalDevConfig{})
	labels := GetServiceLabels("demo", "manager")

	first, err := manager.ReconcileHashedServiceConfigMap(context.Background(), infra, "demo-manager", labels, map[string]any{
		"http_port": 8080,
	})
	if err != nil {
		t.Fatalf("reconcile first configmap: %v", err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "manager-old",
			Namespace: infra.Namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "manager", Image: "manager:test"}},
			Volumes: []corev1.Volume{{
				Name: "config",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: first.ConfigMapName},
					},
				},
			}},
		},
	}
	if err := client.Create(context.Background(), pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	second, err := manager.ReconcileHashedServiceConfigMap(context.Background(), infra, "demo-manager", labels, map[string]any{
		"http_port": 18080,
	})
	if err != nil {
		t.Fatalf("reconcile second configmap: %v", err)
	}
	if second.ConfigMapName == first.ConfigMapName {
		t.Fatalf("expected changed config to use a new configmap name, got %q", second.ConfigMapName)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Name: first.ConfigMapName, Namespace: infra.Namespace}, &corev1.ConfigMap{}); err != nil {
		t.Fatalf("expected live pod referenced configmap to be retained: %v", err)
	}

	if err := client.Delete(context.Background(), pod); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	if _, err := manager.ReconcileHashedServiceConfigMap(context.Background(), infra, "demo-manager", labels, map[string]any{
		"http_port": 18080,
	}); err != nil {
		t.Fatalf("reconcile second configmap after old pod deletion: %v", err)
	}
	if err := client.Get(context.Background(), types.NamespacedName{Name: first.ConfigMapName, Namespace: infra.Namespace}, &corev1.ConfigMap{}); !errors.IsNotFound(err) {
		t.Fatalf("expected unused old configmap to be cleaned up, got err=%v", err)
	}
}

func TestEnsurePodTemplateAnnotationsClonesInput(t *testing.T) {
	annotations := map[string]string{
		PodTemplateConfigHashAnnotation: "abc123",
		"custom":                        "value",
	}

	got := EnsurePodTemplateAnnotations(annotations)
	if !reflect.DeepEqual(got, annotations) {
		t.Fatalf("expected cloned annotations to match input, got %#v", got)
	}

	annotations["custom"] = "changed"
	if got["custom"] != "value" {
		t.Fatalf("expected cloned annotations to be isolated from caller mutation, got %#v", got)
	}
}

func TestApplyDaemonSetUpdatesExistingObject(t *testing.T) {
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

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
	}
	existing := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-netd",
			Namespace: infra.Namespace,
			Labels: map[string]string{
				"old": "label",
			},
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "old"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "old"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "old",
						Image: "old:tag",
					}},
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(infra.DeepCopy(), existing).
		Build()
	manager := NewResourceManager(client, scheme, nil, LocalDevConfig{})

	desired := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      existing.Name,
			Namespace: existing.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name": "demo-netd",
			},
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "new"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "new"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "netd",
						Image: "sandbox0ai/infra:0.2.0-rc.7",
					}},
				},
			},
		},
	}

	if err := manager.ApplyDaemonSet(context.Background(), infra, desired); err != nil {
		t.Fatalf("apply daemonset: %v", err)
	}

	got := &appsv1.DaemonSet{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: existing.Name, Namespace: existing.Namespace}, got); err != nil {
		t.Fatalf("get daemonset: %v", err)
	}
	if got.Spec.Template.Spec.Containers[0].Image != "sandbox0ai/infra:0.2.0-rc.7" {
		t.Fatalf("expected updated image, got %q", got.Spec.Template.Spec.Containers[0].Image)
	}
	if got.Spec.Template.Spec.Containers[0].Name != "netd" {
		t.Fatalf("expected updated container, got %q", got.Spec.Template.Spec.Containers[0].Name)
	}
	if got.Labels["app.kubernetes.io/name"] != "demo-netd" {
		t.Fatalf("expected updated labels, got %#v", got.Labels)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].Name != infra.Name {
		t.Fatalf("expected daemonset owner reference, got %#v", got.OwnerReferences)
	}
}
