package common

import (
	"context"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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

func TestApplyDeploymentUpdatesExistingObject(t *testing.T) {
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
	replicas := int32(2)
	existing := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-storage-proxy",
			Namespace: infra.Namespace,
			Labels: map[string]string{
				"old": "label",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
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

	desiredReplicas := int32(1)
	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      existing.Name,
			Namespace: existing.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name": "demo-storage-proxy",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &desiredReplicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "new"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "new"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "storage-proxy",
						Image: "sandbox0ai/storage-proxy:0.2.0-rc.7",
					}},
				},
			},
		},
	}

	if err := manager.ApplyDeployment(context.Background(), infra, desired); err != nil {
		t.Fatalf("apply deployment: %v", err)
	}

	got := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: existing.Name, Namespace: existing.Namespace}, got); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got.Spec.Template.Spec.Containers[0].Image != "sandbox0ai/storage-proxy:0.2.0-rc.7" {
		t.Fatalf("expected updated image, got %q", got.Spec.Template.Spec.Containers[0].Image)
	}
	if got.Spec.Template.Spec.Containers[0].Name != "storage-proxy" {
		t.Fatalf("expected updated container, got %q", got.Spec.Template.Spec.Containers[0].Name)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != desiredReplicas {
		t.Fatalf("expected updated replicas, got %#v", got.Spec.Replicas)
	}
	if got.Labels["app.kubernetes.io/name"] != "demo-storage-proxy" {
		t.Fatalf("expected updated labels, got %#v", got.Labels)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].Name != infra.Name {
		t.Fatalf("expected deployment owner reference, got %#v", got.OwnerReferences)
	}
}

func TestDeploymentMatchesDesired(t *testing.T) {
	replicas := int32(1)
	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "demo",
			Namespace:   "sandbox0-system",
			Labels:      map[string]string{"app": "demo"},
			Annotations: map[string]string{"config": "abc"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "infra.sandbox0.ai/v1alpha1",
				Kind:       "Sandbox0Infra",
				Name:       "demo",
			}},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "demo"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "demo", Image: "demo:latest"}}},
			},
		},
	}

	current := desired.DeepCopy()
	if !deploymentMatchesDesired(current, desired) {
		t.Fatal("expected identical deployment to match desired state")
	}

	current.Spec.Template.Spec.Containers[0].Image = "demo:new"
	if deploymentMatchesDesired(current, desired) {
		t.Fatal("expected deployment with changed pod template to require update")
	}
}

func TestDaemonSetMatchesDesired(t *testing.T) {
	desired := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "demo",
			Namespace:   "sandbox0-system",
			Labels:      map[string]string{"app": "demo"},
			Annotations: map[string]string{"config": "abc"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "infra.sandbox0.ai/v1alpha1",
				Kind:       "Sandbox0Infra",
				Name:       "demo",
			}},
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "demo"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "demo", Image: "demo:latest"}}},
			},
		},
	}

	current := desired.DeepCopy()
	if !daemonSetMatchesDesired(current, desired) {
		t.Fatal("expected identical daemonset to match desired state")
	}

	current.Labels["config"] = "new"
	if daemonSetMatchesDesired(current, desired) {
		t.Fatal("expected daemonset with changed labels to require update")
	}
}
