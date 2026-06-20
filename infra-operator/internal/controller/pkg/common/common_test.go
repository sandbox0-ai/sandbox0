package common

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestAppendObservabilityEnvVarsAddsStandardOTELConfig(t *testing.T) {
	insecure := false
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Region:  "aws-us-east-1",
			Cluster: &infrav1alpha1.ClusterConfig{ID: "cluster-a"},
			Observability: &infrav1alpha1.ObservabilityConfig{
				ResourceAttributes: map[string]string{
					"deployment.environment": "test",
					"service.name":           "wrong",
				},
				Traces: &infrav1alpha1.ObservabilityTracesConfig{
					Enabled:    true,
					Exporter:   infrav1alpha1.ObservabilityTraceExporterOTLP,
					Endpoint:   "http://collector.sandbox0:4317",
					Headers:    map[string]string{"authorization": "Bearer token", "x-scope": "platform"},
					Insecure:   &insecure,
					Timeout:    metav1.Duration{Duration: 2 * time.Second},
					SampleRate: "0.5",
				},
			},
		},
	}

	env := AppendObservabilityEnvVars([]corev1.EnvVar{
		{Name: "SERVICE", Value: "manager"},
	}, infra, ObservabilityEnvConfig{
		ServiceName: "manager",
		RegionID:    ResolveRegionID(infra),
		ClusterID:   ResolveClusterID(infra),
	})

	if envByName(env, "POD_NAME").ValueFrom == nil {
		t.Fatal("expected POD_NAME field ref")
	}
	attrs := envByName(env, envOTELResourceAttributes).Value
	for _, want := range []string{
		"deployment.environment=test",
		"service.name=manager",
		"sandbox0.region.id=aws-us-east-1",
		"sandbox0.cluster.id=cluster-a",
		"k8s.pod.name=$(POD_NAME)",
	} {
		if !strings.Contains(attrs, want) {
			t.Fatalf("OTEL_RESOURCE_ATTRIBUTES = %q, missing %q", attrs, want)
		}
	}
	if got := envByName(env, envOTELTracesExporter).Value; got != "otlp" {
		t.Fatalf("OTEL_TRACES_EXPORTER = %q, want otlp", got)
	}
	if got := envByName(env, envOTELExporterOTLPTraceEndpoint).Value; got != "http://collector.sandbox0:4317" {
		t.Fatalf("trace endpoint = %q", got)
	}
	if got := envByName(env, envOTELExporterOTLPTraceHeaders).Value; got != "authorization=Bearer+token,x-scope=platform" {
		t.Fatalf("trace headers = %q", got)
	}
	if got := envByName(env, envOTELExporterOTLPTraceInsecure).Value; got != "false" {
		t.Fatalf("trace insecure = %q", got)
	}
	if got := envByName(env, envOTELExporterOTLPTraceTimeout).Value; got != "2s" {
		t.Fatalf("trace timeout = %q", got)
	}
	if got := envByName(env, envOTELTracesSampler).Value; got != "parentbased_traceidratio" {
		t.Fatalf("trace sampler = %q", got)
	}
	if got := envByName(env, envOTELTracesSamplerArg).Value; got != "0.5" {
		t.Fatalf("trace sampler arg = %q", got)
	}
}

func TestAppendObservabilityEnvVarsKeepsExplicitEnvVars(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Observability: &infrav1alpha1.ObservabilityConfig{
				Traces: &infrav1alpha1.ObservabilityTracesConfig{Enabled: true},
			},
		},
	}

	env := AppendObservabilityEnvVars([]corev1.EnvVar{
		{Name: "NODE_NAME", Value: "preset-node"},
		{Name: envOTELTracesExporter, Value: "stdout"},
	}, infra, ObservabilityEnvConfig{ServiceName: "scheduler"})

	if got := envByName(env, "NODE_NAME").Value; got != "preset-node" {
		t.Fatalf("NODE_NAME = %q, want preset-node", got)
	}
	if got := envByName(env, envOTELTracesExporter).Value; got != "stdout" {
		t.Fatalf("OTEL_TRACES_EXPORTER = %q, want stdout", got)
	}
}

func TestAppendObservabilityEnvVarsUsesBuiltinBackendTraceDefaults(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Observability: &infrav1alpha1.ObservabilityConfig{
				Backend: &infrav1alpha1.ObservabilityBackendConfig{
					Type: infrav1alpha1.ObservabilityBackendTypeBuiltin,
				},
			},
		},
	}

	env := AppendObservabilityEnvVars(nil, infra, ObservabilityEnvConfig{ServiceName: "manager"})

	if got := envByName(env, envOTELTracesExporter).Value; got != "otlp" {
		t.Fatalf("OTEL_TRACES_EXPORTER = %q, want otlp", got)
	}
	if got := envByName(env, envOTELExporterOTLPTraceEndpoint).Value; got != "http://demo-otel-collector.sandbox0-system.svc:4317" {
		t.Fatalf("trace endpoint = %q", got)
	}
	if got := envByName(env, envOTELExporterOTLPTraceInsecure).Value; got != "true" {
		t.Fatalf("trace insecure = %q", got)
	}
}

func TestAppendObservabilityEnvVarsUsesExternalExistingCollectorSecretHeaders(t *testing.T) {
	insecure := false
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Observability: &infrav1alpha1.ObservabilityConfig{
				Backend: &infrav1alpha1.ObservabilityBackendConfig{
					Type: infrav1alpha1.ObservabilityBackendTypeExternal,
					External: &infrav1alpha1.ExternalObservabilityBackendConfig{
						Mode: infrav1alpha1.ObservabilityExternalModeExistingCollector,
						OTLP: &infrav1alpha1.ObservabilityOTLPConfig{
							Endpoint: "otel.example.com:4317",
							HeadersSecret: &infrav1alpha1.ObservabilityHeadersSecretRef{
								Name: "otel-headers",
								Key:  "trace-headers",
							},
							Insecure: &insecure,
							Timeout:  metav1.Duration{Duration: time.Second},
						},
					},
				},
			},
		},
	}

	env := AppendObservabilityEnvVars(nil, infra, ObservabilityEnvConfig{ServiceName: "regional-gateway"})

	if got := envByName(env, envOTELExporterOTLPTraceEndpoint).Value; got != "otel.example.com:4317" {
		t.Fatalf("trace endpoint = %q", got)
	}
	headers := envByName(env, envOTELExporterOTLPTraceHeaders)
	if headers.ValueFrom == nil || headers.ValueFrom.SecretKeyRef == nil {
		t.Fatalf("expected trace headers secret ref, got %#v", headers)
	}
	if got := headers.ValueFrom.SecretKeyRef.Name; got != "otel-headers" {
		t.Fatalf("headers secret name = %q", got)
	}
	if got := headers.ValueFrom.SecretKeyRef.Key; got != "trace-headers" {
		t.Fatalf("headers secret key = %q", got)
	}
	if got := envByName(env, envOTELExporterOTLPTraceInsecure).Value; got != "false" {
		t.Fatalf("trace insecure = %q", got)
	}
	if got := envByName(env, envOTELExporterOTLPTraceTimeout).Value; got != "1s" {
		t.Fatalf("trace timeout = %q", got)
	}
}

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

func TestReconcileHashedServiceConfigMapSkipsNoopUpdate(t *testing.T) {
	infra := newCommonTestInfra()
	updateCount := 0
	manager, _ := newCommonTestResourceManager(t, interceptor.Funcs{
		Update: func(ctx context.Context, client ctrlclient.WithWatch, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
			updateCount++
			return client.Update(ctx, obj, opts...)
		},
	}, infra.DeepCopy())

	labels := GetServiceLabels(infra.Name, "manager")
	if _, err := manager.ReconcileHashedServiceConfigMap(context.Background(), infra, "demo-manager", labels, map[string]any{
		"http_port": 8080,
	}); err != nil {
		t.Fatalf("reconcile hashed service configmap: %v", err)
	}
	updateCount = 0

	if _, err := manager.ReconcileHashedServiceConfigMap(context.Background(), infra, "demo-manager", labels, map[string]any{
		"http_port": 8080,
	}); err != nil {
		t.Fatalf("reconcile unchanged hashed service configmap: %v", err)
	}
	if updateCount != 0 {
		t.Fatalf("expected unchanged hashed configmap to skip update, got %d updates", updateCount)
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

func TestReconcileDeploymentSkipsNoopUpdate(t *testing.T) {
	infra := newCommonTestInfra()
	updateCount := 0
	manager, client := newCommonTestResourceManager(t, interceptor.Funcs{
		Update: func(ctx context.Context, client ctrlclient.WithWatch, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
			updateCount++
			return client.Update(ctx, obj, opts...)
		},
	}, infra.DeepCopy())
	labels := GetServiceLabels(infra.Name, "manager")
	def := ServiceDefinition{
		Name:       "manager",
		TargetPort: 8080,
		Image:      "sandbox0ai/infra:test",
	}

	if err := manager.ReconcileDeployment(context.Background(), infra, "demo-manager", labels, 1, def); err != nil {
		t.Fatalf("reconcile deployment: %v", err)
	}
	updateCount = 0
	if err := manager.ReconcileDeployment(context.Background(), infra, "demo-manager", labels, 1, def); err != nil {
		t.Fatalf("reconcile unchanged deployment: %v", err)
	}
	if updateCount != 0 {
		t.Fatalf("expected unchanged deployment to skip update, got %d updates", updateCount)
	}

	deploy := &appsv1.Deployment{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-manager", Namespace: infra.Namespace}, deploy); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	deploy.Spec.Template.Spec.Containers[0].Image = "old:test"
	if err := client.Update(context.Background(), deploy); err != nil {
		t.Fatalf("drift deployment: %v", err)
	}
	updateCount = 0
	if err := manager.ReconcileDeployment(context.Background(), infra, "demo-manager", labels, 1, def); err != nil {
		t.Fatalf("reconcile drifted deployment: %v", err)
	}
	if updateCount != 1 {
		t.Fatalf("expected drifted deployment to update once, got %d updates", updateCount)
	}
}

func TestReconcileServiceSkipsNoopUpdateAndPreservesAllocatedFields(t *testing.T) {
	infra := newCommonTestInfra()
	scheme := newCommonTestScheme(t)
	labels := GetServiceLabels(infra.Name, "cluster-gateway")
	ipFamilyPolicy := corev1.IPFamilyPolicySingleStack
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-cluster-gateway",
			Namespace: infra.Namespace,
			Labels:    EnsureManagedLabels(labels, "demo-cluster-gateway"),
		},
		Spec: corev1.ServiceSpec{
			Type:            corev1.ServiceTypeClusterIP,
			ClusterIP:       "10.96.0.25",
			ClusterIPs:      []string{"10.96.0.25"},
			IPFamilies:      []corev1.IPFamily{corev1.IPv4Protocol},
			IPFamilyPolicy:  &ipFamilyPolicy,
			SessionAffinity: corev1.ServiceAffinityNone,
			Selector:        labels,
			Ports: []corev1.ServicePort{
				BuildServicePort("http", 80, 8080, corev1.ServiceTypeClusterIP),
			},
		},
	}
	if err := NewObjectScope(infra).SetControllerReference(service, scheme); err != nil {
		t.Fatalf("set service owner: %v", err)
	}

	updateCount := 0
	manager, client := newCommonTestResourceManager(t, interceptor.Funcs{
		Update: func(ctx context.Context, client ctrlclient.WithWatch, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
			updateCount++
			return client.Update(ctx, obj, opts...)
		},
	}, infra.DeepCopy(), service)

	if err := manager.ReconcileService(context.Background(), infra, service.Name, labels, corev1.ServiceTypeClusterIP, nil, 80, 8080); err != nil {
		t.Fatalf("reconcile unchanged service: %v", err)
	}
	if updateCount != 0 {
		t.Fatalf("expected unchanged service to skip update, got %d updates", updateCount)
	}
	got := &corev1.Service{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, got); err != nil {
		t.Fatalf("get service: %v", err)
	}
	if got.Spec.ClusterIP != "10.96.0.25" {
		t.Fatalf("expected allocated clusterIP to be preserved, got %q", got.Spec.ClusterIP)
	}
}

func TestApplyStatefulSetSkipsNoopUpdate(t *testing.T) {
	infra := newCommonTestInfra()
	updateCount := 0
	manager, client := newCommonTestResourceManager(t, interceptor.Funcs{
		Update: func(ctx context.Context, client ctrlclient.WithWatch, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
			updateCount++
			return client.Update(ctx, obj, opts...)
		},
	}, infra.DeepCopy())
	desired := newCommonTestStatefulSet(infra, "demo-postgres", "postgres", "postgres:16")

	if err := manager.ApplyStatefulSet(context.Background(), infra, desired); err != nil {
		t.Fatalf("apply statefulset: %v", err)
	}
	updateCount = 0
	if err := manager.ApplyStatefulSet(context.Background(), infra, desired); err != nil {
		t.Fatalf("apply unchanged statefulset: %v", err)
	}
	if updateCount != 0 {
		t.Fatalf("expected unchanged statefulset to skip update, got %d updates", updateCount)
	}

	sts := &appsv1.StatefulSet{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, sts); err != nil {
		t.Fatalf("get statefulset: %v", err)
	}
	sts.Spec.Template.Spec.Containers[0].Image = "postgres:old"
	if err := client.Update(context.Background(), sts); err != nil {
		t.Fatalf("drift statefulset: %v", err)
	}
	updateCount = 0
	if err := manager.ApplyStatefulSet(context.Background(), infra, desired); err != nil {
		t.Fatalf("apply drifted statefulset: %v", err)
	}
	if updateCount != 1 {
		t.Fatalf("expected drifted statefulset to update once, got %d updates", updateCount)
	}
}

func TestApplyDaemonSetSkipsNoopUpdate(t *testing.T) {
	infra := newCommonTestInfra()
	updateCount := 0
	manager, _ := newCommonTestResourceManager(t, interceptor.Funcs{
		Update: func(ctx context.Context, client ctrlclient.WithWatch, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
			updateCount++
			return client.Update(ctx, obj, opts...)
		},
	}, infra.DeepCopy())
	desired := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-netd", Namespace: infra.Namespace},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "netd"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "netd"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "netd", Image: "sandbox0ai/infra:test"}},
				},
			},
		},
	}

	if err := manager.ApplyDaemonSet(context.Background(), infra, desired); err != nil {
		t.Fatalf("apply daemonset: %v", err)
	}
	updateCount = 0
	if err := manager.ApplyDaemonSet(context.Background(), infra, desired); err != nil {
		t.Fatalf("apply unchanged daemonset: %v", err)
	}
	if updateCount != 0 {
		t.Fatalf("expected unchanged daemonset to skip update, got %d updates", updateCount)
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

func newCommonTestScheme(t *testing.T) *runtime.Scheme {
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
	return scheme
}

func newCommonTestInfra() *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
	}
}

func newCommonTestResourceManager(t *testing.T, interceptors interceptor.Funcs, objects ...ctrlclient.Object) (*ResourceManager, ctrlclient.Client) {
	t.Helper()
	scheme := newCommonTestScheme(t)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithInterceptorFuncs(interceptors).
		Build()
	return NewResourceManager(client, scheme, nil, LocalDevConfig{}), client
}

func newCommonTestStatefulSet(infra *infrav1alpha1.Sandbox0Infra, name, containerName, image string) *appsv1.StatefulSet {
	labels := map[string]string{"app": name}
	replicas := int32(1)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: containerName, Image: image}},
				},
			},
		},
	}
}

func envByName(env []corev1.EnvVar, name string) corev1.EnvVar {
	for _, item := range env {
		if item.Name == name {
			return item
		}
	}
	return corev1.EnvVar{}
}
