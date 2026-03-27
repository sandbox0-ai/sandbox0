package plan

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestCompileDerivesCrossServiceReferences(t *testing.T) {
	sharedRuntime := "shared"
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Region: "aws/us-east-1",
			Cluster: &infrav1alpha1.ClusterConfig{
				ID: "cluster-a",
			},
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
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
						Enabled: true,
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Port: 9443,
						},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
					},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
						Enabled: true,
					},
					Config: &infrav1alpha1.ManagerConfig{
						HTTPPort: 18080,
					},
				},
				Netd: &infrav1alpha1.NetdServiceConfig{
					BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
						Enabled: true,
					},
					RuntimeClassName: &sharedRuntime,
				},
			},
		},
	}

	compiled := Compile(infra)

	if !compiled.Components.EnableManager || !compiled.Components.EnableClusterGateway || !compiled.Components.EnableNetd {
		t.Fatalf("expected manager, cluster-gateway, and netd to be enabled: %#v", compiled.Components)
	}
	if !compiled.Manager.TemplateStoreEnabled {
		t.Fatalf("expected template store to be enabled")
	}
	if got := compiled.Manager.NetworkPolicyProvider; got != "netd" {
		t.Fatalf("expected netd network policy provider, got %q", got)
	}
	if got := compiled.Services.Manager.URL; got != "http://demo-manager.sandbox0-system.svc.cluster.local:18080" {
		t.Fatalf("unexpected manager service URL: %q", got)
	}
	if got := compiled.Netd.EgressAuthResolverURL; got != compiled.Services.Manager.URL {
		t.Fatalf("expected netd resolver URL to match manager service URL, got %q", got)
	}
	if got := compiled.RegionalGateway.DefaultClusterGatewayURL; got != "http://demo-cluster-gateway:9443" {
		t.Fatalf("unexpected cluster gateway URL: %q", got)
	}
	if got := compiled.Manager.DefaultClusterID; got != "cluster-a" {
		t.Fatalf("unexpected default cluster ID: %q", got)
	}
	if got := compiled.Manager.RegionID; got != "aws/us-east-1" {
		t.Fatalf("unexpected region ID: %q", got)
	}
	if got := compiled.Manager.SandboxPodPlacement.NodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared sandbox placement, got %q", got)
	}
	if len(compiled.Netd.Tolerations) != 1 || compiled.Netd.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared netd tolerations, got %#v", compiled.Netd.Tolerations)
	}
}

func TestCompilePreservesExplicitNetdResolverURL(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
						Enabled: true,
					},
				},
				Netd: &infrav1alpha1.NetdServiceConfig{
					BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
						Enabled: true,
					},
					Config: &infrav1alpha1.NetdConfig{
						EgressAuthResolverURL: "http://explicit-resolver:9000",
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	if got := compiled.Netd.EgressAuthResolverURL; got != "http://explicit-resolver:9000" {
		t.Fatalf("expected explicit resolver URL to win, got %q", got)
	}
}
