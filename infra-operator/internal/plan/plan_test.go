package plan

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
)

func TestCompileDerivesCrossServiceReferences(t *testing.T) {
	sharedRuntime := "shared"
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Region: "aws-us-east-1",
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
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{
							Port: 9443,
						},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
					},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ManagerConfig{
						HTTPPort: 18080,
					},
				},
				Scheduler: &infrav1alpha1.SchedulerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Replicas:             2,
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{Port: 443},
					},
					IngressExposureConfig: infrav1alpha1.IngressExposureConfig{
						Ingress: &infrav1alpha1.IngressConfig{
							Enabled: true,
							Host:    "edge.example.com",
						},
					},
					Config: &infrav1alpha1.RegionalGatewayConfig{
						GatewayConfig: infrav1alpha1.GatewayConfig{
							BaseURL: "https://edge.example.com",
						},
					},
				},
				Netd: &infrav1alpha1.NetdServiceConfig{
					EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{
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
	if got := compiled.Manager.RegionID; got != "aws-us-east-1" {
		t.Fatalf("unexpected region ID: %q", got)
	}
	if got := compiled.Manager.SandboxPodPlacement.NodeSelector["sandbox0.ai/node-role"]; got != "shared" {
		t.Fatalf("expected shared sandbox placement, got %q", got)
	}
	if got := compiled.Manager.SandboxPodPlacement.NodeSelector[dataplane.NodeDataPlaneReadyLabel]; got != dataplane.ReadyLabelValue {
		t.Fatalf("expected manager sandbox placement to require data-plane-ready nodes, got %q", got)
	}
	if _, ok := compiled.Netd.NodeSelector[dataplane.NodeDataPlaneReadyLabel]; ok {
		t.Fatalf("expected netd placement not to require data-plane-ready nodes, got %#v", compiled.Netd.NodeSelector)
	}
	if len(compiled.Netd.Tolerations) != 1 || compiled.Netd.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared netd tolerations, got %#v", compiled.Netd.Tolerations)
	}
	if !compiled.Scheduler.Enabled {
		t.Fatal("expected scheduler plan to be enabled")
	}
	if compiled.Scheduler.Config == nil || compiled.Netd.Config == nil {
		t.Fatal("expected scheduler and netd configs to be compiled")
	}
	if compiled.Scheduler.HomeCluster == nil || compiled.Scheduler.HomeCluster.ClusterID != "cluster-a" {
		t.Fatalf("expected scheduler home cluster to be compiled, got %#v", compiled.Scheduler.HomeCluster)
	}
	if compiled.Netd.RuntimeClassName == nil || *compiled.Netd.RuntimeClassName != sharedRuntime {
		t.Fatalf("expected netd runtime class name %q, got %#v", sharedRuntime, compiled.Netd.RuntimeClassName)
	}
	if !compiled.RegionalGateway.Enabled || compiled.RegionalGateway.Replicas != 2 {
		t.Fatalf("expected regional gateway plan to carry enablement and replicas, got %#v", compiled.RegionalGateway)
	}
	if compiled.RegionalGateway.Config == nil || compiled.RegionalGateway.Config.SSHEndpointHost != "" {
		t.Fatalf("expected regional gateway config without ssh endpoint, got %#v", compiled.RegionalGateway.Config)
	}
	if compiled.RegionalGateway.IngressConfig == nil || !compiled.RegionalGateway.IngressConfig.Enabled {
		t.Fatalf("expected regional gateway ingress config to be compiled, got %#v", compiled.RegionalGateway.IngressConfig)
	}
}

func TestCompileExposesStorageProxyHTTPToManager(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				StorageProxy: &infrav1alpha1.StorageProxyServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{Port: 18083},
					},
					Config: &infrav1alpha1.StorageProxyConfig{
						HTTPPort: 8081,
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	if got := compiled.Manager.Config.StorageProxyBaseURL; got != "demo-storage-proxy.sandbox0-system.svc.cluster.local" {
		t.Fatalf("unexpected manager storage-proxy base URL: %q", got)
	}
	if got := compiled.Manager.Config.StorageProxyHTTPPort; got != 8081 {
		t.Fatalf("expected HTTP port to stay 8081, got %d", got)
	}
}

func TestCompileRejectsInvalidClusterID(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Cluster: &infrav1alpha1.ClusterConfig{ID: "sandbox0-gcp-use4-gke"},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	compiled := Compile(infra)
	if len(compiled.Validation.FatalErrors) == 0 {
		t.Fatal("expected validation error")
	}
	if got := compiled.Validation.FatalErrors[0]; !strings.Contains(got, "spec.cluster.id is invalid") {
		t.Fatalf("unexpected validation error: %q", got)
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
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				Netd: &infrav1alpha1.NetdServiceConfig{
					EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{
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

func TestCompileEnablesManagerTemplateStoreForRegionalGatewayWithoutScheduler(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{
							Enabled: true,
						},
					},
				},
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "internal",
					},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	if !compiled.Manager.TemplateStoreEnabled {
		t.Fatal("expected regional single-cluster mode to enable manager template store")
	}
}

func TestCompileTracksBuiltinAndExternalBackendEnablement(t *testing.T) {
	t.Run("builtin backends can be disabled explicitly", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Database: &infrav1alpha1.DatabaseConfig{
					Type: infrav1alpha1.DatabaseTypeBuiltin,
					Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
						Enabled: false,
					},
				},
				Storage: &infrav1alpha1.StorageConfig{
					Type: infrav1alpha1.StorageTypeBuiltin,
					Builtin: &infrav1alpha1.BuiltinStorageConfig{
						Enabled: false,
					},
				},
				Registry: &infrav1alpha1.RegistryConfig{
					Provider: infrav1alpha1.RegistryProviderBuiltin,
					Builtin: &infrav1alpha1.BuiltinRegistryConfig{
						Enabled: false,
					},
				},
			},
		}

		compiled := Compile(infra)

		if compiled.Components.EnableDatabase {
			t.Fatal("expected builtin database to be disabled")
		}
		if compiled.Components.EnableStorage {
			t.Fatal("expected builtin storage to be disabled")
		}
		if compiled.Components.EnableRegistry {
			t.Fatal("expected builtin registry to be disabled")
		}
	})

	t.Run("external backends still participate in reconciliation", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Database: &infrav1alpha1.DatabaseConfig{
					Type: infrav1alpha1.DatabaseTypeExternal,
					External: &infrav1alpha1.ExternalDatabaseConfig{
						Host:     "db.example.com",
						Port:     5432,
						Database: "sandbox0",
						Username: "sandbox0",
					},
				},
				Storage: &infrav1alpha1.StorageConfig{
					Type: infrav1alpha1.StorageTypeS3,
					S3: &infrav1alpha1.S3StorageConfig{
						Endpoint: "https://s3.example.com",
						Bucket:   "sandbox0",
						Region:   "us-east-1",
					},
				},
				Registry: &infrav1alpha1.RegistryConfig{
					Provider: infrav1alpha1.RegistryProviderHarbor,
					Harbor: &infrav1alpha1.HarborRegistryConfig{
						Registry: "harbor.example.com",
					},
				},
			},
		}

		compiled := Compile(infra)

		if !compiled.Components.EnableDatabase {
			t.Fatal("expected external database to remain enabled")
		}
		if !compiled.Components.EnableStorage {
			t.Fatal("expected external storage to remain enabled")
		}
		if !compiled.Components.EnableRegistry {
			t.Fatal("expected external registry to remain enabled")
		}
	})
}

func TestCompileIncludesStatusProjectionForEnabledComponents(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Cluster: &infrav1alpha1.ClusterConfig{ID: "cluster-a"},
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Port:     5432,
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{Port: 18080},
					},
				},
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{Port: 18081},
					},
					IngressExposureConfig: infrav1alpha1.IngressExposureConfig{
						Ingress: &infrav1alpha1.IngressConfig{
							Enabled:   true,
							Host:      "edge.example.com",
							TLSSecret: "edge-tls",
						},
					},
				},
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	if len(compiled.Status.ExpectedConditions) != 5 {
		t.Fatalf("expected internal-auth/database/global/regional/cluster conditions for a home cluster, got %#v", compiled.Status.ExpectedConditions)
	}
	if got := compiled.Status.Endpoints.GlobalGateway; got != "http://demo-global-gateway:18080" {
		t.Fatalf("unexpected global-gateway endpoint %q", got)
	}
	if got := compiled.Status.Endpoints.RegionalGatewayInternal; got != "http://demo-regional-gateway:18081" {
		t.Fatalf("unexpected regional-gateway internal endpoint %q", got)
	}
	if got := compiled.Status.Endpoints.RegionalGateway; got != "https://edge.example.com" {
		t.Fatalf("unexpected regional-gateway external endpoint %q", got)
	}
	if got := compiled.Status.Endpoints.ClusterGateway; got != "http://demo-cluster-gateway:8443" {
		t.Fatalf("unexpected cluster-gateway endpoint %q", got)
	}
	if !compiled.Status.Cluster.Present || compiled.Status.Cluster.ID != "cluster-a" {
		t.Fatalf("expected projected home-cluster metadata, got %#v", compiled.Status.Cluster)
	}
	if len(compiled.Status.RetainedResources) != 2 {
		t.Fatalf("expected retained resource candidates, got %#v", compiled.Status.RetainedResources)
	}
}

func TestCompileSkipsClusterRegistrationForCoLocatedHomeCluster(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Cluster: &infrav1alpha1.ClusterConfig{ID: "cluster-a"},
			Services: &infrav1alpha1.ServicesConfig{
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				Scheduler: &infrav1alpha1.SchedulerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	if compiled.Components.EnableClusterRegistration {
		t.Fatal("did not expect co-located home cluster to enable external cluster registration")
	}
	if !compiled.Status.Cluster.Present || compiled.Status.Cluster.ID != "cluster-a" {
		t.Fatalf("expected co-located home cluster metadata projection, got %#v", compiled.Status.Cluster)
	}
	if containsString(compiled.Status.ExpectedConditions, infrav1alpha1.ConditionTypeClusterRegistered) {
		t.Fatalf("did not expect cluster registration condition for a co-located home cluster, got %#v", compiled.Status.ExpectedConditions)
	}
	if containsString(workflowStepNames(compiled.Workflow.Steps), "register-cluster") {
		t.Fatalf("did not expect register-cluster workflow step for a co-located home cluster, got %#v", workflowStepNames(compiled.Workflow.Steps))
	}
}

func TestCompileUsesConfiguredBaseURLsForGatewayStatusEndpoints(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.GlobalGatewayConfig{
						GatewayConfig: infrav1alpha1.GatewayConfig{
							BaseURL: "https://global.example.com",
						},
					},
				},
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.RegionalGatewayConfig{
						GatewayConfig: infrav1alpha1.GatewayConfig{
							BaseURL: "https://api.example.com",
						},
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	if got := compiled.Status.Endpoints.GlobalGateway; got != "https://global.example.com" {
		t.Fatalf("unexpected global-gateway endpoint %q", got)
	}
	if got := compiled.Status.Endpoints.RegionalGateway; got != "https://api.example.com" {
		t.Fatalf("unexpected regional-gateway endpoint %q", got)
	}
}

func TestCompileEnablesClusterRegistrationForExternalDataPlaneCluster(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ControlPlane: &infrav1alpha1.ControlPlaneConfig{
				URL: "https://control-plane.example.com",
			},
			Cluster: &infrav1alpha1.ClusterConfig{ID: "cluster-a"},
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	if !compiled.Components.EnableClusterRegistration {
		t.Fatal("expected external data-plane cluster to enable cluster registration")
	}
	if !compiled.Status.Cluster.Present || compiled.Status.Cluster.ID != "cluster-a" {
		t.Fatalf("expected cluster status projection for external data-plane cluster, got %#v", compiled.Status.Cluster)
	}
	if !containsString(compiled.Status.ExpectedConditions, infrav1alpha1.ConditionTypeClusterRegistered) {
		t.Fatalf("expected cluster registration condition, got %#v", compiled.Status.ExpectedConditions)
	}
	if !containsString(workflowStepNames(compiled.Workflow.Steps), "register-cluster") {
		t.Fatalf("expected register-cluster workflow step, got %#v", workflowStepNames(compiled.Workflow.Steps))
	}
}

func TestCompileTracksEnterpriseLicenseRequirements(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Scheduler: &infrav1alpha1.SchedulerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.RegionalGatewayConfig{
						SchedulerEnabled: true,
						SchedulerURL:     "http://scheduler:8080",
					},
				},
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.GlobalGatewayConfig{
						GatewayConfig: infrav1alpha1.GatewayConfig{
							OIDCProviders: []infrav1alpha1.OIDCProviderConfig{{Enabled: true}},
						},
					},
				},
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
						GatewayConfig: infrav1alpha1.GatewayConfig{
							OIDCProviders: []infrav1alpha1.OIDCProviderConfig{{Enabled: true}},
						},
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	if !compiled.Enterprise.Scheduler {
		t.Fatal("expected scheduler enterprise license to be required")
	}
	if !compiled.Enterprise.RegionalGateway {
		t.Fatal("expected regional-gateway enterprise license to be required")
	}
	if !compiled.Enterprise.GlobalGateway {
		t.Fatal("expected global-gateway enterprise license to be required")
	}
	if !compiled.Enterprise.ClusterGateway {
		t.Fatal("expected cluster-gateway enterprise license to be required")
	}
}

func TestCompileInfersRegionalGatewayEnterpriseLicenseFromCompiledSchedulerRouting(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s0home",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				Scheduler: &infrav1alpha1.SchedulerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.RegionalGatewayConfig{},
				},
			},
		},
	}

	compiled := Compile(infra)

	if compiled.Services.Scheduler.URL == "" {
		t.Fatal("expected compiled scheduler service URL")
	}
	if !compiled.Enterprise.RegionalGateway {
		t.Fatal("expected regional-gateway enterprise license to be required when scheduler routing is compiled")
	}
}

func TestCompileDisablesInitUserWhenDatabaseIsDisabled(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
			},
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					Enabled: false,
				},
			},
		},
	}

	compiled := Compile(infra)

	if compiled.Components.EnableDatabase {
		t.Fatal("expected database to be disabled")
	}
	if compiled.Components.EnableInitUser {
		t.Fatal("expected init user to be disabled when database is disabled")
	}
}

func TestCompileTracksValidationRequirements(t *testing.T) {
	t.Run("control-plane public key is required when data-plane uses control-plane config", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				ControlPlane: &infrav1alpha1.ControlPlaneConfig{},
				Services: &infrav1alpha1.ServicesConfig{
					ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
					},
				},
			},
		}

		compiled := Compile(infra)

		if !compiled.Validation.RequireControlPlanePublicKey {
			t.Fatal("expected control-plane public key requirement to be tracked")
		}
		if !containsString(compiled.Validation.FatalErrors, "controlPlane.internalAuthPublicKeySecret.name is required when controlPlane are enabled") {
			t.Fatalf("expected control-plane validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("global gateway without database is invalid", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Services: &infrav1alpha1.ServicesConfig{
					GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
					},
				},
			},
		}

		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, "globalGateway requires database to be enabled") {
			t.Fatalf("expected global-gateway/database validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("cluster metadata without data-plane is invalid", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Cluster: &infrav1alpha1.ClusterConfig{ID: "cluster-a"},
			},
		}

		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, "cluster configuration requires at least one data-plane service") {
			t.Fatalf("expected cluster validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("netd egress auth without manager is invalid", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Services: &infrav1alpha1.ServicesConfig{
					Netd: &infrav1alpha1.NetdServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						Config: &infrav1alpha1.NetdConfig{
							EgressAuthEnabled: true,
						},
					},
				},
			},
		}

		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, "netd egress auth requires manager to be enabled") {
			t.Fatalf("expected netd/manager validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("init user is invalid for federated regional gateways", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				InitUser: &infrav1alpha1.InitUserConfig{
					Email: "admin@example.com",
				},
				Database: &infrav1alpha1.DatabaseConfig{
					Type: infrav1alpha1.DatabaseTypeBuiltin,
				},
				Services: &infrav1alpha1.ServicesConfig{
					RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
						Config: &infrav1alpha1.RegionalGatewayConfig{
							AuthMode: "federated_global",
						},
					},
				},
			},
		}

		compiled := Compile(infra)
		if compiled.Components.EnableInitUser {
			t.Fatal("expected init user to be disabled for federated regional gateways")
		}
		if !containsString(compiled.Validation.FatalErrors, "initUser requires globalGateway, regionalGateway.authMode=self_hosted, or clusterGateway authMode public/both") {
			t.Fatalf("expected init-user topology validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("regional gateway requires cluster gateway internal auth", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Services: &infrav1alpha1.ServicesConfig{
					RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
					},
					ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
						Config: &infrav1alpha1.ClusterGatewayConfig{
							AuthMode: "public",
						},
					},
				},
			},
		}

		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, "regionalGateway requires clusterGateway authMode internal/both when clusterGateway is enabled") {
			t.Fatalf("expected regional/cluster auth mode validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})
}

func TestCompileTracksWorkflowRequirements(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ControlPlane: &infrav1alpha1.ControlPlaneConfig{
				InternalAuthPublicKeySecret: infrav1alpha1.SecretKeyRef{
					Name: "control-plane-public-key",
				},
			},
			Cluster: &infrav1alpha1.ClusterConfig{
				ID: "cluster-a",
			},
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
			},
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.GlobalGatewayConfig{
						GatewayConfig: infrav1alpha1.GatewayConfig{
							OIDCProviders: []infrav1alpha1.OIDCProviderConfig{
								{Enabled: true},
							},
						},
					},
				},
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.RegionalGatewayConfig{
						SchedulerEnabled: true,
						SchedulerURL:     "http://scheduler:8080",
					},
				},
				Scheduler: &infrav1alpha1.SchedulerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						GatewayConfig: infrav1alpha1.GatewayConfig{
							OIDCProviders: []infrav1alpha1.OIDCProviderConfig{
								{Enabled: true},
							},
						},
						AuthMode: "public",
					},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				Netd: &infrav1alpha1.NetdServiceConfig{
					EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
				},
				StorageProxy: &infrav1alpha1.StorageProxyServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	got := workflowStepNames(compiled.Workflow.Steps)
	want := []string{
		"control-plane-public-key",
		"internal-auth",
		"database",
		"global-gateway-enterprise-license",
		"global-gateway",
		"init-user-secret",
		"regional-gateway-enterprise-license",
		"regional-gateway",
		"scheduler-enterprise-license",
		"scheduler-rbac",
		"scheduler",
		"cluster-gateway-enterprise-license",
		"cluster-gateway",
		"ctld",
		"manager-rbac",
		"manager",
		"netd-rbac",
		"netd",
		"data-plane-node-readiness",
		"builtin-template-pods",
		"storage-proxy-rbac",
		"storage-proxy",
		"register-cluster",
	}
	if len(got) != len(want) {
		t.Fatalf("expected workflow steps %#v, got %#v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected workflow step[%d]=%q, got %#v", i, want[i], got)
		}
	}
}

func TestCompileSkipsInitUserSecretStepForOIDCOnlyBootstrap(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			InitUser: &infrav1alpha1.InitUserConfig{
				Email: "admin@example.com",
			},
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
			},
			Services: &infrav1alpha1.ServicesConfig{
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.GlobalGatewayConfig{
						GatewayConfig: infrav1alpha1.GatewayConfig{
							BuiltInAuth: infrav1alpha1.BuiltInAuthConfig{
								Enabled: false,
							},
							OIDCProviders: []infrav1alpha1.OIDCProviderConfig{
								{Enabled: true},
							},
						},
					},
				},
			},
		},
	}

	compiled := Compile(infra)
	got := workflowStepNames(compiled.Workflow.Steps)
	if containsString(got, "init-user-secret") {
		t.Fatalf("expected oidc-only bootstrap to skip init-user-secret, got %#v", got)
	}
	if !compiled.Components.EnableInitUser {
		t.Fatal("expected init user bootstrap to remain enabled")
	}
}

func TestCompileTracksCleanupPlan(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					Enabled: false,
				},
			},
			Storage: &infrav1alpha1.StorageConfig{
				Type: infrav1alpha1.StorageTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinStorageConfig{
					Enabled: false,
				},
			},
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderBuiltin,
				Builtin: &infrav1alpha1.BuiltinRegistryConfig{
					Enabled: false,
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: false},
					},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: false},
					},
				},
				Netd: &infrav1alpha1.NetdServiceConfig{
					EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: false},
				},
			},
		},
	}

	compiled := Compile(infra)

	if !compiled.Cleanup.CleanupBuiltinDatabase {
		t.Fatal("expected builtin database cleanup to be enabled")
	}
	if !compiled.Cleanup.CleanupBuiltinStorage {
		t.Fatal("expected builtin storage cleanup to be enabled")
	}
	if !compiled.Cleanup.CleanupBuiltinRegistry {
		t.Fatal("expected builtin registry cleanup to be enabled")
	}

	for _, want := range []ResourceRef{
		{Kind: "Deployment", Namespace: "sandbox0-system", Name: "demo-global-gateway"},
		{Kind: "Deployment", Namespace: "sandbox0-system", Name: "demo-manager"},
		{Kind: "DaemonSet", Namespace: "sandbox0-system", Name: "demo-netd"},
		{Kind: "StatefulSet", Namespace: "sandbox0-system", Name: "demo-postgres"},
		{Kind: "Deployment", Namespace: "sandbox0-system", Name: "demo-egress-broker"},
		{Kind: "ClusterRole", Name: "demo-manager"},
		{Kind: "ClusterRoleBinding", Name: "demo-netd"},
	} {
		if !containsResourceRef(compiled.Cleanup.DeleteNamespaced, compiled.Cleanup.DeleteClusterScoped, want) {
			t.Fatalf("expected cleanup target %#v, got namespaced=%#v cluster=%#v", want, compiled.Cleanup.DeleteNamespaced, compiled.Cleanup.DeleteClusterScoped)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsResourceRef(namespaced, cluster []ResourceRef, want ResourceRef) bool {
	candidates := namespaced
	if want.Namespace == "" {
		candidates = cluster
	}
	for _, candidate := range candidates {
		if candidate == want {
			return true
		}
	}
	return false
}

func workflowStepNames(steps []WorkflowStepPlan) []string {
	names := make([]string, 0, len(steps))
	for _, step := range steps {
		names = append(names, step.Name)
	}
	return names
}
