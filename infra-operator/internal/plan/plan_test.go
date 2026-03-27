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
			Services: &infrav1alpha1.ServicesConfig{
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
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

	if len(compiled.Status.ExpectedConditions) != 4 {
		t.Fatalf("expected internal-auth/database/global/cluster conditions, got %#v", compiled.Status.ExpectedConditions)
	}
	if !compiled.Status.Endpoints.IncludeGlobalGateway {
		t.Fatal("expected global-gateway endpoint to be included")
	}
	if !compiled.Status.Endpoints.IncludeClusterGateway {
		t.Fatal("expected cluster-gateway endpoint to be included")
	}
	if compiled.Status.Endpoints.IncludeRegionalGateway || compiled.Status.Endpoints.IncludeRegionalGatewayInt {
		t.Fatalf("expected regional-gateway endpoints to be excluded, got %#v", compiled.Status.Endpoints)
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
