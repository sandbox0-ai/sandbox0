package scheduler

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestDesiredHomeClusterForCoLocatedHomeCluster(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s0home",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Cluster: &infrav1alpha1.ClusterConfig{
				ID:   "home",
				Name: "sandbox0-use1-dev-home",
			},
			Services: &infrav1alpha1.ServicesConfig{
				Scheduler: &infrav1alpha1.SchedulerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					ServiceExposureConfig: infrav1alpha1.ServiceExposureConfig{
						Service: &infrav1alpha1.ServiceNetworkConfig{Port: 80},
					},
				},
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
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

	compiled := infraplan.Compile(infra)
	cluster := compiled.Scheduler.HomeCluster
	if cluster == nil {
		t.Fatal("expected desired home cluster")
	}
	if cluster.ClusterID != "home" {
		t.Fatalf("unexpected cluster id %q", cluster.ClusterID)
	}
	if cluster.ClusterName != "sandbox0-use1-dev-home" {
		t.Fatalf("unexpected cluster name %q", cluster.ClusterName)
	}
	if cluster.ClusterGatewayURL != "http://s0home-cluster-gateway:80" {
		t.Fatalf("unexpected cluster gateway URL %q", cluster.ClusterGatewayURL)
	}
	if !cluster.Enabled {
		t.Fatal("expected home cluster to be enabled")
	}
	if cluster.Weight != 100 {
		t.Fatalf("unexpected cluster weight %d", cluster.Weight)
	}
}

func TestDesiredHomeClusterSkipsExternalRegistrationClusters(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ControlPlane: &infrav1alpha1.ControlPlaneConfig{URL: "https://control-plane.example.com"},
			Cluster:      &infrav1alpha1.ClusterConfig{ID: "cluster-a", Name: "cluster-a"},
			Services: &infrav1alpha1.ServicesConfig{
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

	compiled := infraplan.Compile(infra)
	if !compiled.Components.EnableClusterRegistration {
		t.Fatal("expected external data-plane cluster registration to be enabled")
	}
	if cluster := compiled.Scheduler.HomeCluster; cluster != nil {
		t.Fatalf("expected no desired home cluster, got %#v", cluster)
	}
}

func TestBuildConfigPropagatesRegistryHosts(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-sandbox0-database-credentials",
			Namespace: "sandbox0-system",
		},
		Data: map[string][]byte{
			"username": []byte("sandbox0"),
			"password": []byte("secret"),
			"database": []byte("sandbox0"),
			"port":     []byte("5432"),
		},
	}
	reconciler := &Reconciler{
		Resources: common.NewResourceManager(fake.NewClientBuilder().WithObjects(secret).Build(), nil, nil, common.LocalDevConfig{}),
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
			Registry: &infrav1alpha1.RegistryConfig{
				Provider: infrav1alpha1.RegistryProviderBuiltin,
				Builtin: &infrav1alpha1.BuiltinRegistryConfig{
					Enabled: true,
					Port:    5000,
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Scheduler: &infrav1alpha1.SchedulerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	cfg, err := reconciler.buildConfig(context.Background(), infraplan.Compile(infra))
	if err != nil {
		t.Fatalf("buildConfig returned error: %v", err)
	}
	if cfg.RegistryPullRegistry != "demo-registry.sandbox0-system.svc:5000" {
		t.Fatalf("unexpected pull registry %q", cfg.RegistryPullRegistry)
	}
	if cfg.RegistryPushRegistry != "demo-registry.sandbox0-system.svc:5000" {
		t.Fatalf("unexpected push registry %q", cfg.RegistryPushRegistry)
	}
}
