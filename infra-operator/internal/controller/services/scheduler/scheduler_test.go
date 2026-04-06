package scheduler

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
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
	cluster := desiredHomeCluster(infra, compiled)
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
	if cluster := desiredHomeCluster(infra, compiled); cluster != nil {
		t.Fatalf("expected no desired home cluster, got %#v", cluster)
	}
}
