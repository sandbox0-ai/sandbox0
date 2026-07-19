package plan

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

const testRegionStateID = "11111111-1111-4111-8111-111111111111"

func TestCompileProjectsTeamQuotaDefaultsOnlyToRegionOwner(t *testing.T) {
	limit := int64(25)
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Status: infrav1alpha1.Sandbox0InfraStatus{
			TeamQuota: &infrav1alpha1.TeamQuotaStatus{StateID: testRegionStateID},
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			TeamQuota: &infrav1alpha1.TeamQuotaConfig{
				Defaults: []infrav1alpha1.TeamQuotaPolicyConfig{
					{
						Key:   "sandbox_runtime_count",
						Kind:  "capacity",
						Limit: &limit,
					},
				},
				DistributedEnforcement: infrav1alpha1.TeamQuotaDistributedEnforcementConfig{
					PolicyCacheTTL: metav1.Duration{Duration: 7 * time.Second},
				},
			},
			Network: &infrav1alpha1.NetworkConfig{},
			Services: &infrav1alpha1.ServicesConfig{
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

	compiled := Compile(infra)
	if len(compiled.RegionalGateway.Config.TeamQuota.Defaults) != 1 {
		t.Fatalf("regional gateway defaults = %#v, want one owner policy", compiled.RegionalGateway.Config.TeamQuota.Defaults)
	}
	if !compiled.RegionalGateway.Config.TeamQuota.PolicyOwner {
		t.Fatal("expected regional gateway to own policy reconciliation")
	}
	if got := compiled.RegionalGateway.Config.TeamQuota.Defaults[0]; got.Limit == nil || *got.Limit != 25 {
		t.Fatalf("regional gateway default = %#v, want sandbox runtime limit 25", got)
	}
	if compiled.Manager.Config.TeamQuotaDistributedEnforcement.PolicyCacheTTL.Duration != 7*time.Second {
		t.Fatalf("manager policy cache ttl = %s, want 7s", compiled.Manager.Config.TeamQuotaDistributedEnforcement.PolicyCacheTTL.Duration)
	}
	if compiled.Manager.Config.TeamQuotaDistributedEnforcement.StateID != testRegionStateID {
		t.Fatalf("manager state ID = %q, want %q", compiled.Manager.Config.TeamQuotaDistributedEnforcement.StateID, testRegionStateID)
	}
	if compiled.Network.Config.TeamQuotaDistributedEnforcement.PolicyCacheTTL.Duration != 7*time.Second {
		t.Fatalf("network policy cache ttl = %s, want 7s", compiled.Network.Config.TeamQuotaDistributedEnforcement.PolicyCacheTTL.Duration)
	}
	if compiled.Network.Config.TeamQuotaDistributedEnforcement.StateID != testRegionStateID {
		t.Fatalf("network state ID = %q, want %q", compiled.Network.Config.TeamQuotaDistributedEnforcement.StateID, testRegionStateID)
	}
}

func TestCompileValidatesRegionOwnerStateIdentityLifecycle(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Region:    "region-1",
			Database:  testExternalDatabase(),
			Redis:     testExternalRedis(),
			TeamQuota: &infrav1alpha1.TeamQuotaConfig{},
			Services: &infrav1alpha1.ServicesConfig{
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}
	const missingStatusMessage = "region Team Quota owner requires a canonical UUID v4 in status.teamQuota.stateId initialized by infra-operator"

	compiled := Compile(infra)
	if !containsString(compiled.Validation.FatalErrors, missingStatusMessage) {
		t.Fatalf("owner without initialized status was accepted: %#v", compiled.Validation.FatalErrors)
	}

	infra.Status.TeamQuota = &infrav1alpha1.TeamQuotaStatus{StateID: testRegionStateID}
	compiled = Compile(infra)
	if containsString(compiled.Validation.FatalErrors, missingStatusMessage) {
		t.Fatalf("canonical owner status was rejected: %#v", compiled.Validation.FatalErrors)
	}
	if got := compiled.RegionalGateway.Config.TeamQuota.DistributedEnforcement.StateID; got != testRegionStateID {
		t.Fatalf("owner runtime state ID = %q, want status value %q", got, testRegionStateID)
	}

	infra.Spec.TeamQuota.StateID = testRegionStateID
	compiled = Compile(infra)
	if containsString(compiled.Validation.FatalErrors, "recovery input must match") {
		t.Fatalf("matching recovery input was rejected: %#v", compiled.Validation.FatalErrors)
	}

	infra.Spec.TeamQuota.StateID = "4f54208d-4f01-42da-bdbc-88cc5793857b"
	compiled = Compile(infra)
	const mismatchMessage = "region Team Quota owner spec.teamQuota.stateId recovery input must match the immutable status.teamQuota.stateId"
	if !containsString(compiled.Validation.FatalErrors, mismatchMessage) {
		t.Fatalf("changed recovery input was accepted: %#v", compiled.Validation.FatalErrors)
	}
	if got := compiled.RegionalGateway.Config.TeamQuota.DistributedEnforcement.StateID; got != testRegionStateID {
		t.Fatalf("mismatched spec overrode authoritative status: got %q, want %q", got, testRegionStateID)
	}
}

func TestCompileProjectsConsumerOnlyTeamQuotaSettingsWithoutDefaults(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ControlPlane: &infrav1alpha1.ControlPlaneConfig{
				URL: "https://region.example.com",
				InternalAuthPublicKeySecret: infrav1alpha1.SecretKeyRef{
					Name: "control-plane-public-key",
					Key:  "public.key",
				},
			},
			Database: testExternalDatabase(),
			Redis:    testExternalRedis(),
			TeamQuota: &infrav1alpha1.TeamQuotaConfig{
				StateID: testRegionStateID,
				DistributedEnforcement: infrav1alpha1.TeamQuotaDistributedEnforcementConfig{
					PolicyCacheTTL: metav1.Duration{Duration: 7 * time.Second},
					LeaseTTL:       metav1.Duration{Duration: 21 * time.Second},
					RenewInterval:  metav1.Duration{Duration: 7 * time.Second},
				},
			},
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
	joined := strings.Join(compiled.Validation.FatalErrors, "\n")
	if strings.Contains(joined, "complete policy") || strings.Contains(joined, "must omit spec.teamQuota.defaults") {
		t.Fatalf("consumer-only settings were rejected: %#v", compiled.Validation.FatalErrors)
	}
	distributed := compiled.Manager.Config.TeamQuotaDistributedEnforcement
	if distributed.PolicyCacheTTL.Duration != 7*time.Second ||
		distributed.LeaseTTL.Duration != 21*time.Second ||
		distributed.RenewInterval.Duration != 7*time.Second ||
		distributed.StateID != testRegionStateID {
		t.Fatalf("manager distributed settings = %#v, want 7s/21s/7s", distributed)
	}
}

func TestCompileRejectsDefaultsOnConsumerOnlyTeamQuota(t *testing.T) {
	limit := int64(10)
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ControlPlane: &infrav1alpha1.ControlPlaneConfig{
				URL: "https://region.example.com",
				InternalAuthPublicKeySecret: infrav1alpha1.SecretKeyRef{
					Name: "control-plane-public-key",
					Key:  "public.key",
				},
			},
			Database: testExternalDatabase(),
			Redis:    testExternalRedis(),
			TeamQuota: &infrav1alpha1.TeamQuotaConfig{
				StateID: testRegionStateID,
				Defaults: []infrav1alpha1.TeamQuotaPolicyConfig{{
					Key:   string(teamquota.KeySandboxRuntimeCount),
					Kind:  string(teamquota.KindCapacity),
					Limit: &limit,
				}},
			},
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
	if !containsString(compiled.Validation.FatalErrors, "consumer-only Team Quota configuration must omit spec.teamQuota.defaults") {
		t.Fatalf("expected consumer-only defaults validation error, got %#v", compiled.Validation.FatalErrors)
	}
}

func TestValidateTeamQuotaPolicyRateInterval(t *testing.T) {
	tokens := int64(10)
	burst := int64(20)
	tests := []struct {
		name     string
		interval time.Duration
		wantErr  bool
	}{
		{name: "minimum", interval: time.Millisecond},
		{name: "maximum", interval: time.Hour},
		{name: "zero", interval: 0, wantErr: true},
		{name: "negative", interval: -time.Millisecond, wantErr: true},
		{name: "fractional millisecond", interval: 1500 * time.Microsecond, wantErr: true},
		{name: "above maximum", interval: time.Hour + time.Millisecond, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interval := metav1.Duration{Duration: tt.interval}
			err := validateTeamQuotaPolicy(infrav1alpha1.TeamQuotaPolicyConfig{
				Key:      string(teamquota.KeyAPIRequests),
				Kind:     string(teamquota.KindRate),
				Tokens:   &tokens,
				Interval: &interval,
				Burst:    &burst,
			})
			if tt.wantErr && err == nil {
				t.Fatalf("validateTeamQuotaPolicy(%s) error = nil, want error", tt.interval)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validateTeamQuotaPolicy(%s) error = %v", tt.interval, err)
			}
		})
	}
}

func TestCompileDerivesCrossServiceReferences(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Region:  "aws-us-east-1",
			Network: &infrav1alpha1.NetworkConfig{},
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
			},
		},
	}

	compiled := Compile(infra)

	if !compiled.Components.EnableManager || !compiled.Components.EnableClusterGateway || !compiled.Components.EnableNetwork {
		t.Fatalf("expected manager, cluster-gateway, and network runtime to be enabled: %#v", compiled.Components)
	}
	if !compiled.Manager.TemplateStoreEnabled {
		t.Fatalf("expected template store to be enabled")
	}
	if got := compiled.Manager.NetworkPolicyProvider; got != "netd" {
		t.Fatalf("expected ctld network runtime provider, got %q", got)
	}
	if got := compiled.Services.Manager.URL; got != "http://demo-manager.sandbox0-system.svc.cluster.local:18080" {
		t.Fatalf("unexpected manager service URL: %q", got)
	}
	if got := compiled.Network.EgressAuthResolverURL; got != compiled.Services.Manager.URL {
		t.Fatalf("expected network runtime resolver URL to match manager service URL, got %q", got)
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
	if len(compiled.Manager.SandboxPodPlacement.Tolerations) != 1 || compiled.Manager.SandboxPodPlacement.Tolerations[0].Key != "sandbox0.ai/sandbox" {
		t.Fatalf("expected shared manager sandbox tolerations, got %#v", compiled.Manager.SandboxPodPlacement.Tolerations)
	}
	if !compiled.Scheduler.Enabled {
		t.Fatal("expected scheduler plan to be enabled")
	}
	if compiled.Scheduler.Config == nil || compiled.Network.Config == nil {
		t.Fatal("expected scheduler and network runtime configs to be compiled")
	}
	if compiled.Scheduler.HomeCluster == nil || compiled.Scheduler.HomeCluster.ClusterID != "cluster-a" {
		t.Fatalf("expected scheduler home cluster to be compiled, got %#v", compiled.Scheduler.HomeCluster)
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

func TestBuiltinTemplatesDefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	defaults := Compile(&infrav1alpha1.Sandbox0Infra{}).BuiltinTemplates()
	if len(defaults) != 5 {
		t.Fatalf("default builtin template count = %d, want 5", len(defaults))
	}
	wantDefaults := []string{
		template.DefaultTemplateID,
		template.OpenClawTemplateID,
		template.HermesTemplateID,
		template.BrowserTemplateID,
		template.CodingAgentTemplateID,
	}
	for i, want := range wantDefaults {
		if defaults[i].TemplateID != want {
			t.Fatalf("default builtin template[%d] = %q, want %q; all defaults=%#v", i, defaults[i].TemplateID, want, defaults)
		}
	}

	overridden := Compile(&infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			BuiltinTemplates: []infrav1alpha1.BuiltinTemplateConfig{{
				TemplateID: "default",
				Image:      "example.com/default:latest",
			}},
		},
	}).BuiltinTemplates()
	if len(overridden) != 1 {
		t.Fatalf("overridden builtin template count = %d, want 1", len(overridden))
	}
	if overridden[0].Image != "example.com/default:latest" {
		t.Fatalf("overridden image = %q, want custom image", overridden[0].Image)
	}

	empty := Compile(&infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			BuiltinTemplates: []infrav1alpha1.BuiltinTemplateConfig{},
		},
	}).BuiltinTemplates()
	if len(empty) != 0 {
		t.Fatalf("explicit empty builtin templates = %#v, want none", empty)
	}
}

func TestCompileEnablesClickHouseBeforeSandboxObservability(t *testing.T) {
	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{
				Type: infrav1alpha1.ClickHouseTypeExternal,
				External: &infrav1alpha1.ExternalClickHouseConfig{
					DSNSecret: infrav1alpha1.ClickHouseDSNSecretRef{Name: "clickhouse-dsn"},
				},
			},
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Enabled: &enabled,
				Backend: infrav1alpha1.SandboxObservabilityBackendClickHouse,
			},
		},
	}

	compiled := Compile(infra)

	if !compiled.Components.EnableClickHouse {
		t.Fatalf("expected clickhouse component to be enabled")
	}
	if !compiled.Components.EnableSandboxObservability {
		t.Fatalf("expected sandbox observability to be enabled")
	}
	clickHouseIndex := -1
	sandboxObsIndex := -1
	for i, step := range compiled.Workflow.Steps {
		switch step.Name {
		case "clickhouse":
			clickHouseIndex = i
		case "sandbox-observability":
			sandboxObsIndex = i
		}
	}
	if clickHouseIndex < 0 || sandboxObsIndex < 0 || clickHouseIndex > sandboxObsIndex {
		t.Fatalf("expected clickhouse step before sandbox-observability, got clickhouse=%d sandbox-observability=%d steps=%#v", clickHouseIndex, sandboxObsIndex, compiled.Workflow.Steps)
	}
}

func TestCompileEnablesMeteringAfterClickHouse(t *testing.T) {
	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			ClickHouse: &infrav1alpha1.ClickHouseConfig{
				Type: infrav1alpha1.ClickHouseTypeExternal,
				External: &infrav1alpha1.ExternalClickHouseConfig{
					DSNSecret: infrav1alpha1.ClickHouseDSNSecretRef{Name: "clickhouse-dsn"},
				},
			},
			Metering: &infrav1alpha1.MeteringConfig{
				Enabled: &enabled,
			},
		},
	}

	compiled := Compile(infra)

	if !compiled.Components.EnableClickHouse {
		t.Fatalf("expected clickhouse component to be enabled")
	}
	if !compiled.Components.EnableMetering {
		t.Fatalf("expected metering component to be enabled")
	}
	if len(compiled.Validation.FatalErrors) != 0 {
		t.Fatalf("fatal errors = %#v, want none", compiled.Validation.FatalErrors)
	}
	clickHouseIndex := -1
	meteringIndex := -1
	for i, step := range compiled.Workflow.Steps {
		switch step.Name {
		case "clickhouse":
			clickHouseIndex = i
		case "metering":
			meteringIndex = i
		}
	}
	if clickHouseIndex < 0 || meteringIndex < 0 || clickHouseIndex > meteringIndex {
		t.Fatalf("expected clickhouse step before metering, got clickhouse=%d metering=%d steps=%#v", clickHouseIndex, meteringIndex, compiled.Workflow.Steps)
	}
	if !containsString(compiled.Status.ExpectedConditions, infrav1alpha1.ConditionTypeMeteringReady) {
		t.Fatalf("expected MeteringReady condition, got %#v", compiled.Status.ExpectedConditions)
	}
}

func TestCompileRejectsMeteringWithoutClickHouse(t *testing.T) {
	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Metering: &infrav1alpha1.MeteringConfig{
				Enabled: &enabled,
			},
		},
	}

	compiled := Compile(infra)

	found := false
	for _, msg := range compiled.Validation.FatalErrors {
		if strings.Contains(msg, "metering requires spec.clickHouse type builtin or external") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected metering clickhouse dependency error, got %#v", compiled.Validation.FatalErrors)
	}
}

func TestCompileDefaultsDataPlaneIdentityFromPublicExposure(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "fullmode",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			PublicExposure: &infrav1alpha1.PublicExposureConfig{
				RegionID: "aws-us-east-1",
			},
			Network: &infrav1alpha1.NetworkConfig{},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	if got := compiled.Manager.RegionID; got != "aws-us-east-1" {
		t.Fatalf("manager region ID = %q, want aws-us-east-1", got)
	}
	if got := compiled.Manager.DefaultClusterID; got != naming.DefaultClusterID {
		t.Fatalf("manager default cluster ID = %q, want %q", got, naming.DefaultClusterID)
	}
	if got := compiled.Manager.Config.RegionID; got != "aws-us-east-1" {
		t.Fatalf("manager config region ID = %q, want aws-us-east-1", got)
	}
	if got := compiled.Manager.Config.DefaultClusterId; got != naming.DefaultClusterID {
		t.Fatalf("manager config default cluster ID = %q, want %q", got, naming.DefaultClusterID)
	}
	if got := compiled.Network.RegionID; got != "aws-us-east-1" {
		t.Fatalf("network runtime region ID = %q, want aws-us-east-1", got)
	}
	if got := compiled.Network.ClusterID; got != naming.DefaultClusterID {
		t.Fatalf("network runtime cluster ID = %q, want %q", got, naming.DefaultClusterID)
	}
	if got := compiled.RegionalGateway.Config.RegionID; got != "aws-us-east-1" {
		t.Fatalf("regional gateway config region ID = %q, want aws-us-east-1", got)
	}
}

func TestCompileStorageAndNetworkRuntimeConfig(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Storage: &infrav1alpha1.StorageConfig{
				Runtime: &infrav1alpha1.StorageProxyConfig{HTTPPort: 18082, CacheSizeLimit: "2Gi"},
			},
			Network: &infrav1alpha1.NetworkConfig{
				MITMCASecretName: "canonical-mitm-ca",
				Config: &infrav1alpha1.NetdConfig{
					EgressAuthResolverURL: "http://canonical-resolver:9000",
					MetricsPort:           19091,
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
					EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					Replicas:             2,
				}},
			},
		},
	}

	compiled := Compile(infra)

	if !compiled.Components.EnableStorageRuntime || !compiled.Components.EnableNetwork {
		t.Fatalf("storage and network runtimes were not enabled: %#v", compiled.Components)
	}
	if compiled.Manager.Replicas != 2 {
		t.Fatalf("manager replicas = %d, want canonical process replicas 2", compiled.Manager.Replicas)
	}
	if got := compiled.Services.ManagerStorage.Name; got != "demo-manager" {
		t.Fatalf("storage runtime service name = %q, want manager", got)
	}
	if got := compiled.Services.ManagerStorage.URL; got != "http://demo-manager.sandbox0-system.svc.cluster.local:18082" {
		t.Fatalf("storage runtime URL = %q, want manager storage port", got)
	}
	if got := compiled.Services.ManagerStorage.Port; got != 18082 {
		t.Fatalf("manager storage runtime port = %d, want 18082", got)
	}
	if got := compiled.Network.Config.EgressAuthResolverURL; got != "http://canonical-resolver:9000" {
		t.Fatalf("network resolver URL = %q, want canonical config", got)
	}
	if got := compiled.Network.Config.MetricsPort; got != 19091 {
		t.Fatalf("network metrics port = %d, want canonical config", got)
	}
	if got := compiled.ResolveNetdMITMCASecretName(); got != "canonical-mitm-ca" {
		t.Fatalf("network MITM CA secret = %q, want canonical-mitm-ca", got)
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

func TestCompilePreservesExplicitNetworkResolverURL(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Network: &infrav1alpha1.NetworkConfig{
				Config: &infrav1alpha1.NetdConfig{
					EgressAuthResolverURL: "http://explicit-resolver:9000",
				},
			},
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

	if got := compiled.Network.EgressAuthResolverURL; got != "http://explicit-resolver:9000" {
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

func TestCompileRequiresClusterGatewayEnterpriseLicenseForSandboxAudit(t *testing.T) {
	enabled := true
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxObservability: &infrav1alpha1.SandboxObservabilityConfig{
				Enabled: &enabled,
				Backend: infrav1alpha1.SandboxObservabilityBackendClickHouse,
				Audit:   &infrav1alpha1.SandboxObservabilityAuditConfig{Enabled: true},
			},
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

	if !compiled.Enterprise.ClusterGateway {
		t.Fatal("expected sandbox audit to require a cluster-gateway enterprise license")
	}
	if !containsString(workflowStepNames(compiled.Workflow.Steps), "cluster-gateway-enterprise-license") {
		t.Fatalf("expected enterprise license workflow step, got %#v", workflowStepNames(compiled.Workflow.Steps))
	}

	infra.Spec.SandboxObservability.Audit.Enabled = false
	compiled = Compile(infra)
	if compiled.Enterprise.ClusterGateway {
		t.Fatal("did not expect observability without audit to require a cluster-gateway enterprise license")
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
	t.Run("global identity overload guard requires Redis", func(t *testing.T) {
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
		if !containsString(
			compiled.Validation.FatalErrors,
			"public identity overload guard requires spec.redis type builtin or external",
		) {
			t.Fatalf("expected overload guard Redis validation error, got %#v", compiled.Validation.FatalErrors)
		}

		infra.Spec.Redis = testExternalRedis()
		compiled = Compile(infra)
		if containsString(
			compiled.Validation.FatalErrors,
			"public identity overload guard requires spec.redis type builtin or external",
		) {
			t.Fatalf("shared Redis was not accepted: %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("regional Team Quota owner requires complete defaults", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Redis: testExternalRedis(),
				Services: &infrav1alpha1.ServicesConfig{
					RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
					},
				},
			},
		}

		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, "region Team Quota owner requires spec.teamQuota with one complete policy for all 21 keys") {
			t.Fatalf("expected Team Quota owner validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("Team Quota consumers require trusted region state ID", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Region:       "region-1",
				ControlPlane: &infrav1alpha1.ControlPlaneConfig{},
				Database:     testExternalDatabase(),
				Redis:        testExternalRedis(),
				Services: &infrav1alpha1.ServicesConfig{
					Manager: &infrav1alpha1.ManagerServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
					},
				},
			},
		}
		const message = "Team Quota consumers require spec.teamQuota.stateId to be a canonical UUID v4 copied from the region owner's status.teamQuota.stateId"

		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, message) {
			t.Fatalf("expected missing state ID error, got %#v", compiled.Validation.FatalErrors)
		}

		infra.Spec.TeamQuota = &infrav1alpha1.TeamQuotaConfig{StateID: "not-a-uuid"}
		compiled = Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, message) {
			t.Fatalf("expected invalid state ID error, got %#v", compiled.Validation.FatalErrors)
		}

		infra.Spec.TeamQuota.StateID = testRegionStateID
		compiled = Compile(infra)
		if containsString(compiled.Validation.FatalErrors, message) {
			t.Fatalf("valid state ID was rejected: %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("fullmode cluster gateway Team Quota owner requires complete defaults", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Redis: testExternalRedis(),
				Services: &infrav1alpha1.ServicesConfig{
					ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
						Config: &infrav1alpha1.ClusterGatewayConfig{AuthMode: "public"},
					},
				},
			},
		}

		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, "region Team Quota owner requires spec.teamQuota with one complete policy for all 21 keys") {
			t.Fatalf("expected fullmode Team Quota owner validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("Team Quota consumers require a region ID", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Database:  testExternalDatabase(),
				Redis:     testExternalRedis(),
				TeamQuota: testCompleteTeamQuota(),
				Services: &infrav1alpha1.ServicesConfig{
					ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
						Config: &infrav1alpha1.ClusterGatewayConfig{AuthMode: "public"},
					},
				},
			},
		}

		compiled := Compile(infra)
		const message = "Team Quota consumers require a non-empty region ID from spec.region or spec.publicExposure.regionId"
		if !containsString(compiled.Validation.FatalErrors, message) {
			t.Fatalf("expected missing region ID error, got %#v", compiled.Validation.FatalErrors)
		}

		infra.Spec.Region = "private-region"
		compiled = Compile(infra)
		if containsString(compiled.Validation.FatalErrors, message) {
			t.Fatalf("explicit canonical region ID was rejected: %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("local consumers require a policy owner", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Database: testExternalDatabase(),
				Redis:    testExternalRedis(),
				Services: &infrav1alpha1.ServicesConfig{
					ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
						Config: &infrav1alpha1.ClusterGatewayConfig{AuthMode: "internal"},
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
		if !containsString(
			compiled.Validation.FatalErrors,
			"Team Quota consumers require a regional-gateway or fullmode cluster-gateway policy owner, or spec.controlPlane for an external region policy owner",
		) {
			t.Fatalf("expected missing Team Quota policy owner error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("scheduler is a PostgreSQL-only Team Quota consumer", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Region:    "region-1",
				Database:  testExternalDatabase(),
				TeamQuota: &infrav1alpha1.TeamQuotaConfig{StateID: testRegionStateID},
				Services: &infrav1alpha1.ServicesConfig{
					Scheduler: &infrav1alpha1.SchedulerServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
					},
				},
			},
		}

		const ownerMessage = "Team Quota consumers require a regional-gateway or fullmode cluster-gateway policy owner, or spec.controlPlane for an external region policy owner"
		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, ownerMessage) {
			t.Fatalf("expected scheduler-only policy owner error, got %#v", compiled.Validation.FatalErrors)
		}
		if containsString(compiled.Validation.FatalErrors, "Team Quota distributed consumers require spec.redis type builtin or external") {
			t.Fatalf("PostgreSQL-only scheduler incorrectly required Redis: %#v", compiled.Validation.FatalErrors)
		}

		infra.Spec.ControlPlane = &infrav1alpha1.ControlPlaneConfig{}
		compiled = Compile(infra)
		if containsString(compiled.Validation.FatalErrors, ownerMessage) {
			t.Fatalf("external scheduler policy owner was not accepted: %#v", compiled.Validation.FatalErrors)
		}
		if containsString(compiled.Validation.FatalErrors, "Team Quota distributed consumers require spec.redis type builtin or external") {
			t.Fatalf("external PostgreSQL-only scheduler incorrectly required Redis: %#v", compiled.Validation.FatalErrors)
		}
		if compiled.Scheduler.Config.RegionID != "region-1" ||
			compiled.Scheduler.Config.TeamQuotaStateID != testRegionStateID {
			t.Fatalf("scheduler state identity config = %#v", compiled.Scheduler.Config)
		}

		infra.Spec.Database = &infrav1alpha1.DatabaseConfig{
			Type: infrav1alpha1.DatabaseTypeBuiltin,
			Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
				Enabled: true,
			},
		}
		compiled = Compile(infra)
		if !containsString(
			compiled.Validation.FatalErrors,
			"consumer-only Team Quota services require spec.database.type external so they share the region PostgreSQL",
		) {
			t.Fatalf("expected scheduler shared PostgreSQL error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("external data plane omits owner defaults but requires Redis", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				ControlPlane: &infrav1alpha1.ControlPlaneConfig{},
				Database:     testExternalDatabase(),
				Redis:        testExternalRedis(),
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
		if containsString(compiled.Validation.FatalErrors, "region Team Quota owner requires spec.teamQuota with one complete policy for all 21 keys") {
			t.Fatalf("external data plane was incorrectly treated as policy owner: %#v", compiled.Validation.FatalErrors)
		}
		if containsString(compiled.Validation.FatalErrors, "Team Quota distributed consumers require spec.redis type builtin or external") {
			t.Fatalf("external data plane Redis was not accepted: %#v", compiled.Validation.FatalErrors)
		}
		if containsString(compiled.Validation.FatalErrors, "consumer-only distributed Team Quota services require spec.redis.type external so they share the region Redis") {
			t.Fatalf("external data plane shared Redis was not accepted: %#v", compiled.Validation.FatalErrors)
		}
		if containsString(compiled.Validation.FatalErrors, "consumer-only Team Quota services require spec.database.type external so they share the region PostgreSQL") {
			t.Fatalf("external data plane shared PostgreSQL was not accepted: %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("external data plane rejects cluster-local builtin Redis", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				ControlPlane: &infrav1alpha1.ControlPlaneConfig{},
				Database:     testExternalDatabase(),
				Redis: &infrav1alpha1.RedisConfig{
					Type: infrav1alpha1.RedisTypeBuiltin,
					Builtin: &infrav1alpha1.BuiltinRedisConfig{
						Enabled: true,
					},
				},
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
		if !containsString(compiled.Validation.FatalErrors, "consumer-only distributed Team Quota services require spec.redis.type external so they share the region Redis") {
			t.Fatalf("expected shared external Redis validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("external data plane rejects cluster-local builtin PostgreSQL", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				ControlPlane: &infrav1alpha1.ControlPlaneConfig{},
				Database: &infrav1alpha1.DatabaseConfig{
					Type: infrav1alpha1.DatabaseTypeBuiltin,
				},
				Redis: testExternalRedis(),
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
		if !containsString(compiled.Validation.FatalErrors, "consumer-only Team Quota services require spec.database.type external so they share the region PostgreSQL") {
			t.Fatalf("expected shared external PostgreSQL validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("Team Quota distributed consumer requires Redis", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				ControlPlane: &infrav1alpha1.ControlPlaneConfig{},
				Database:     testExternalDatabase(),
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
		if !containsString(compiled.Validation.FatalErrors, "Team Quota distributed consumers require spec.redis type builtin or external") {
			t.Fatalf("expected Team Quota Redis validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("Team Quota owner rejects a semantically invalid policy", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Redis:     testExternalRedis(),
				TeamQuota: testCompleteTeamQuota(),
				Services: &infrav1alpha1.ServicesConfig{
					RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
					},
				},
			},
		}
		for index := range infra.Spec.TeamQuota.Defaults {
			if infra.Spec.TeamQuota.Defaults[index].Kind == string(teamquota.KindCapacity) {
				*infra.Spec.TeamQuota.Defaults[index].Limit = -1
				break
			}
		}

		compiled := Compile(infra)
		joined := strings.Join(compiled.Validation.FatalErrors, "\n")
		if !strings.Contains(joined, "default policy") || !strings.Contains(joined, "non-negative") {
			t.Fatalf("expected semantic Team Quota policy error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("Team Quota owner rejects a negative policy cache TTL", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Redis:     testExternalRedis(),
				TeamQuota: testCompleteTeamQuota(),
				Services: &infrav1alpha1.ServicesConfig{
					RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
						},
					},
				},
			},
		}
		infra.Spec.TeamQuota.DistributedEnforcement.PolicyCacheTTL.Duration = -time.Second

		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, "spec.teamQuota.distributedEnforcement.policyCacheTtl must be non-negative") {
			t.Fatalf("expected Team Quota cache TTL validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("Team Quota leases require whole milliseconds", func(t *testing.T) {
		for _, test := range []struct {
			name    string
			mutate  func(*infrav1alpha1.TeamQuotaDistributedEnforcementConfig)
			message string
		}{
			{
				name: "lease TTL",
				mutate: func(config *infrav1alpha1.TeamQuotaDistributedEnforcementConfig) {
					config.LeaseTTL.Duration = 15*time.Millisecond + 500*time.Microsecond
					config.RenewInterval.Duration = 5 * time.Millisecond
				},
				message: "spec.teamQuota.distributedEnforcement.leaseTtl must use whole milliseconds",
			},
			{
				name: "renew interval",
				mutate: func(config *infrav1alpha1.TeamQuotaDistributedEnforcementConfig) {
					config.LeaseTTL.Duration = 15 * time.Millisecond
					config.RenewInterval.Duration = 5*time.Millisecond + 500*time.Microsecond
				},
				message: "spec.teamQuota.distributedEnforcement.renewInterval must use whole milliseconds",
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				infra := &infrav1alpha1.Sandbox0Infra{
					Spec: infrav1alpha1.Sandbox0InfraSpec{
						Region:    "test-region",
						Database:  testExternalDatabase(),
						Redis:     testExternalRedis(),
						TeamQuota: testCompleteTeamQuota(),
						Services: &infrav1alpha1.ServicesConfig{
							RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
								WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
									EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
								},
							},
						},
					},
				}
				test.mutate(&infra.Spec.TeamQuota.DistributedEnforcement)

				compiled := Compile(infra)
				if !containsString(compiled.Validation.FatalErrors, test.message) {
					t.Fatalf("expected %q, got %#v", test.message, compiled.Validation.FatalErrors)
				}
			})
		}
	})

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

	t.Run("network without manager is invalid", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Network: &infrav1alpha1.NetworkConfig{
					Config: &infrav1alpha1.NetdConfig{
						EgressAuthEnabled: true,
					},
				},
			},
		}

		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, "network requires services.manager to be enabled") {
			t.Fatalf("expected network/manager validation error, got %#v", compiled.Validation.FatalErrors)
		}
		if !containsString(compiled.Validation.FatalErrors, "network egress auth requires manager to be enabled") {
			t.Fatalf("expected network egress-auth/manager validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("storage runtime requires at least one manager replica", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Storage: &infrav1alpha1.StorageConfig{
					Runtime: &infrav1alpha1.StorageProxyConfig{},
				},
				Services: &infrav1alpha1.ServicesConfig{
					Manager: &infrav1alpha1.ManagerServiceConfig{
						WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
							EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
							Replicas:             0,
						},
					},
				},
			},
		}

		compiled := Compile(infra)
		if !containsString(compiled.Validation.FatalErrors, "manager replicas must be at least 1 when the storage API is enabled") {
			t.Fatalf("expected manager replica validation error, got %#v", compiled.Validation.FatalErrors)
		}
	})

	t.Run("manager service ports must be distinct", func(t *testing.T) {
		infra := &infrav1alpha1.Sandbox0Infra{
			Spec: infrav1alpha1.Sandbox0InfraSpec{
				Storage: &infrav1alpha1.StorageConfig{
					Runtime: &infrav1alpha1.StorageProxyConfig{HTTPPort: 9090},
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

		compiled := Compile(infra)
		want := "manager Service port 9090 is configured by both services.manager.config.metricsPort and storage.runtime.httpPort; use distinct ports"
		if !containsString(compiled.Validation.FatalErrors, want) {
			t.Fatalf("expected manager Service port validation error, got %#v", compiled.Validation.FatalErrors)
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

func testExternalRedis() *infrav1alpha1.RedisConfig {
	return &infrav1alpha1.RedisConfig{
		Type: infrav1alpha1.RedisTypeExternal,
		External: &infrav1alpha1.ExternalRedisConfig{
			URLSecret: infrav1alpha1.RedisURLSecretRef{Name: "redis-url", Key: "url"},
		},
	}
}

func testExternalDatabase() *infrav1alpha1.DatabaseConfig {
	return &infrav1alpha1.DatabaseConfig{
		Type: infrav1alpha1.DatabaseTypeExternal,
		External: &infrav1alpha1.ExternalDatabaseConfig{
			Host:     "db.example.com",
			Port:     5432,
			Database: "sandbox0",
			Username: "sandbox0",
		},
	}
}

func testCompleteTeamQuota() *infrav1alpha1.TeamQuotaConfig {
	config := &infrav1alpha1.TeamQuotaConfig{
		StateID:  testRegionStateID,
		Defaults: make([]infrav1alpha1.TeamQuotaPolicyConfig, 0, len(teamquota.Keys())),
	}
	for _, key := range teamquota.Keys() {
		kind, _ := teamquota.KindForKey(key)
		policy := infrav1alpha1.TeamQuotaPolicyConfig{
			Key:  string(key),
			Kind: string(kind),
		}
		switch kind {
		case teamquota.KindCapacity, teamquota.KindConcurrency:
			value := int64(100)
			policy.Limit = &value
		case teamquota.KindRate:
			tokens := int64(10)
			burst := int64(20)
			interval := metav1.Duration{Duration: time.Second}
			policy.Tokens = &tokens
			policy.Burst = &burst
			policy.Interval = &interval
		}
		config.Defaults = append(config.Defaults, policy)
	}
	return config
}

func TestCompileStartsFullmodeTeamQuotaOwnerBeforeConsumers(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "fullmode",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			PublicExposure: &infrav1alpha1.PublicExposureConfig{
				RegionID: "aws-us-east-1",
			},
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					Enabled: true,
				},
			},
			Redis: &infrav1alpha1.RedisConfig{
				Type: infrav1alpha1.RedisTypeBuiltin,
				Builtin: &infrav1alpha1.BuiltinRedisConfig{
					Enabled: true,
				},
			},
			TeamQuota: testCompleteTeamQuota(),
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
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
	names := workflowStepNames(compiled.Workflow.Steps)
	clusterGatewayIndex := -1
	managerIndex := -1
	for index, name := range names {
		switch name {
		case "cluster-gateway":
			clusterGatewayIndex = index
		case "manager":
			managerIndex = index
		}
	}
	if clusterGatewayIndex < 0 || managerIndex < 0 {
		t.Fatalf("missing fullmode workflow steps: %#v", names)
	}
	if clusterGatewayIndex >= managerIndex {
		t.Fatalf("fullmode Team Quota owner must start before manager: %#v", names)
	}
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
			Storage: &infrav1alpha1.StorageConfig{
				Runtime: &infrav1alpha1.StorageProxyConfig{},
			},
			Network: &infrav1alpha1.NetworkConfig{},
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
			},
		},
	}

	compiled := Compile(infra)

	got := workflowStepNames(compiled.Workflow.Steps)
	want := []string{
		"control-plane-public-key",
		"internal-auth",
		"database",
		"storage",
		"global-gateway-enterprise-license",
		"global-gateway",
		"init-user-secret",
		"regional-gateway-enterprise-license",
		"regional-gateway",
		"scheduler-enterprise-license",
		"scheduler-rbac",
		"scheduler",
		"manager-rbac",
		"manager",
		"cluster-gateway-enterprise-license",
		"cluster-gateway",
		"storage-runtime-ready",
		"ctld",
		"ctld-ready",
		"network-ready",
		"data-plane-node-readiness",
		"builtin-template-pods",
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

func TestCompileTracksObservabilityBackendIntegration(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Observability: &infrav1alpha1.ObservabilityConfig{
				Backend: &infrav1alpha1.ObservabilityBackendConfig{
					Type: infrav1alpha1.ObservabilityBackendTypeExternal,
					External: &infrav1alpha1.ExternalObservabilityBackendConfig{
						Mode: infrav1alpha1.ObservabilityExternalModeManagedCollector,
						OTLP: &infrav1alpha1.ObservabilityOTLPConfig{
							Endpoint: "otel.example.com:4317",
						},
					},
				},
			},
		},
	}

	compiled := Compile(infra)

	if !compiled.Components.EnableObservability {
		t.Fatal("expected observability backend integration to be enabled")
	}
	if !containsString(workflowStepNames(compiled.Workflow.Steps), "observability") {
		t.Fatalf("expected observability workflow step, got %#v", workflowStepNames(compiled.Workflow.Steps))
	}
	if !containsString(compiled.Status.ExpectedConditions, infrav1alpha1.ConditionTypeObservabilityReady) {
		t.Fatalf("expected observability condition, got %#v", compiled.Status.ExpectedConditions)
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
		{Kind: "DaemonSet", Namespace: "sandbox0-system", Name: "demo-ctld-a"},
		{Kind: "DaemonSet", Namespace: "sandbox0-system", Name: "demo-ctld-b"},
		{Kind: "Service", Namespace: "sandbox0-system", Name: "demo-ctld-network-metrics"},
		{Kind: "ConfigMap", Namespace: "sandbox0-system", Name: "demo-ctld"},
		{Kind: "ConfigMap", Namespace: "sandbox0-system", Name: "demo-netd"},
		{Kind: "StatefulSet", Namespace: "sandbox0-system", Name: "demo-postgres"},
		{Kind: "Deployment", Namespace: "sandbox0-system", Name: "demo-egress-broker"},
		{Kind: "ClusterRole", Name: "demo-manager"},
		{Kind: "ClusterRoleBinding", Name: "demo-ctld"},
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
