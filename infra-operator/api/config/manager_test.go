package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fsrollout"
)

func TestS0FSRolloutControlsPreserveLegacySharedPoolBehavior(t *testing.T) {
	cfg := &ManagerConfig{SharedCarrierPool: SharedCarrierPoolConfig{Enabled: true}}
	if !cfg.TemplateImageFSEnabled() || !cfg.S0FSRuntimeEnabled() {
		t.Fatal("legacy shared carrier switch did not enable import and runtime capability")
	}
	admission, err := cfg.S0FSAdmission()
	if err != nil {
		t.Fatalf("S0FSAdmission() error = %v", err)
	}
	if admission.Mode() != s0fsrollout.AdmissionModeShared || !admission.Admits(naming.ScopePublic, "", "template-a") {
		t.Fatalf("legacy admission mode = %q, want shared admit-all", admission.Mode())
	}
}

func TestS0FSRolloutControlsAllowShadowImportWithAdmissionOff(t *testing.T) {
	enabled := true
	cfg := &ManagerConfig{
		TemplateImageFS: TemplateImageFSConfig{Enabled: &enabled},
		S0FSRuntime: S0FSRuntimeConfig{Enabled: true, Admission: S0FSAdmissionConfig{
			Mode: "off", TeamIDs: []string{"team-a"}, RejectLegacyClaims: true,
		}},
	}
	if !cfg.TemplateImageFSEnabled() || !cfg.S0FSRuntimeEnabled() {
		t.Fatal("explicit shadow import configuration is disabled")
	}
	admission, err := cfg.S0FSAdmission()
	if err != nil {
		t.Fatalf("S0FSAdmission() error = %v", err)
	}
	if admission.Admits(naming.ScopeTeam, "team-a", "template-a") || !admission.RejectLegacyClaims() {
		t.Fatal("off admission did not remain fail-closed")
	}
}

func TestEffectiveRuntimeReadyTimeout(t *testing.T) {
	tests := []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{name: "unset", want: 5 * time.Minute},
		{name: "legacy short value", configured: 90 * time.Second, want: 5 * time.Minute},
		{name: "longer override", configured: 10 * time.Minute, want: 10 * time.Minute},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := EffectiveRuntimeReadyTimeout(test.configured); got != test.want {
				t.Fatalf("EffectiveRuntimeReadyTimeout(%s) = %s, want %s", test.configured, got, test.want)
			}
		})
	}
}

func TestIdlePodRepairGracePeriodFollowsRuntimeReadyTimeout(t *testing.T) {
	if got, want := IdlePodRepairGracePeriod(10*time.Minute), 10*time.Minute+30*time.Second; got != want {
		t.Fatalf("IdlePodRepairGracePeriod() = %s, want %s", got, want)
	}
}

func TestLoadManagerConfigPreservesDefaultTeamQuotas(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manager.yaml")
	if err := os.WriteFile(path, []byte(`
default_team_quotas:
  - dimension: active_sandboxes
    limit_value: 3
  - dimension: sandbox_claims
    limit_value: 5
    interval_ms: 1000
    burst_value: 5
  - dimension: api_requests
    limit_value: 100
    interval_ms: 1000
    burst_value: 200
`), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}

	cfg, err := loadManagerConfig(path)
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	if len(cfg.DefaultTeamQuotas) != 3 {
		t.Fatalf("default team quotas len = %d, want 3", len(cfg.DefaultTeamQuotas))
	}
	if cfg.DefaultTeamQuotas[0].Dimension != "active_sandboxes" || cfg.DefaultTeamQuotas[0].LimitValue != 3 {
		t.Fatalf("first default quota = %+v, want active_sandboxes=3", cfg.DefaultTeamQuotas[0])
	}
	if cfg.DefaultTeamQuotas[1].Dimension != "sandbox_claims" ||
		cfg.DefaultTeamQuotas[1].LimitValue != 5 ||
		cfg.DefaultTeamQuotas[1].IntervalMS != 1000 ||
		cfg.DefaultTeamQuotas[1].BurstValue != 5 {
		t.Fatalf("second default quota = %+v, want sandbox_claims rate policy", cfg.DefaultTeamQuotas[1])
	}
	if cfg.DefaultTeamQuotas[2].Dimension != "api_requests" ||
		cfg.DefaultTeamQuotas[2].IntervalMS != 1000 ||
		cfg.DefaultTeamQuotas[2].BurstValue != 200 {
		t.Fatalf("third default quota = %+v, want api_requests rate policy", cfg.DefaultTeamQuotas[2])
	}
}

func TestLoadManagerConfigPreservesSandboxMaxMemory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manager.yaml")
	if err := os.WriteFile(path, []byte(`
sandbox_max_memory: 16Gi
`), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}

	cfg, err := loadManagerConfig(path)
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	if cfg.SandboxMaxMemory != "16Gi" {
		t.Fatalf("sandbox max memory = %q, want 16Gi", cfg.SandboxMaxMemory)
	}
}

func TestLoadManagerConfigPreservesPreferredNodeSelector(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manager.yaml")
	if err := os.WriteFile(path, []byte(`
sandbox_pod_placement:
  preferred_node_selector:
    sandbox0.ai/capacity-type: fixed
`), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}

	cfg, err := loadManagerConfig(path)
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	if got := cfg.SandboxPodPlacement.PreferredNodeSelector["sandbox0.ai/capacity-type"]; got != "fixed" {
		t.Fatalf("preferred capacity type = %q, want fixed", got)
	}
}

func TestLoadManagerConfigDefaultsPodTeardownLimits(t *testing.T) {
	cfg, err := loadManagerConfig("")
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	applyPodTeardownDefaults(cfg)
	if cfg.PodTeardown.MaxConcurrentPerNode != 4 ||
		cfg.PodTeardown.MaxConcurrentPerDegradedNode != 1 ||
		cfg.PodTeardown.MaxConcurrentReplacements != 40 {
		t.Fatalf("pod teardown defaults = %#v", cfg.PodTeardown)
	}
}

func TestLoadManagerConfigPreservesPodLifecyclePlatformConfig(t *testing.T) {
	path := writeManagerConfigFile(t, `
autoscaler_safe_to_evict_annotation_keys:
  - goatscaler.io/safe-to-evict
pod_teardown:
  max_concurrent_per_node: 6
  max_concurrent_per_degraded_node: 2
  max_concurrent_replacements: 24
`)
	cfg, err := loadManagerConfig(path)
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	applyPodTeardownDefaults(cfg)
	if len(cfg.AutoscalerSafeToEvictAnnotationKeys) != 1 || cfg.AutoscalerSafeToEvictAnnotationKeys[0] != "goatscaler.io/safe-to-evict" {
		t.Fatalf("autoscaler annotation keys = %#v", cfg.AutoscalerSafeToEvictAnnotationKeys)
	}
	if cfg.PodTeardown.MaxConcurrentPerNode != 6 ||
		cfg.PodTeardown.MaxConcurrentPerDegradedNode != 2 ||
		cfg.PodTeardown.MaxConcurrentReplacements != 24 {
		t.Fatalf("pod teardown config = %#v", cfg.PodTeardown)
	}
}

func TestLoadManagerConfigDefaultsLeaderElectionOn(t *testing.T) {
	cfg, err := loadManagerConfig("")
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	if !cfg.LeaderElection {
		t.Fatal("leader election default = false, want true")
	}
}

func TestLoadManagerConfigAllowsLeaderElectionOff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manager.yaml")
	if err := os.WriteFile(path, []byte("leader_election: false\n"), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}

	cfg, err := loadManagerConfig(path)
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	if cfg.LeaderElection {
		t.Fatal("leader election = true, want explicit false")
	}
}

func writeManagerConfigFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "manager.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write manager config: %v", err)
	}
	return path
}
