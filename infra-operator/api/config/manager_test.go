package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
	if cfg.SandboxRuntimeBackend != SandboxRuntimeBackendKubernetes {
		t.Fatalf("sandbox runtime backend = %q, want kubernetes", cfg.SandboxRuntimeBackend)
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

func TestLoadManagerConfigPreservesNodeAuthority(t *testing.T) {
	path := writeManagerConfigFile(t, `
sandbox_runtime_backend: nomad
node_authority:
  enabled: true
  listen_host: 172.16.100.2
  port: 9444
  tls_secret_name: manager-node-tls
  cert_file: /tls/server.crt
  key_file: /tls/server.key
  client_ca_file: /tls/client-ca.crt
  writer_lease_ttl:
    duration: 20s
  writer_renewal_grace:
    duration: 3s
  runtime_slot_heartbeat_ttl:
    duration: 25s
  identities:
    - common_name: node-agent-1
      cluster_id: cluster-1
      node_id: node-1
      node_uid: node-uid-1
      pod_uid: agent-1
  claim:
    secret_name: manager-nomad-claim
    profile_catalog_file: /claim/profiles.json
    writer_token_key_file: /claim/writer.key
    claim_ttl:
      duration: 12s
    slo:
      duration: 750ms
  terminal:
    enabled: true
    control_secret_name: manager-nomad-control
    nomad_endpoints_file: /control/nomad.json
    interval:
      duration: 2s
    pass_timeout:
      duration: 1m
    scan_limit: 64
`)
	cfg, err := loadManagerConfig(path)
	if err != nil {
		t.Fatalf("loadManagerConfig: %v", err)
	}
	node := cfg.NodeAuthority
	if cfg.SandboxRuntimeBackend != SandboxRuntimeBackendNomad || !node.Enabled || node.ListenHost != "172.16.100.2" || node.Port != 9444 ||
		node.TLSSecretName != "manager-node-tls" || node.CertFile != "/tls/server.crt" ||
		node.WriterLeaseTTL.Duration != 20*time.Second || node.WriterRenewalGrace.Duration != 3*time.Second ||
		node.RuntimeSlotHeartbeatTTL.Duration != 25*time.Second || len(node.Identities) != 1 {
		t.Fatalf("node authority config = %#v", node)
	}
	if node.Identities[0].NodeUID != "node-uid-1" || node.Terminal.ControlSecretName != "manager-nomad-control" ||
		node.Terminal.NomadEndpointsFile != "/control/nomad.json" || node.Terminal.Interval.Duration != 2*time.Second ||
		node.Terminal.PassTimeout.Duration != time.Minute || node.Terminal.ScanLimit != 64 {
		t.Fatalf("node authority nested config = %#v", node)
	}
	if node.Claim.SecretName != "manager-nomad-claim" || node.Claim.ProfileCatalogFile != "/claim/profiles.json" ||
		node.Claim.WriterTokenKeyFile != "/claim/writer.key" || node.Claim.ClaimTTL.Duration != 12*time.Second ||
		node.Claim.SLO.Duration != 750*time.Millisecond {
		t.Fatalf("node authority claim config = %#v", node.Claim)
	}
}

func TestSandboxRuntimeDefaultsNomadClaimPolicy(t *testing.T) {
	cfg := &ManagerConfig{SandboxRuntimeBackend: SandboxRuntimeBackendNomad}
	applySandboxRuntimeDefaults(cfg)
	if cfg.NodeAuthority.Claim.ProfileCatalogFile != NodeAuthorityRuntimeProfilesPath ||
		cfg.NodeAuthority.Claim.WriterTokenKeyFile != NodeAuthorityWriterTokenKeyPath ||
		cfg.NodeAuthority.Claim.ClaimTTL.Duration != 15*time.Second ||
		cfg.NodeAuthority.Claim.SLO.Duration != time.Second {
		t.Fatalf("Nomad claim defaults = %#v", cfg.NodeAuthority.Claim)
	}
}

func TestNodeAuthorityDefaultsApplyOnlyWhenEnabled(t *testing.T) {
	disabled := &ManagerConfig{}
	applyNodeAuthorityDefaults(disabled)
	if disabled.NodeAuthority.Port != 0 || disabled.NodeAuthority.WriterLeaseTTL.Duration != 0 {
		t.Fatalf("disabled node authority acquired defaults: %#v", disabled.NodeAuthority)
	}

	enabled := &ManagerConfig{NodeAuthority: NodeAuthorityConfig{Enabled: true}}
	applyNodeAuthorityDefaults(enabled)
	if enabled.NodeAuthority.Port != 8421 || enabled.NodeAuthority.WriterLeaseTTL.Duration != 30*time.Second ||
		enabled.NodeAuthority.WriterRenewalGrace.Duration != 5*time.Second ||
		enabled.NodeAuthority.RuntimeSlotHeartbeatTTL.Duration != 30*time.Second {
		t.Fatalf("node authority defaults = %#v", enabled.NodeAuthority)
	}
}

func TestRootFSMaintenanceDefaultsIncludeCompositeMaterializer(t *testing.T) {
	cfg := &ManagerConfig{}
	applyRootFSMaintenanceDefaults(cfg)
	if cfg.RootFSMaintenance.MaterializerInterval.Duration != time.Second ||
		cfg.RootFSMaintenance.MaterializerScanLimit != 1000 ||
		cfg.RootFSMaintenance.MaterializerMinPackBytes != 32<<20 ||
		cfg.RootFSMaintenance.MaterializerMaxDelay.Duration != 5*time.Minute ||
		cfg.RootFSMaintenance.MaterializerForcedFlushesPerRun != 1 ||
		cfg.RootFSMaintenance.MaterializerGarbageInterval.Duration != time.Minute ||
		cfg.RootFSMaintenance.MaterializerUploadingStale.Duration != time.Hour ||
		cfg.RootFSMaintenance.MaterializerTerminalRetention.Duration != 24*time.Hour {
		t.Fatalf("rootfs materializer defaults = %#v", cfg.RootFSMaintenance)
	}
}

func TestRootFSImporterDefaultsAreBounded(t *testing.T) {
	cfg := &ManagerConfig{}
	applyRootFSImporterDefaults(cfg)
	if cfg.RootFSImporter.Interval.Duration != time.Second ||
		cfg.RootFSImporter.LeaseTTL.Duration != 2*time.Minute ||
		cfg.RootFSImporter.LeaseRenewal.Duration != 30*time.Second ||
		cfg.RootFSImporter.MaxAttempts != 5 ||
		cfg.RootFSImporter.GarbageInterval.Duration != time.Minute ||
		cfg.RootFSImporter.TerminalRetention.Duration != 24*time.Hour ||
		cfg.RootFSImporter.GarbageLimit != 100 {
		t.Fatalf("rootfs importer defaults = %#v", cfg.RootFSImporter)
	}
}

func TestLoadManagerConfigLoadsRootFSImporterContract(t *testing.T) {
	path := writeManagerConfigFile(t, `
rootfs_importer:
  worker_id: manager.rootfs.import.test
  interval:
    duration: 2s
  lease_ttl:
    duration: 3m
  lease_renewal:
    duration: 45s
  max_attempts: 7
  garbage_interval:
    duration: 2m
  terminal_retention:
    duration: 48h
  garbage_limit: 75
  work_root: /var/lib/sandbox0/rootfs-import
  procd_path: /opt/sandbox0/bin/procd
  procd_protocol: sandbox0.procd.v1
  procd_digest: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  plain_http_hosts:
    - registry.internal:5000
`)
	cfg, err := loadManagerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	importer := cfg.RootFSImporter
	if importer.WorkerID != "manager.rootfs.import.test" || importer.Interval.Duration != 2*time.Second ||
		importer.LeaseTTL.Duration != 3*time.Minute || importer.LeaseRenewal.Duration != 45*time.Second ||
		importer.MaxAttempts != 7 || importer.GarbageInterval.Duration != 2*time.Minute ||
		importer.TerminalRetention.Duration != 48*time.Hour || importer.GarbageLimit != 75 ||
		importer.WorkRoot != "/var/lib/sandbox0/rootfs-import" || importer.ProcdPath != "/opt/sandbox0/bin/procd" ||
		importer.ProcdProtocol != "sandbox0.procd.v1" || len(importer.PlainHTTPHosts) != 1 ||
		importer.PlainHTTPHosts[0] != "registry.internal:5000" {
		t.Fatalf("rootfs importer config = %#v", importer)
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
