package runtimeconfig

import (
	"testing"
	"time"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testTeamQuotaStateID = "11111111-1111-4111-8111-111111111111"

func TestResolveTeamQuotaSpecUsesOwnerStatusWithoutMutatingSpec(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			TeamQuota: &infrav1alpha1.TeamQuotaConfig{},
		},
		Status: infrav1alpha1.Sandbox0InfraStatus{
			TeamQuota: &infrav1alpha1.TeamQuotaStatus{StateID: testTeamQuotaStateID},
		},
	}

	resolved := ResolveTeamQuotaSpec(infra)
	if resolved == nil || resolved.StateID != testTeamQuotaStateID {
		t.Fatalf("resolved Team Quota = %#v, want owner status state ID", resolved)
	}
	if infra.Spec.TeamQuota.StateID != "" {
		t.Fatalf("resolver mutated owner spec state ID to %q", infra.Spec.TeamQuota.StateID)
	}
}

func TestResolveTeamQuotaSpecKeepsExplicitConsumerValue(t *testing.T) {
	const consumerStateID = "4f54208d-4f01-42da-bdbc-88cc5793857b"
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			TeamQuota: &infrav1alpha1.TeamQuotaConfig{StateID: consumerStateID},
		},
		Status: infrav1alpha1.Sandbox0InfraStatus{
			TeamQuota: &infrav1alpha1.TeamQuotaStatus{StateID: testTeamQuotaStateID},
		},
	}

	resolved := ResolveTeamQuotaSpec(infra)
	if resolved == nil || resolved.StateID != consumerStateID {
		t.Fatalf("resolved consumer state ID = %#v, want explicit spec value %q", resolved, consumerStateID)
	}
}

func TestToTeamQuotaCopiesDefaultsForRegionOwner(t *testing.T) {
	limit := int64(100)
	tokens := int64(25)
	interval := metav1.Duration{Duration: time.Second}
	burst := int64(50)
	spec := &infrav1alpha1.TeamQuotaConfig{
		StateID: testTeamQuotaStateID,
		Defaults: []infrav1alpha1.TeamQuotaPolicyConfig{
			{
				Key:   "sandbox_runtime_count",
				Kind:  "capacity",
				Limit: &limit,
			},
			{
				Key:      "api_requests",
				Kind:     "rate",
				Tokens:   &tokens,
				Interval: &interval,
				Burst:    &burst,
			},
		},
		DistributedEnforcement: infrav1alpha1.TeamQuotaDistributedEnforcementConfig{
			PolicyCacheTTL: metav1.Duration{Duration: 8 * time.Second},
			LeaseTTL:       metav1.Duration{Duration: 30 * time.Second},
			RenewInterval:  metav1.Duration{Duration: 10 * time.Second},
		},
	}

	cfg := ToTeamQuota(spec)
	if !cfg.PolicyOwner {
		t.Fatal("expected region entrypoint to own policy reconciliation")
	}
	if len(cfg.Defaults) != 2 {
		t.Fatalf("defaults len = %d, want 2", len(cfg.Defaults))
	}
	if cfg.Defaults[0].Limit == nil || *cfg.Defaults[0].Limit != 100 {
		t.Fatalf("capacity default = %#v, want limit 100", cfg.Defaults[0])
	}
	if cfg.Defaults[1].Tokens == nil || *cfg.Defaults[1].Tokens != 25 ||
		cfg.Defaults[1].Interval == nil || cfg.Defaults[1].Interval.Duration != time.Second ||
		cfg.Defaults[1].Burst == nil || *cfg.Defaults[1].Burst != 50 {
		t.Fatalf("rate default = %#v, want 25 tokens/second with burst 50", cfg.Defaults[1])
	}
	if cfg.DistributedEnforcement.PolicyCacheTTL.Duration != 8*time.Second {
		t.Fatalf("policy cache ttl = %s, want 8s", cfg.DistributedEnforcement.PolicyCacheTTL.Duration)
	}
	if cfg.DistributedEnforcement.StateID != testTeamQuotaStateID {
		t.Fatalf("state ID = %q, want %q", cfg.DistributedEnforcement.StateID, testTeamQuotaStateID)
	}
	if cfg.DistributedEnforcement.LeaseTTL.Duration != 30*time.Second ||
		cfg.DistributedEnforcement.RenewInterval.Duration != 10*time.Second {
		t.Fatalf("lease timing = %#v, want 30s/10s", cfg.DistributedEnforcement)
	}

	limit = 200
	tokens = 40
	interval.Duration = 2 * time.Second
	burst = 80
	if *cfg.Defaults[0].Limit != 100 ||
		*cfg.Defaults[1].Tokens != 25 ||
		cfg.Defaults[1].Interval.Duration != time.Second ||
		*cfg.Defaults[1].Burst != 50 {
		t.Fatal("runtime defaults alias the CR fields")
	}
}

func TestToTeamQuotaDistributedEnforcementExcludesDefaults(t *testing.T) {
	limit := int64(10)
	spec := &infrav1alpha1.TeamQuotaConfig{
		StateID: testTeamQuotaStateID,
		Defaults: []infrav1alpha1.TeamQuotaPolicyConfig{
			{Key: "sandbox_identity_count", Kind: "capacity", Limit: &limit},
		},
	}

	cfg := ToTeamQuotaDistributedEnforcement(spec)
	if cfg.PolicyCacheTTL.Duration != 0 {
		t.Fatalf("policy cache ttl = %s, want explicit zero", cfg.PolicyCacheTTL.Duration)
	}
	if cfg.RedisURL != "" || cfg.RedisKeyPrefix != "" || cfg.RedisTimeout.Duration != 0 {
		t.Fatalf("operator-derived Redis fields = %#v, want empty", cfg)
	}
	if cfg.StateID != testTeamQuotaStateID {
		t.Fatalf("state ID = %q, want %q", cfg.StateID, testTeamQuotaStateID)
	}
	if cfg.LeaseTTL.Duration != 15*time.Second || cfg.RenewInterval.Duration != 5*time.Second {
		t.Fatalf("lease timing = %#v, want defaults 15s/5s", cfg)
	}
}

func TestToTeamQuotaDistributedEnforcementPreservesExplicitZeroTTL(t *testing.T) {
	spec := &infrav1alpha1.TeamQuotaConfig{
		StateID: testTeamQuotaStateID,
		DistributedEnforcement: infrav1alpha1.TeamQuotaDistributedEnforcementConfig{
			PolicyCacheTTL: metav1.Duration{Duration: 0},
		},
	}

	cfg := ToTeamQuotaDistributedEnforcement(spec)
	if cfg.PolicyCacheTTL.Duration != 0 {
		t.Fatalf("policy cache ttl = %s, want zero", cfg.PolicyCacheTTL.Duration)
	}
}

func TestToTeamQuotaDistributedEnforcementSupportsConsumerOnlySpec(t *testing.T) {
	spec := &infrav1alpha1.TeamQuotaConfig{
		StateID: testTeamQuotaStateID,
		DistributedEnforcement: infrav1alpha1.TeamQuotaDistributedEnforcementConfig{
			PolicyCacheTTL: metav1.Duration{Duration: 7 * time.Second},
			LeaseTTL:       metav1.Duration{Duration: 21 * time.Second},
			RenewInterval:  metav1.Duration{Duration: 7 * time.Second},
		},
	}

	cfg := ToTeamQuotaDistributedEnforcement(spec)
	if cfg.PolicyCacheTTL.Duration != 7*time.Second ||
		cfg.LeaseTTL.Duration != 21*time.Second ||
		cfg.RenewInterval.Duration != 7*time.Second {
		t.Fatalf("consumer-only distributed config = %#v, want 7s/21s/7s", cfg)
	}
}

func TestToTeamQuotaDefaultsPolicyCacheTTLWithoutSpec(t *testing.T) {
	cfg := ToTeamQuota(nil)
	if !cfg.PolicyOwner {
		t.Fatal("expected region entrypoint to own policy reconciliation")
	}
	if len(cfg.Defaults) != 0 {
		t.Fatalf("defaults = %#v, want empty", cfg.Defaults)
	}
	if cfg.DistributedEnforcement.PolicyCacheTTL.Duration != 5*time.Second {
		t.Fatalf("policy cache ttl = %s, want default 5s", cfg.DistributedEnforcement.PolicyCacheTTL.Duration)
	}
	if cfg.DistributedEnforcement.LeaseTTL.Duration != 15*time.Second ||
		cfg.DistributedEnforcement.RenewInterval.Duration != 5*time.Second {
		t.Fatalf("lease timing = %#v, want defaults 15s/5s", cfg.DistributedEnforcement)
	}
}

func TestSetTeamQuotaOwnerVersionUsesObjectIncarnationAndGeneration(t *testing.T) {
	createdAt := metav1.NewTime(time.Date(
		2026,
		time.July,
		19,
		1,
		2,
		3,
		456_789_987,
		time.FixedZone("test", 8*60*60),
	))
	owner := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: createdAt,
			Generation:        17,
		},
	}
	cfg := ToTeamQuota(nil)

	SetTeamQuotaOwnerVersion(&cfg, owner)

	wantEpoch := createdAt.UTC().Format(time.RFC3339Nano)
	if cfg.DefaultsOwnerEpoch != wantEpoch || cfg.DefaultsGeneration != 17 {
		t.Fatalf(
			"owner version = %q/%d, want %q/17",
			cfg.DefaultsOwnerEpoch,
			cfg.DefaultsGeneration,
			wantEpoch,
		)
	}
}
