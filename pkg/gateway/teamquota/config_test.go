package teamquota

import (
	"context"
	"testing"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPoliciesFromConfigPreservesKindSpecificFields(t *testing.T) {
	limit := int64(8)
	connectionLimit := int64(32)
	tokens := int64(10)
	burst := int64(20)
	interval := metav1.Duration{Duration: 1500 * time.Millisecond}
	policies, err := PoliciesFromConfig([]apiconfig.TeamQuotaPolicyConfig{
		{
			Key:   string(coreteamquota.KeySandboxRuntimeCount),
			Kind:  string(coreteamquota.KindCapacity),
			Limit: &limit,
		},
		{
			Key:   string(coreteamquota.KeyActiveConnectionCount),
			Kind:  string(coreteamquota.KindConcurrency),
			Limit: &connectionLimit,
		},
		{
			Key:      string(coreteamquota.KeyAPIRequests),
			Kind:     string(coreteamquota.KindRate),
			Tokens:   &tokens,
			Interval: &interval,
			Burst:    &burst,
		},
	})
	if err != nil {
		t.Fatalf("PoliciesFromConfig() error = %v", err)
	}
	if len(policies) != 3 ||
		policies[0].Limit != 8 ||
		policies[1].Kind != coreteamquota.KindConcurrency ||
		policies[1].Limit != 32 ||
		policies[2].IntervalMillis != 1500 {
		t.Fatalf("policies = %#v", policies)
	}
}

func TestPoliciesFromConfigValidatesRateInterval(t *testing.T) {
	tokens := int64(1)
	burst := int64(1)
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
			_, err := PoliciesFromConfig([]apiconfig.TeamQuotaPolicyConfig{{
				Key:      string(coreteamquota.KeyAPIRequests),
				Kind:     string(coreteamquota.KindRate),
				Tokens:   &tokens,
				Interval: &interval,
				Burst:    &burst,
			}})
			if tt.wantErr && err == nil {
				t.Fatalf("PoliciesFromConfig(%s) error = nil, want error", tt.interval)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("PoliciesFromConfig(%s) error = %v", tt.interval, err)
			}
		})
	}
}

func TestReplaceConfiguredDefaultsPassesOwnerFence(t *testing.T) {
	ownerEpoch := time.Date(2026, time.July, 19, 1, 2, 3, 456_789_987, time.UTC)
	manager := &capturingPolicyManager{}
	err := ReplaceConfiguredDefaults(
		context.Background(),
		manager,
		apiconfig.TeamQuotaConfig{
			DefaultsOwnerEpoch: ownerEpoch.Format(time.RFC3339Nano),
			DefaultsGeneration: 9,
		},
	)
	if err != nil {
		t.Fatalf("ReplaceConfiguredDefaults() error = %v", err)
	}
	if manager.calls != 1 ||
		!manager.version.OwnerEpoch.Equal(ownerEpoch) ||
		manager.version.Generation != 9 {
		t.Fatalf(
			"captured defaults fence = calls %d version %+v",
			manager.calls,
			manager.version,
		)
	}
}

func TestReplaceConfiguredDefaultsRequiresValidOwnerVersion(t *testing.T) {
	manager := &capturingPolicyManager{}
	err := ReplaceConfiguredDefaults(
		context.Background(),
		manager,
		apiconfig.TeamQuotaConfig{
			DefaultsOwnerEpoch: "not-a-time",
			DefaultsGeneration: 1,
		},
	)
	if err == nil || manager.calls != 0 {
		t.Fatalf("invalid owner epoch result = (%v, %d calls)", err, manager.calls)
	}
}

type capturingPolicyManager struct {
	coreteamquota.PolicyManager

	version coreteamquota.DefaultPolicyVersion
	calls   int
}

func (s *capturingPolicyManager) ReplaceDefaultPoliciesVersioned(
	_ context.Context,
	_ []coreteamquota.Policy,
	version coreteamquota.DefaultPolicyVersion,
) error {
	s.calls++
	s.version = version
	return nil
}
