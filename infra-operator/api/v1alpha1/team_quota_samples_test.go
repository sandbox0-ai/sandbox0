package v1alpha1

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"sigs.k8s.io/yaml"
)

func TestRegionOwnerSamplesContainCompleteTeamQuotaDefaults(t *testing.T) {
	ownerSamples := []string{
		"../../chart/samples/single-cluster/fullmode.yaml",
		"../../chart/samples/single-cluster/fullmode-gke-gvisor.yaml",
		"../../chart/samples/multi-cluster/control-plane.yaml",
	}
	var (
		referenceSample   string
		referencePolicies map[coreteamquota.Key]TeamQuotaPolicyConfig
	)
	for _, samplePath := range ownerSamples {
		samplePath := samplePath
		t.Run(filepath.Base(samplePath), func(t *testing.T) {
			infra := loadSandbox0InfraSample(t, samplePath)
			if infra.Spec.TeamQuota == nil {
				t.Fatal("region-owner sample omits spec.teamQuota")
			}
			if infra.Spec.TeamQuota.StateID != "" {
				t.Fatalf(
					"region-owner sample embeds reusable state ID %q; owner samples must let infra-operator generate status.teamQuota.stateId",
					infra.Spec.TeamQuota.StateID,
				)
			}
			defaults := infra.Spec.TeamQuota.Defaults
			if len(defaults) != len(coreteamquota.Keys()) {
				t.Fatalf("defaults = %d, want %d", len(defaults), len(coreteamquota.Keys()))
			}
			seen := make(map[coreteamquota.Key]struct{}, len(defaults))
			policies := make(map[coreteamquota.Key]TeamQuotaPolicyConfig, len(defaults))
			for _, policy := range defaults {
				key := coreteamquota.Key(policy.Key)
				if _, duplicate := seen[key]; duplicate {
					t.Fatalf("duplicate Team Quota key %q", key)
				}
				seen[key] = struct{}{}
				kind, known := coreteamquota.KindForKey(key)
				if !known {
					t.Fatalf("unknown Team Quota key %q", key)
				}
				if policy.Kind != string(kind) {
					t.Fatalf("policy %q kind = %q, want %q", key, policy.Kind, kind)
				}
				switch kind {
				case coreteamquota.KindCapacity, coreteamquota.KindConcurrency:
					if policy.Limit == nil || policy.Tokens != nil || policy.Interval != nil || policy.Burst != nil {
						t.Fatalf("%s policy %q has invalid field shape: %#v", kind, key, policy)
					}
				case coreteamquota.KindRate:
					if policy.Limit != nil || policy.Tokens == nil || policy.Interval == nil || policy.Burst == nil {
						t.Fatalf("rate policy %q has invalid field shape: %#v", key, policy)
					}
					if _, err := coreteamquota.RateIntervalMillis(policy.Interval.Duration); err != nil {
						t.Fatalf("rate policy %q interval is invalid: %v", key, err)
					}
				}
				policies[key] = policy
			}
			for _, key := range coreteamquota.Keys() {
				if _, ok := seen[key]; !ok {
					t.Fatalf("sample is missing Team Quota key %q", key)
				}
			}
			if referencePolicies == nil {
				referenceSample = samplePath
				referencePolicies = policies
				return
			}
			if !reflect.DeepEqual(policies, referencePolicies) {
				t.Fatalf(
					"Team Quota defaults differ from reference sample %s:\n got: %#v\nwant: %#v",
					referenceSample,
					policies,
					referencePolicies,
				)
			}
		})
	}
}

func TestMultiClusterDataPlaneSampleConfiguresOnlyDistributedTeamQuota(t *testing.T) {
	infra := loadSandbox0InfraSample(
		t,
		"../../chart/samples/multi-cluster/data-plane.yaml",
	)
	if infra.Spec.TeamQuota == nil {
		t.Fatal("data-plane sample omits consumer Team Quota settings")
	}
	if len(infra.Spec.TeamQuota.Defaults) != 0 {
		t.Fatalf("data-plane defaults = %#v, want region-owner policy omitted", infra.Spec.TeamQuota.Defaults)
	}
	const stateIDPlaceholder = "${SANDBOX0_TEAM_QUOTA_STATE_ID}"
	if infra.Spec.TeamQuota.StateID != stateIDPlaceholder {
		t.Fatalf(
			"data-plane state ID = %q, want non-reusable owner-status placeholder %q",
			infra.Spec.TeamQuota.StateID,
			stateIDPlaceholder,
		)
	}
	distributed := infra.Spec.TeamQuota.DistributedEnforcement
	if distributed.PolicyCacheTTL.Duration == 0 ||
		distributed.LeaseTTL.Duration == 0 ||
		distributed.RenewInterval.Duration == 0 {
		t.Fatalf("data-plane distributed settings = %#v, want explicit consumer timings", distributed)
	}
}

func loadSandbox0InfraSample(t *testing.T, path string) *Sandbox0Infra {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for _, document := range bytes.Split(raw, []byte("\n---")) {
		var metadata struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal(document, &metadata); err != nil {
			t.Fatalf("decode type metadata from %s: %v", path, err)
		}
		if metadata.Kind != "Sandbox0Infra" {
			continue
		}
		var infra Sandbox0Infra
		if err := yaml.UnmarshalStrict(document, &infra); err != nil {
			t.Fatalf("decode Sandbox0Infra from %s: %v", path, err)
		}
		return &infra
	}
	t.Fatalf("Sandbox0Infra document not found in %s", path)
	return nil
}
