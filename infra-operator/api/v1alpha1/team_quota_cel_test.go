package v1alpha1

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

func TestTeamQuotaRateIntervalCELRuleBoundsAdmissionCost(t *testing.T) {
	rule := teamQuotaXValidationRule(
		t,
		"rate policy interval must be between 1ms and 1h",
	)
	const want = "self.kind != 'rate' || !has(self.interval) || (duration(self.interval) >= duration('1ms') && duration(self.interval) <= duration('1h'))"
	if rule != want {
		t.Fatalf("rate interval CEL rule = %q, want %q", rule, want)
	}
}

func TestTeamQuotaRateIntervalBoundsCELInputCost(t *testing.T) {
	source, err := os.ReadFile("team_quota_types.go")
	if err != nil {
		t.Fatalf("read team_quota_types.go: %v", err)
	}
	const boundedInterval = "// +kubebuilder:validation:Type=string\n\t// +kubebuilder:validation:MaxLength=32\n\tInterval *metav1.Duration"
	if !strings.Contains(string(source), boundedInterval) {
		t.Fatal("rate interval must bound its string length for CRD CEL cost estimation")
	}
}

func TestTeamQuotaDefaultsCELRuleAllowsConsumerOnlyConfig(t *testing.T) {
	rule := teamQuotaXValidationRule(
		t,
		"when configured, defaults must contain every known team quota key exactly once",
	)
	if !strings.HasPrefix(rule, "!has(self.defaults) || ") {
		t.Fatalf("defaults CEL rule = %q, want consumer-only omission guard", rule)
	}
	for _, key := range coreteamquota.Keys() {
		quotedKey := "'" + string(key) + "'"
		if !strings.Contains(rule, quotedKey) {
			t.Fatalf("defaults CEL rule is missing key %q", key)
		}
	}
}

func TestTeamQuotaDefaultsCardinalityMarkersMatchRegistry(t *testing.T) {
	source, err := os.ReadFile("team_quota_types.go")
	if err != nil {
		t.Fatalf("read team_quota_types.go: %v", err)
	}
	pattern := regexp.MustCompile(
		`(?s)\+kubebuilder:validation:MinItems=([0-9]+).*` +
			`\+kubebuilder:validation:MaxItems=([0-9]+).*` +
			`Defaults \[\]TeamQuotaPolicyConfig`,
	)
	match := pattern.FindSubmatch(source)
	if len(match) != 3 {
		t.Fatal("Team Quota defaults cardinality markers not found")
	}
	minItems, err := strconv.Atoi(string(match[1]))
	if err != nil {
		t.Fatalf("decode defaults MinItems: %v", err)
	}
	maxItems, err := strconv.Atoi(string(match[2]))
	if err != nil {
		t.Fatalf("decode defaults MaxItems: %v", err)
	}
	want := len(coreteamquota.Keys())
	if minItems != want || maxItems != want {
		t.Fatalf(
			"defaults cardinality markers = %d..%d, want %d",
			minItems,
			maxItems,
			want,
		)
	}
}

func TestTeamQuotaDistributedCELRulesAllowDefaultEmptyObject(t *testing.T) {
	for _, message := range []string{
		"policyCacheTtl must be non-negative",
		"leaseTtl must be positive",
		"renewInterval must be positive",
		"renewInterval doubled must be less than leaseTtl",
	} {
		rule := teamQuotaXValidationRule(t, message)
		if !strings.HasPrefix(rule, "!has(") {
			t.Fatalf(
				"distributed enforcement CEL rule %q = %q, want optional-field guard",
				message,
				rule,
			)
		}
	}
}

func teamQuotaXValidationRule(t *testing.T, message string) string {
	t.Helper()
	source, err := os.ReadFile("team_quota_types.go")
	if err != nil {
		t.Fatalf("read team_quota_types.go: %v", err)
	}
	pattern := regexp.MustCompile(
		`// \+kubebuilder:validation:XValidation:rule="([^"]+)",message="` +
			regexp.QuoteMeta(message) +
			`"`,
	)
	match := pattern.FindSubmatch(source)
	if len(match) != 2 {
		t.Fatalf("XValidation marker with message %q not found", message)
	}
	return string(match[1])
}
