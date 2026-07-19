package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
)

func TestValidateClaimRequestMountBoundaryAndOneItemOver(t *testing.T) {
	req := &ClaimRequest{
		Mounts: make([]ClaimMount, templatepkg.MaxNetworkCollectionItems),
	}
	if err := ValidateClaimRequestSize(req); err != nil {
		t.Fatalf("ValidateClaimRequestSize(boundary) error = %v", err)
	}
	req.Mounts = append(req.Mounts, ClaimMount{})
	if err := ValidateClaimRequestSize(req); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateClaimRequestSize(one item over) error = %v, want TooLargeError", err)
	}
}

func TestValidateSandboxConfigCollectionBoundaries(t *testing.T) {
	config := &SandboxConfig{
		EnvVars:  make(map[string]string, templatepkg.MaxMapItems),
		Services: make([]SandboxAppService, templatepkg.MaxNetworkCollectionItems),
	}
	for i := 0; i < templatepkg.MaxMapItems; i++ {
		config.EnvVars[fmt.Sprintf("VAR_%03d", i)] = "value"
	}
	if err := ValidateSandboxConfigSize(config); err != nil {
		t.Fatalf("ValidateSandboxConfigSize(boundary) error = %v", err)
	}

	t.Run("map count one item over", func(t *testing.T) {
		over := *config
		over.EnvVars = make(map[string]string, len(config.EnvVars)+1)
		for name, value := range config.EnvVars {
			over.EnvVars[name] = value
		}
		over.EnvVars["OVER"] = "value"
		if err := ValidateSandboxConfigSize(&over); !resourceguard.IsTooLarge(err) {
			t.Fatalf("ValidateSandboxConfigSize() error = %v, want TooLargeError", err)
		}
	})

	t.Run("slice count one item over", func(t *testing.T) {
		over := *config
		over.Services = append(
			append([]SandboxAppService(nil), config.Services...),
			SandboxAppService{},
		)
		if err := ValidateSandboxConfigSize(&over); !resourceguard.IsTooLarge(err) {
			t.Fatalf("ValidateSandboxConfigSize() error = %v, want TooLargeError", err)
		}
	})

	t.Run("map value one byte over", func(t *testing.T) {
		over := &SandboxConfig{
			EnvVars: map[string]string{
				"VALUE": strings.Repeat("v", int(templatepkg.MaxMapValueBytes)+1),
			},
		}
		if err := ValidateSandboxConfigSize(over); !resourceguard.IsTooLarge(err) {
			t.Fatalf("ValidateSandboxConfigSize() error = %v, want TooLargeError", err)
		}
	})
}

func TestValidateSandboxConfigCanonicalBoundaryAndOneByteOver(t *testing.T) {
	config := &SandboxConfig{
		Webhook: &WebhookConfig{
			URL:    "https://example.test/hook",
			Secret: "x",
		},
	}
	payload, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal initial sandbox config: %v", err)
	}
	secretBytes := 1 + int(templatepkg.MaxCanonicalSpecBytes) - len(payload)
	if secretBytes <= 0 {
		t.Fatalf("computed webhook secret length = %d, want positive", secretBytes)
	}
	config.Webhook.Secret = strings.Repeat("s", secretBytes)
	if err := ValidateSandboxConfigSize(config); err != nil {
		t.Fatalf("ValidateSandboxConfigSize(canonical boundary) error = %v", err)
	}
	payload, err = json.Marshal(config)
	if err != nil {
		t.Fatalf("marshal boundary sandbox config: %v", err)
	}
	if int64(len(payload)) != templatepkg.MaxCanonicalSpecBytes {
		t.Fatalf("canonical sandbox config length = %d, want %d", len(payload), templatepkg.MaxCanonicalSpecBytes)
	}
	config.Webhook.Secret += "s"
	if err := ValidateSandboxConfigSize(config); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateSandboxConfigSize(one byte over) error = %v, want TooLargeError", err)
	}
}

func TestValidateCompiledNetworkPolicyCollectionBoundary(t *testing.T) {
	policy := &v1alpha1.NetworkPolicySpec{
		Egress: &v1alpha1.NetworkEgressPolicy{
			TrafficRules: make(
				[]v1alpha1.TrafficRule,
				templatepkg.MaxNetworkCollectionItems,
			),
		},
	}
	if err := ValidateCompiledNetworkPolicySize(policy); err != nil {
		t.Fatalf("ValidateCompiledNetworkPolicySize(boundary) error = %v", err)
	}
	policy.Egress.TrafficRules = append(
		policy.Egress.TrafficRules,
		v1alpha1.TrafficRule{},
	)
	if err := ValidateCompiledNetworkPolicySize(policy); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCompiledNetworkPolicySize(one item over) error = %v, want TooLargeError", err)
	}
}

func TestMarshalSandboxRecordJSONRejectsOversizedObjects(t *testing.T) {
	t.Run("claim config", func(t *testing.T) {
		record := &SandboxRecord{
			Config: SandboxConfig{
				EnvVars: map[string]string{
					"SECRET": strings.Repeat("s", int(templatepkg.MaxMapValueBytes)+1),
				},
			},
		}
		if _, _, _, err := marshalSandboxRecordJSON(record); !resourceguard.IsTooLarge(err) {
			t.Fatalf("marshalSandboxRecordJSON() error = %v, want TooLargeError", err)
		}
	})

	t.Run("template spec", func(t *testing.T) {
		record := &SandboxRecord{}
		record.TemplateSpec.Description = strings.Repeat(
			"d",
			int(templatepkg.MaxDescriptionBytes)+1,
		)
		if _, _, _, err := marshalSandboxRecordJSON(record); !resourceguard.IsTooLarge(err) {
			t.Fatalf("marshalSandboxRecordJSON() error = %v, want TooLargeError", err)
		}
	})
}
