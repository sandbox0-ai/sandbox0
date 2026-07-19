package template

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
)

func TestValidateTemplateSpecScalarAndTagBoundaries(t *testing.T) {
	spec := &v1alpha1.SandboxTemplateSpec{
		Description: strings.Repeat("d", int(MaxDescriptionBytes)),
		DisplayName: strings.Repeat("n", int(MaxDisplayNameBytes)),
		Tags:        make([]string, MaxTagCount),
	}
	for i := range spec.Tags {
		spec.Tags[i] = strings.Repeat("t", int(MaxTagBytes))
	}
	if err := ValidateTemplateSpecSize(spec); err != nil {
		t.Fatalf("ValidateTemplateSpecSize(boundary) error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*v1alpha1.SandboxTemplateSpec)
	}{
		{
			name: "description one byte over",
			mutate: func(value *v1alpha1.SandboxTemplateSpec) {
				value.Description += "d"
			},
		},
		{
			name: "display name one byte over",
			mutate: func(value *v1alpha1.SandboxTemplateSpec) {
				value.DisplayName += "n"
			},
		},
		{
			name: "tag count one item over",
			mutate: func(value *v1alpha1.SandboxTemplateSpec) {
				value.Tags = append(value.Tags, "tag")
			},
		},
		{
			name: "tag one byte over",
			mutate: func(value *v1alpha1.SandboxTemplateSpec) {
				value.Tags = append([]string(nil), value.Tags...)
				value.Tags[0] += "t"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copySpec := *spec
			tt.mutate(&copySpec)
			if err := ValidateTemplateSpecSize(&copySpec); !resourceguard.IsTooLarge(err) {
				t.Fatalf("ValidateTemplateSpecSize() error = %v, want TooLargeError", err)
			}
		})
	}
}

func TestValidateTemplateSpecMapBoundaries(t *testing.T) {
	spec := &v1alpha1.SandboxTemplateSpec{
		EnvVars: make(map[string]string, MaxMapItems),
	}
	for i := 0; i < MaxMapItems; i++ {
		spec.EnvVars[fmt.Sprintf("VAR_%03d", i)] = "value"
	}
	if err := ValidateTemplateSpecSize(spec); err != nil {
		t.Fatalf("ValidateTemplateSpecSize(map count boundary) error = %v", err)
	}
	spec.EnvVars["OVER"] = "value"
	if err := ValidateTemplateSpecSize(spec); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateTemplateSpecSize(map count over) error = %v, want TooLargeError", err)
	}

	spec.EnvVars = map[string]string{
		"VALUE": strings.Repeat("v", int(MaxMapValueBytes)),
	}
	if err := ValidateTemplateSpecSize(spec); err != nil {
		t.Fatalf("ValidateTemplateSpecSize(map value boundary) error = %v", err)
	}
	spec.EnvVars["VALUE"] += "v"
	if err := ValidateTemplateSpecSize(spec); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateTemplateSpecSize(map value over) error = %v, want TooLargeError", err)
	}
}

func TestValidateNetworkPolicyCollectionBoundaries(t *testing.T) {
	policy := &v1alpha1.SandboxNetworkPolicy{
		Egress: &v1alpha1.NetworkEgressPolicy{
			TrafficRules: make([]v1alpha1.TrafficRule, MaxNetworkCollectionItems),
		},
	}
	if err := ValidateNetworkPolicySize(policy); err != nil {
		t.Fatalf("ValidateNetworkPolicySize(boundary) error = %v", err)
	}
	policy.Egress.TrafficRules = append(
		policy.Egress.TrafficRules,
		v1alpha1.TrafficRule{},
	)
	if err := ValidateNetworkPolicySize(policy); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateNetworkPolicySize(one item over) error = %v, want TooLargeError", err)
	}
}

func TestValidateCredentialBindingsCanonicalLimits(t *testing.T) {
	binding := v1alpha1.CredentialBinding{}
	base, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("marshal empty credential binding: %v", err)
	}
	binding.Ref = strings.Repeat("r", int(egressauth.MaxCredentialBindingBytes)-len(base))
	if err := ValidateCredentialBindingsSize(
		[]v1alpha1.CredentialBinding{binding},
	); err != nil {
		t.Fatalf("ValidateCredentialBindingsSize(per-binding boundary) error = %v", err)
	}
	binding.Ref += "r"
	if err := ValidateCredentialBindingsSize(
		[]v1alpha1.CredentialBinding{binding},
	); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCredentialBindingsSize(per-binding over) error = %v, want TooLargeError", err)
	}

	bindings := make([]v1alpha1.CredentialBinding, egressauth.MaxCredentialBindingCount)
	if err := ValidateCredentialBindingsSize(bindings); err != nil {
		t.Fatalf("ValidateCredentialBindingsSize(count boundary) error = %v", err)
	}
	bindings = append(bindings, v1alpha1.CredentialBinding{})
	if err := ValidateCredentialBindingsSize(bindings); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCredentialBindingsSize(count over) error = %v, want TooLargeError", err)
	}

	bindings = make([]v1alpha1.CredentialBinding, 9)
	for i := range bindings {
		bindings[i].Ref = strings.Repeat("r", 30<<10)
	}
	if err := ValidateCredentialBindingsSize(bindings); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateCredentialBindingsSize(aggregate over) error = %v, want TooLargeError", err)
	}
}

func TestValidateTemplateSpecCanonicalBoundaryAndOneByteOver(t *testing.T) {
	spec := &v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{Image: "x"},
	}
	payload, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal initial template spec: %v", err)
	}
	imageBytes := 1 + int(MaxCanonicalSpecBytes) - len(payload)
	if imageBytes <= 0 {
		t.Fatalf("computed image length = %d, want positive", imageBytes)
	}
	spec.MainContainer.Image = strings.Repeat("i", imageBytes)
	if err := ValidateTemplateSpecSize(spec); err != nil {
		t.Fatalf("ValidateTemplateSpecSize(canonical boundary) error = %v", err)
	}
	payload, err = json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal boundary template spec: %v", err)
	}
	if int64(len(payload)) != MaxCanonicalSpecBytes {
		t.Fatalf("canonical template spec length = %d, want %d", len(payload), MaxCanonicalSpecBytes)
	}
	spec.MainContainer.Image += "i"
	if err := ValidateTemplateSpecSize(spec); !resourceguard.IsTooLarge(err) {
		t.Fatalf("ValidateTemplateSpecSize(one byte over) error = %v, want TooLargeError", err)
	}
}
