package v1alpha1

import "testing"

func TestNetworkPolicyRequiresSynchronousApply(t *testing.T) {
	tests := []struct {
		name string
		spec *NetworkPolicySpec
		want bool
	}{
		{name: "nil policy"},
		{name: "implicit allow all", spec: &NetworkPolicySpec{}},
		{name: "explicit allow all", spec: &NetworkPolicySpec{Mode: NetworkModeAllowAll}},
		{
			name: "allow all with egress policy",
			spec: &NetworkPolicySpec{
				Mode:   NetworkModeAllowAll,
				Egress: &NetworkEgressPolicy{},
			},
			want: true,
		},
		{
			name: "block all",
			spec: &NetworkPolicySpec{
				Mode: NetworkModeBlockAll,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NetworkPolicyRequiresSynchronousApply(tt.spec); got != tt.want {
				t.Fatalf("NetworkPolicyRequiresSynchronousApply() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestParseNetworkPolicyFromAnnotationStrict(t *testing.T) {
	raw := `{"version":"v1","sandboxId":"sandbox-1","teamId":"team-1","mode":"block-all"}`
	spec, err := ParseNetworkPolicyFromAnnotationStrict(raw)
	if err != nil || spec == nil || spec.SandboxID != "sandbox-1" {
		t.Fatalf("strict policy = %+v, %v", spec, err)
	}
	for _, invalid := range []string{
		`{"version":"v1","sandboxId":"sandbox-1","teamId":"team-1","mode":"block-all","unknown":true}`,
		raw + ` {}`,
	} {
		if _, err := ParseNetworkPolicyFromAnnotationStrict(invalid); err == nil {
			t.Fatalf("invalid strict policy %q was accepted", invalid)
		}
	}
}
