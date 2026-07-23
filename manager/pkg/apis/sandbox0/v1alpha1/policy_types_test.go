package v1alpha1

import "testing"

func TestNetworkPolicyRequiresApply(t *testing.T) {
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
			if got := NetworkPolicyRequiresApply(tt.spec); got != tt.want {
				t.Fatalf("NetworkPolicyRequiresApply() = %t, want %t", got, tt.want)
			}
		})
	}
}
