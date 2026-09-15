package runtimeslot

import "testing"

func TestNetworkPrepareRequiresCompleteMutationAuthority(t *testing.T) {
	for _, test := range []struct {
		name     string
		revision int64
		expected string
		valid    bool
	}{
		{"initial claim", 0, "", true},
		{"mutation", 7, NetworkPolicyDigest("previous"), true},
		{"missing digest", 7, "", false},
		{"missing revision", 0, NetworkPolicyDigest("previous"), false},
		{"negative revision", -1, NetworkPolicyDigest("previous"), false},
		{"invalid digest", 7, "previous", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := testNodeChannelNetworkPrepare()
			request.PolicyRevision, request.ExpectedPolicyDigest = test.revision, test.expected
			if err := request.Validate(); (err == nil) != test.valid {
				t.Fatalf("Validate() = %v, valid=%v", err, test.valid)
			}
		})
	}
}
