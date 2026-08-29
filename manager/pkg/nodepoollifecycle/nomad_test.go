package nodepoollifecycle

import "testing"

func TestNomadAllocationTerminalUsesClientExecutionTruth(t *testing.T) {
	for _, test := range []struct {
		client, desired string
		terminal        bool
	}{
		{client: "complete", desired: "run", terminal: true},
		{client: "failed", desired: "run", terminal: true},
		{client: "lost", desired: "stop", terminal: true},
		{client: "running", desired: "stop", terminal: false},
		{client: "pending", desired: "run", terminal: false},
	} {
		allocation := nomadAllocation{ClientStatus: test.client, DesiredStatus: test.desired}
		if got := allocation.terminal(); got != test.terminal {
			t.Fatalf("terminal(%s,%s) = %v, want %v", test.client, test.desired, got, test.terminal)
		}
	}
}
