package runtimecontrol

import "testing"

func TestAssignmentRevisionIsStable(t *testing.T) {
	first := Assignment{
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 2,
		SecurityClass:     "standard",
		EnvVars: map[string]string{
			"B": "2",
			"A": "1",
		},
	}
	second := Assignment{
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 2,
		SecurityClass:     "standard",
		EnvVars: map[string]string{
			"A": "1",
			"B": "2",
		},
	}

	firstRevision, err := first.Revision()
	if err != nil {
		t.Fatal(err)
	}
	secondRevision, err := second.Revision()
	if err != nil {
		t.Fatal(err)
	}
	if firstRevision != secondRevision {
		t.Fatalf("assignment revisions differ: %q != %q", firstRevision, secondRevision)
	}
}

func TestAssignmentRevisionRejectsInvalidGeneration(t *testing.T) {
	if _, err := (Assignment{SandboxID: "sandbox-a"}).Revision(); err == nil {
		t.Fatal("Revision() error = nil, want invalid generation")
	}
}

func TestAssignmentRejectsInvalidSecurityAndEphemeralMounts(t *testing.T) {
	base := Assignment{SandboxID: "sandbox-a", RuntimeGeneration: 1, SecurityClass: "standard"}
	tests := []Assignment{
		{SandboxID: "sandbox-a", RuntimeGeneration: 1, SecurityClass: "host"},
		{SandboxID: "sandbox-a", RuntimeGeneration: 1, SecurityClass: "standard", EphemeralMounts: []EphemeralMount{{MountPath: "/proc/cache", SizeBytes: 1 << 20}}},
		{SandboxID: "sandbox-a", RuntimeGeneration: 1, SecurityClass: "standard", EphemeralMounts: []EphemeralMount{{MountPath: "/workspace", SizeBytes: 1 << 20}, {MountPath: "/workspace/cache", SizeBytes: 1 << 20}}},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid assignment error = %v", err)
	}
	for _, assignment := range tests {
		if err := assignment.Validate(); err == nil {
			t.Fatalf("invalid assignment accepted: %#v", assignment)
		}
	}
}
