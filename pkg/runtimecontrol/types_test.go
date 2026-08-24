package runtimecontrol

import "testing"

func TestAssignmentRevisionIsStable(t *testing.T) {
	first := Assignment{
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 2,
		EnvVars: map[string]string{
			"B": "2",
			"A": "1",
		},
	}
	second := Assignment{
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 2,
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
