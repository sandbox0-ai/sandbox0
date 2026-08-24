package runtimecontroller

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

func TestAssignmentFromEnv(t *testing.T) {
	assignment := testStaticAssignment()
	payload, err := json.Marshal(assignment)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(runtimecontrol.EnvControlMode, runtimecontrol.ControlModeStatic)
	t.Setenv(runtimecontrol.EnvStaticAssignment, string(payload))

	got, err := AssignmentFromEnv()
	if err != nil {
		t.Fatalf("AssignmentFromEnv() error = %v", err)
	}
	if got == nil || got.SandboxID != assignment.SandboxID || got.RuntimeGeneration != assignment.RuntimeGeneration {
		t.Fatalf("assignment = %#v", got)
	}
}

func TestAssignmentFromEnvRequiresNomadAssignment(t *testing.T) {
	t.Setenv(runtimecontrol.EnvControlMode, "")
	t.Setenv(runtimecontrol.EnvStaticAssignment, "")
	if _, err := AssignmentFromEnv(); err == nil {
		t.Fatal("AssignmentFromEnv() accepted missing Nomad assignment")
	}
}

func TestAssignmentFromEnvRejectsMalformedInput(t *testing.T) {
	valid, err := json.Marshal(testStaticAssignment())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		mode string
		raw  string
	}{
		{name: "assignment without mode", raw: string(valid)},
		{name: "unknown mode", mode: "polling", raw: string(valid)},
		{name: "missing assignment", mode: runtimecontrol.ControlModeStatic},
		{name: "unknown field", mode: runtimecontrol.ControlModeStatic, raw: strings.TrimSuffix(string(valid), "}") + `,"unknown":true}`},
		{name: "trailing value", mode: runtimecontrol.ControlModeStatic, raw: string(valid) + `{}`},
		{name: "oversize", mode: runtimecontrol.ControlModeStatic, raw: strings.Repeat("x", maxStaticAssignmentBytes+1)},
		{name: "sandbox mismatch", mode: runtimecontrol.ControlModeStatic, raw: `{"sandbox_id":"sandbox-1","runtime_generation":1,"env_vars":{"SANDBOX0_SANDBOX_ID":"sandbox-2"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(runtimecontrol.EnvControlMode, test.mode)
			t.Setenv(runtimecontrol.EnvStaticAssignment, test.raw)
			if _, err := AssignmentFromEnv(); err == nil {
				t.Fatal("invalid static runtime control input was accepted")
			}
		})
	}
}

func testStaticAssignment() runtimecontrol.Assignment {
	return runtimecontrol.Assignment{
		SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 3, SecurityClass: "standard",
		EnvVars: map[string]string{runtimecontrol.EnvSandboxID: "sandbox-1", "MODE": "test"},
	}
}
