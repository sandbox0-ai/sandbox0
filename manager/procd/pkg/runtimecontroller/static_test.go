package runtimecontroller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"go.uber.org/zap"
)

func TestStaticAssignmentFromEnv(t *testing.T) {
	assignment := testStaticAssignment()
	payload, err := json.Marshal(assignment)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(runtimecontrol.EnvControlMode, runtimecontrol.ControlModeStatic)
	t.Setenv(runtimecontrol.EnvStaticAssignment, string(payload))

	got, static, err := StaticAssignmentFromEnv()
	if err != nil {
		t.Fatalf("StaticAssignmentFromEnv() error = %v", err)
	}
	if !static || got == nil || got.SandboxID != assignment.SandboxID || got.RuntimeGeneration != assignment.RuntimeGeneration {
		t.Fatalf("static assignment = %#v, static = %t", got, static)
	}
}

func TestStaticAssignmentFromEnvPreservesStreamModeWhenAbsent(t *testing.T) {
	t.Setenv(runtimecontrol.EnvControlMode, "")
	t.Setenv(runtimecontrol.EnvStaticAssignment, "")
	assignment, static, err := StaticAssignmentFromEnv()
	if err != nil || static || assignment != nil {
		t.Fatalf("assignment = %#v, static = %t, error = %v", assignment, static, err)
	}
}

func TestStaticAssignmentFromEnvRejectsMalformedInput(t *testing.T) {
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
			if _, _, err := StaticAssignmentFromEnv(); err == nil {
				t.Fatal("invalid static runtime control input was accepted")
			}
		})
	}
}

func TestActivateStaticMakesRuntimeServeable(t *testing.T) {
	controller := New(nil, nil, nil, nil, 49983, zap.NewNop())
	assignment := testStaticAssignment()
	if err := ActivateStatic(context.Background(), controller, assignment); err != nil {
		t.Fatalf("ActivateStatic() error = %v", err)
	}
	if ready, reason := controller.CanServe(); !ready {
		t.Fatalf("static runtime is not serveable: %s", reason)
	}
	revision, err := assignment.Revision()
	if err != nil {
		t.Fatal(err)
	}
	state := controller.State()
	if state.Revision != revision || state.RuntimeGeneration != assignment.RuntimeGeneration ||
		state.Observed != runtimecontrol.ObservedReady {
		t.Fatalf("static runtime state = %#v", state)
	}
}

func testStaticAssignment() runtimecontrol.Assignment {
	return runtimecontrol.Assignment{
		SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 3,
		EnvVars: map[string]string{runtimecontrol.EnvSandboxID: "sandbox-1", "MODE": "test"},
	}
}
