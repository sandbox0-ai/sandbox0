package runtimecontroller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

const maxStaticAssignmentBytes = 64 << 10

// StaticAssignmentFromEnv selects the one-shot Nomad activation mode. An
// absent mode preserves the Kubernetes CTLD stream path.
func StaticAssignmentFromEnv() (*runtimecontrol.Assignment, bool, error) {
	mode, modeSet := os.LookupEnv(runtimecontrol.EnvControlMode)
	raw, assignmentSet := os.LookupEnv(runtimecontrol.EnvStaticAssignment)
	if !modeSet {
		if assignmentSet && raw != "" {
			return nil, false, fmt.Errorf("%s requires %s=%s",
				runtimecontrol.EnvStaticAssignment, runtimecontrol.EnvControlMode, runtimecontrol.ControlModeStatic)
		}
		return nil, false, nil
	}
	if mode == "" {
		if assignmentSet && raw != "" {
			return nil, false, fmt.Errorf("%s requires %s=%s",
				runtimecontrol.EnvStaticAssignment, runtimecontrol.EnvControlMode, runtimecontrol.ControlModeStatic)
		}
		return nil, false, nil
	}
	if mode != runtimecontrol.ControlModeStatic {
		return nil, false, fmt.Errorf("unsupported runtime control mode %q", mode)
	}
	if !assignmentSet || raw == "" {
		return nil, false, fmt.Errorf("%s is required in static runtime control mode", runtimecontrol.EnvStaticAssignment)
	}
	assignment, err := decodeStaticAssignment(raw)
	if err != nil {
		return nil, false, err
	}
	return assignment, true, nil
}

func decodeStaticAssignment(raw string) (*runtimecontrol.Assignment, error) {
	if len(raw) > maxStaticAssignmentBytes {
		return nil, fmt.Errorf("static runtime assignment exceeds 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.DisallowUnknownFields()
	var assignment runtimecontrol.Assignment
	if err := decoder.Decode(&assignment); err != nil {
		return nil, fmt.Errorf("decode static runtime assignment: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode static runtime assignment: %w", err)
	}
	if err := assignment.Validate(); err != nil {
		return nil, fmt.Errorf("validate static runtime assignment: %w", err)
	}
	if assignment.EnvVars[runtimecontrol.EnvSandboxID] != assignment.SandboxID {
		return nil, fmt.Errorf("static runtime assignment sandbox environment does not match sandbox_id")
	}
	return &assignment, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("trailing JSON value")
}

// ActivateStatic applies an immutable assignment before the Nomad procd HTTP
// server starts accepting requests.
func ActivateStatic(ctx context.Context, controller *Controller, assignment runtimecontrol.Assignment) error {
	if controller == nil {
		return errors.New("runtime controller is required")
	}
	revision, err := assignment.Revision()
	if err != nil {
		return fmt.Errorf("derive static runtime assignment revision: %w", err)
	}
	snapshot := runtimecontrol.Snapshot{
		State: runtimecontrol.DesiredActive, Revision: revision, Assignment: &assignment,
	}
	if err := controller.HandleSnapshot(ctx, snapshot, func(runtimecontrol.Observation) error { return nil }); err != nil {
		return fmt.Errorf("activate static runtime assignment: %w", err)
	}
	if ready, reason := controller.CanServe(); !ready {
		return fmt.Errorf("activate static runtime assignment: %s", reason)
	}
	state := controller.State()
	if state.Revision != revision || state.RuntimeGeneration != assignment.RuntimeGeneration {
		return errors.New("static runtime activation acknowledged another assignment")
	}
	return nil
}
