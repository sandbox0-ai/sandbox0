package runtimecontroller

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

const maxStaticAssignmentBytes = 64 << 10

// AssignmentFromEnv loads the immutable assignment injected by the Nomad
// driver. Procd has no orchestrator fallback.
func AssignmentFromEnv() (*runtimecontrol.Assignment, error) {
	mode := strings.TrimSpace(os.Getenv(runtimecontrol.EnvControlMode))
	if mode != runtimecontrol.ControlModeStatic {
		if mode == "" {
			return nil, fmt.Errorf("%s=%s is required", runtimecontrol.EnvControlMode, runtimecontrol.ControlModeStatic)
		}
		return nil, fmt.Errorf("unsupported runtime control mode %q", mode)
	}
	raw := os.Getenv(runtimecontrol.EnvStaticAssignment)
	if raw == "" {
		return nil, fmt.Errorf("%s is required", runtimecontrol.EnvStaticAssignment)
	}
	return decodeStaticAssignment(raw)
}

func decodeStaticAssignment(raw string) (*runtimecontrol.Assignment, error) {
	if len(raw) > maxStaticAssignmentBytes {
		return nil, errors.New("static runtime assignment exceeds 64 KiB")
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
		return nil, errors.New("static runtime assignment sandbox environment does not match sandbox_id")
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
