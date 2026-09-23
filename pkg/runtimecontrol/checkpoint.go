package runtimecontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"math"
	"strings"
)

// CheckpointCaptureAssignment identifies the prepared source without selecting
// a destination. A paused image can outlive every source-node reservation and
// can have independently authorized fork descendants. This identity is not an
// execution grant; the regional transaction owns capture and restore authority.
type CheckpointCaptureAssignment struct {
	OperationID       string `json:"operation_id"`
	SandboxID         string `json:"sandbox_id"`
	TeamID            string `json:"team_id"`
	RuntimeGeneration int64  `json:"runtime_generation"`
	Revision          string `json:"revision"`
}

func NewCheckpointCaptureAssignment(operationID string, source Assignment) (CheckpointCaptureAssignment, error) {
	if identity, ok := source.EnvVars[EnvSandboxID]; ok && identity != source.SandboxID {
		return CheckpointCaptureAssignment{}, errors.New("checkpoint source environment identity changed")
	}
	revision, err := source.Revision()
	if err != nil {
		return CheckpointCaptureAssignment{}, err
	}
	result := CheckpointCaptureAssignment{OperationID: operationID, SandboxID: source.SandboxID,
		TeamID: source.TeamID, RuntimeGeneration: source.RuntimeGeneration, Revision: revision}
	return result, result.Validate()
}

func (a CheckpointCaptureAssignment) Validate() error {
	if !checkpointIdentity(a.OperationID) || !checkpointIdentity(a.SandboxID) ||
		!checkpointIdentity(a.TeamID) || a.RuntimeGeneration <= 0 {
		return errors.New("invalid checkpoint source identity")
	}
	revision, err := hex.DecodeString(a.Revision)
	if err != nil || len(revision) != sha256.Size || hex.EncodeToString(revision) != a.Revision {
		return errors.New("invalid checkpoint source revision")
	}
	return nil
}

func (a CheckpointCaptureAssignment) Digest() (string, error) {
	if err := a.Validate(); err != nil {
		return "", err
	}
	return checkpointAssignmentDigest(a)
}

type CheckpointRestoreKind string

const (
	CheckpointResume CheckpointRestoreKind = "resume"
	CheckpointFork   CheckpointRestoreKind = "fork"
)

// CheckpointRestoreAssignment binds one independently authorized use of a
// prepared image. Resume advances the original sandbox; fork starts a new
// sandbox in the same team. Failed execution attempts consume generations, so
// a later explicit retry also binds its regional predecessor. Neither may change the captured procd,
// workload configuration, security class or session-reset policy. The reserved
// sandbox-ID environment variable follows fork identity for future processes;
// already-running processes retain their captured environment.
type CheckpointRestoreAssignment struct {
	OperationID string                      `json:"operation_id"`
	Capture     CheckpointCaptureAssignment `json:"capture"`
	Kind        CheckpointRestoreKind       `json:"kind"`
	Target      Assignment                  `json:"target"`
	// FromGeneration is present after a failed restore consumed a generation.
	// Zero preserves the initial resume/fork encoding and digest. The regional
	// transaction must match this predecessor to the retained owner's generation.
	FromGeneration int64 `json:"from_generation,omitempty"`
}

func (a CheckpointRestoreAssignment) PreviousGeneration() int64 {
	if a.FromGeneration != 0 {
		return a.FromGeneration
	}
	if a.Kind == CheckpointResume {
		return a.Capture.RuntimeGeneration
	}
	return 0
}

func (a CheckpointRestoreAssignment) Validate() error {
	if err := a.Capture.Validate(); err != nil {
		return err
	}
	if !checkpointIdentity(a.OperationID) || !checkpointIdentity(a.Target.SandboxID) ||
		a.Target.TeamID != a.Capture.TeamID {
		return errors.New("checkpoint restore changed team or has an invalid operation or target")
	}
	if err := a.Target.Validate(); err != nil {
		return err
	}
	if identity, ok := a.Target.EnvVars[EnvSandboxID]; ok && identity != a.Target.SandboxID {
		return errors.New("checkpoint target environment identity changed")
	}
	switch a.Kind {
	case CheckpointResume:
		if a.Target.SandboxID != a.Capture.SandboxID || a.PreviousGeneration() < a.Capture.RuntimeGeneration {
			return errors.New("checkpoint resume must advance the original sandbox generation once")
		}
	case CheckpointFork:
		if a.Target.SandboxID == a.Capture.SandboxID {
			return errors.New("checkpoint fork requires an independent sandbox")
		}
	default:
		return errors.New("invalid checkpoint restore kind")
	}
	previous := a.PreviousGeneration()
	if previous < 0 || previous == math.MaxInt64 || a.Target.RuntimeGeneration != previous+1 {
		return errors.New("checkpoint restore must advance its admitted predecessor exactly once")
	}
	source := a.Target
	source.SandboxID = a.Capture.SandboxID
	source.RuntimeGeneration = a.Capture.RuntimeGeneration
	if a.Kind == CheckpointFork && source.EnvVars != nil {
		source.EnvVars = maps.Clone(source.EnvVars)
		if _, ok := source.EnvVars[EnvSandboxID]; ok {
			source.EnvVars[EnvSandboxID] = a.Capture.SandboxID
		}
	}
	revision, err := source.Revision()
	if err != nil || revision != a.Capture.Revision {
		return errors.New("checkpoint restore changed the captured workload assignment")
	}
	return nil
}

func (a CheckpointRestoreAssignment) Digest() (string, error) {
	if err := a.Validate(); err != nil {
		return "", err
	}
	return checkpointAssignmentDigest(a)
}

func checkpointIdentity(value string) bool {
	return value != "" && len(value) <= 256 && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n")
}

func checkpointAssignmentDigest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
