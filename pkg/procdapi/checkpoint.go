package procdapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

const RuntimeCheckpointPath = "/internal/v1/runtime/checkpoint"

// RuntimeCheckpointRequest separates capture custody from a later restore.
// CaptureEpoch belongs to the source; LifecycleEpoch belongs to the operation's
// acting sandbox (the target for restore). A fork must not inherit its parent's
// lifecycle epoch. The signed permission covers both epochs and the full target.
type RuntimeCheckpointRequest struct {
	Action         string                                      `json:"action"`
	InstanceID     string                                      `json:"instance_id"`
	CaptureEpoch   int64                                       `json:"capture_epoch"`
	LifecycleEpoch int64                                       `json:"lifecycle_epoch"`
	Capture        runtimecontrol.CheckpointCaptureAssignment  `json:"capture"`
	Restore        *runtimecontrol.CheckpointRestoreAssignment `json:"restore,omitempty"`
}

func (r RuntimeCheckpointRequest) Digest() (string, error) {
	id, err := uuid.Parse(r.InstanceID)
	if err != nil || id == uuid.Nil || id.String() != r.InstanceID ||
		r.CaptureEpoch <= 0 || r.LifecycleEpoch <= 0 {
		return "", errors.New("invalid checkpoint process identity or epoch")
	}
	if err := r.Capture.Validate(); err != nil {
		return "", err
	}
	switch r.Action {
	case MigrationPrepare, MigrationCancel:
		if r.Restore != nil || r.LifecycleEpoch != r.CaptureEpoch {
			return "", errors.New("checkpoint capture cannot select a restore target or change its epoch")
		}
	case MigrationRestore:
		if r.Restore == nil || r.Restore.Capture != r.Capture {
			return "", errors.New("checkpoint restore changed its capture")
		}
		if err := r.Restore.Validate(); err != nil {
			return "", err
		}
		if r.Restore.Kind == runtimecontrol.CheckpointResume && r.LifecycleEpoch < r.CaptureEpoch {
			return "", errors.New("checkpoint resume regressed lifecycle authority")
		}
	default:
		return "", errors.New("invalid checkpoint action")
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (r RuntimeCheckpointRequest) Permission() (string, error) {
	digest, err := r.Digest()
	if err != nil {
		return "", err
	}
	return "runtime:checkpoint:" + digest, nil
}

// ActingSandbox is the token scope. A fork restore uses the child's authority,
// not a source token that could be reused to authorize arbitrary descendants.
func (r RuntimeCheckpointRequest) ActingSandbox() (teamID, sandboxID string) {
	if r.Action == MigrationRestore && r.Restore != nil {
		return r.Restore.Target.TeamID, r.Restore.Target.SandboxID
	}
	return r.Capture.TeamID, r.Capture.SandboxID
}

type RuntimeCheckpointResponse struct {
	InstanceID        string `json:"instance_id"`
	RequestDigest     string `json:"request_digest"`
	RuntimeGeneration int64  `json:"runtime_generation"`
	State             string `json:"state"`
}

func (r RuntimeCheckpointResponse) ValidateFor(request RuntimeCheckpointRequest) error {
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	state, generation := "ready", request.Capture.RuntimeGeneration
	if request.Action == MigrationPrepare {
		state = "prepared"
	}
	if request.Action == MigrationRestore {
		generation = request.Restore.Target.RuntimeGeneration
	}
	if r.InstanceID != request.InstanceID || r.RequestDigest != digest ||
		r.RuntimeGeneration != generation || r.State != state {
		return errors.New("checkpoint response does not match the exact request")
	}
	return nil
}

// CheckpointRuntime sends a single authenticated process-control command.
// Execution capture/restore and their retry policy belong to ctld and manager.
func (c *ProcdClient) CheckpointRuntime(ctx context.Context, address string, request RuntimeCheckpointRequest, token string) (*RuntimeCheckpointResponse, error) {
	if _, err := request.Digest(); err != nil {
		return nil, err
	}
	response, err := doBoundedProcdRequest[RuntimeCheckpointResponse](ctx, c.httpClient, http.MethodPut,
		address+RuntimeCheckpointPath, token, "checkpoint runtime", request, 1<<20)
	if err != nil {
		return nil, err
	}
	if err := response.ValidateFor(request); err != nil {
		return nil, err
	}
	return response, nil
}
